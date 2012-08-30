"""Unit testing
"""

import unittest
import json
import datetime

from mock import Mock, ANY, patch, call
import wsgiref.headers

import fuse.muddleware as mw

_P15 = datetime.timezone(datetime.timedelta(0, 900))

def middleware_maker(maker_id):
	"""Function which returns WSGI middleware stubs
	"""
	def mw(fn):
		"""A middleware takes a WSGI app as parameter and returns a
		WSGI app as a result
		"""
		def app(env, sr):
			"""A WSGI app takes an environment and start_response,
			calls the start_response, and returns an iterator of
			results.
			"""
			return fn(env, sr) + [maker_id]
		return app
	return mw


class TestMuddleware(unittest.TestCase):
	def setUp(self):
		"""WSGI middlewares take an app object and wrap it up. Mock out an
		object we can use to test middlewares.
		"""
		self.result = ["Some result"]
		self.app = Mock(return_value=self.result)
		self.sr = Mock()
		self.env = { }

	def test_JSONDateEncoder_Passthrough(self):
		res = json.dumps([45], cls=mw.JSONDateEncoder)
		self.assertEqual(res, "[45]")

	def testJSONDateEncoder_Date(self):
		res = json.dumps([datetime.datetime(2012, 8, 28, 9, 12, 15, 0, _P15)], cls=mw.JSONDateEncoder)
		self.assertEqual(res, '["2012-08-28T09:12:15.000000+0015"]')

	def testJSONDateEncoder_Interval(self):
		res = json.dumps([datetime.timedelta(54, 8000, 363)], cls=mw.JSONDateEncoder)
		self.assertEqual(res, '["P54DT2H13M20.000363S"]')

	def test_BinaryJSON(self):
		data = mw.BinaryJSONIterator(["some result", "more result"])
		it = iter(data)
		self.assertEqual(next(it), b'["some result", "more result"]')

	def test_AccessFunction1(self):
		wrap = mw.AccessFunctionWrapper(self.app)
		res = wrap(self.env, self.sr)

		self.app.assert_called_once_with(self.env, ANY)
		# Check that the start_response function was called properly
		self.sr.assert_called_once_with("200 OK", ANY)

	def test_AccessFunction2(self):
		# Define a slightly more intelligent wrapped app that modifies
		# the status
		def set_status(request, response):
			response.result = "199 Odd"
			response.data = "Moo"
		self.app.side_effect = set_status

		wrap = mw.AccessFunctionWrapper(self.app)
		res = wrap(self.env, self.sr)

		self.app.assert_called_once_with(self.env, ANY)
		# Check that the start_response function was called properly
		self.sr.assert_called_once_with("199 Odd", ANY)
		# Check that the result was the right type and contains the
		# right data
		self.assertEqual(res, "Moo")

	def test_Compose(self):
		self.app.return_value=[]
		cmw = mw.compose([
			middleware_maker("MW1"),
			middleware_maker("MW2"),
			middleware_maker("MW3")
			])
		app = cmw(self.app)
		res = app(self.env, self.sr)

		self.assertSequenceEqual(res, ["MW3", "MW2", "MW1"])

	def test_Parameterised(self):
		"""Test the behaviour of the 'ParameterisedMiddleware' decorator
		"""
		# Create a stub middleware
		@mw.ParameterisedMiddleware
		class TestMW(object):
			def __init__(self, app, parm):
				self.app = app
				self.parm = parm
			def __call__(self, env, sr):
				self.app.test_parameter = self.parm
				return self.app(env, sr)

		self.app.return_value = ["Result"]
		tmw = TestMW(205)
		app = tmw(self.app)
		res = app(self.env, self.sr)

		self.app.assert_called_once_with(self.env, self.sr)
		self.assertEqual(self.app.test_parameter, 205)
		self.assertEqual(res, ["Result"])

	@patch('fuse.muddleware.log')
	def test_DebugLogger(self, log_object):
		self.app.return_value = ["Some result", "Second line"]

		app = mw.DebugLogger(tag="log_tag", responses=True)(self.app)
		res = app(self.env, self.sr)

		# Check that the upstream app was called properly
		self.app.assert_called_once_with(self.env, self.sr)

		# This should read all of the values in res, thus calling the
		# log object.
		self.assertEqual(list(res), ["Some result", "Second line"])

		# We can then test the call sequence of the log object, which
		# is the main thing we care about
		mw.log.debug.assert_has_calls(
			[call("%s(%s) %s", "log_tag: ", str, "Some result"),
			 call("%s(%s) %s", "log_tag: ", str, "Second line"),
			 ])

# FIXME: Add tests for the exception handler and HTTP change/caching
# test function

if __name__ == '__main__':
	unittest.main()
