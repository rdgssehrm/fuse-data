"""Unit testing
"""

import unittest

from mock import Mock, ANY
import wsgiref.headers

import fuse.muddleware as mw


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

	def test_BinaryJSON(self):
		wrap = mw.BinaryJSONWrapper(self.app)
		res = wrap(self.env, self.sr)

		self.app.assert_called_once_with(self.env, self.sr)
		self.assertIsInstance(res, mw.BinaryJSONIterator)
		self.assertEqual(res.binary, self.result)
		self.assertEqual(list(res), ['["Some result"]'])

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

# FIXME: Add tests for the exception handler and HTTP change/caching
# test function

if __name__ == '__main__':
	unittest.main()
