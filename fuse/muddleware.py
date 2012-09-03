"""WSGI middleware
"""

import sys
import logging
import traceback
import time
import calendar
import json

from wsgiref.headers import Headers

log = logging.getLogger("muddleware")
RFC_2822_DATE = "%a, %d %b %Y %H:%M:%S +0000"
ISO_8601_DATE = "%Y-%m-%dT%H:%M:%S.%f%z"

class StructuredResponse(object):
	def __init__(self, headers):
		self.headers = Headers(headers)
		self.result = "200 OK"
		self.data = None

class JSONDateEncoder(json.JSONEncoder):
	"""Date encoder for JSON. Allows passing datetime objects and
	timedelta objects directly to the JSON code, and it will render
	them using the standardised ISO 8601 format above.

	Note that the interval serialisation does not actually meet the
	ISO 8601 standard fully, as it doesn't specify the leading Y or M
	fields, and so the D field is not normalised. However, dealing
	with months is a nasty thing, and I'm not going to do it here
	(because it's not supported by datetime.timedelta).
	"""
	def default(self, obj):
		try:
			return obj.strftime(ISO_8601_DATE)
		except AttributeError:
			pass

		try:
			secs = obj.seconds
			mins = secs // 60
			secs = secs % 60
			hrs = mins // 60
			mins = mins % 60
			return "P{0.days}DT{1}H{2}M{3}.{0.microseconds:06d}S".format(obj, hrs, mins, secs)
		except AttributeError:
			pass

		return json.JSONEncoder.default(self, obj)

class BinaryJSONIterator(object):
	"""Object used to fake up a WSGI result iterator and still carry
	around binary data. We *can't* turn this into a full middleware,
	because it then becomes impossible to send non-JSON error
	messages, and the whole thing gets icky quite quickly.
	"""
	def __init__(self, data):
		self.binary = data

	def __iter__(self):
		return iter([json.dumps(self.binary, cls=JSONDateEncoder).encode("utf8")])

class AccessFunctionWrapper(object):
	"""This wrapper is the innermost object: It takes a function that
	takes (request, response) parameters, and turns it into a WSGI
	method.
	"""
	def __init__(self, fn):
		self.fn = fn

	def __call__(self, environ, start_response):
		# Set up a response
		headers_list = []
		resp = StructuredResponse(headers_list)
		# Call the function
		self.fn(environ, resp)
		# Send the headers
		start_response(resp.result, headers_list)
		# Render it
		if resp.data is not None:
			return resp.data
		else:
			return ""

class ExceptionHandler(object):
	"""Wrapper to handle and report any exceptions that leak out
	"""
	def __init__(self, fn):
		self.fn = fn

	def __call__(self, environ, start_response):
		try:
			return self.fn(environ, start_response)
		except Exception as ex:
			h = []
			headers = Headers(h)
			start_response("500 Infernal Server Error", h)
			log.error("Exception: %s", str(ex))
			for l in traceback.format_exc().split("\n"):
				log.error("Exception: %s", l)

			return [traceback.format_exc().encode("utf8")]

class ParameterisedMiddleware(object):
	"""Class decorator which allows us to write middlewares which bind
	additional parameters to the middleware creation. e.g.:

	@ParameterisedMiddleware
	class Foo:
		def __init__(self, app, arg):
			...

	MW = Foo(3)   # MW is a middleware, and may then be composed with an app:
	new_app = MW(app)
	"""
	def __init__(self, cls):
		self.cls = cls

	def __call__(self, *args, **kwargs):
		def MW(app):
			return self.cls(app, *args, **kwargs)
		return MW

@ParameterisedMiddleware
class CORS(object):
	"""Add CORS headers to responses
	"""
	def __init__(self, app, sites=["*"]):
		self.app = app
		self.sites = sites

	def __call__(self, environ, start):
		def my_start(result, headers):
			headers.append(("Access-Control-Allow-Origin", " ".join(self.sites)))
			return start(result, headers)

		return self.app(environ, my_start)

@ParameterisedMiddleware
class DebugLogger(object):
	"""Implementation of a DebugLogger object
	"""
	def __init__(self, app, tag="", responses=False, requests=False,
				 environment=False, resp_headers=False):
		self.app = app
		self.responses = responses
		self.requests = requests
		self.environment = environment
		self.resp_headers = resp_headers
		if tag != "":
			tag += ": "
		self.tag = tag

	class LogIterator(object):
		def __init__(self, mw, obj, tag):
			self.obj = iter(obj)
			self.tag = tag
			self.mw = mw

		def __iter__(self):
			return self

		def __next__(self):
			rv = next(self.obj)
			if self.mw.responses:
				log.debug("%s(%s) %s", self.tag, type(rv), rv)
			return rv

	def __call__(self, environ, start_response):
		return self.LogIterator(
			self, self.app(environ, start_response), self.tag)

def compose(mwares):
	"""This function takes a list of middlewares, and returns a
	middleware that acts as the composition of them all.
	"""
	def MW(fn):
		"""This is a middleware: it takes a WSGI app, and wraps it up,
		acting as a WSGI app in its own right.
		"""
		for wrapper in reversed(mwares):
			fn = wrapper(fn)
		return fn

	return MW

def change_test(req, stamp):
	"""Return True if the full content should be returned, False if a
	304 can be returned"""
	try:
		cutoff = time.strptime(req['HTTP_IF_MODIFIED_SINCE'], RFC_2822_DATE)
		cutoff = calendar.timegm(cutoff)
		return stamp >= cutoff
	except KeyError:
		pass
	# Don't catch ValueError, as that's an HTTP problem and deserves a 500

	try:
		cutoff = time.strptime(req['HTTP_IF_UNMODIFIED_SINCE'], RFC_2822_DATE)
		cutoff = calendar.timegm(cutoff)
		return stamp <= cutoff
	except KeyError:
		pass

	return True
