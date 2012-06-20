"""WSGI middleware
"""

import sys
import logging
import traceback
import time
import calendar
#import json

from wsgiref.headers import Headers

log = logging.getLogger("muddleware")
RFC_2822_DATE = "%a, %d %b %Y %H:%M:%S +0000"

class StructuredResponse(object):
	def __init__(self, headers):
		self.headers = Headers(headers)
		self.result = "200 OK"
		self.data = None

class BinaryJSONIterator(object):
	"""Object used to fake up a WSGI result iterator and still carry
	around binary data.
	"""
	def __init__(self, data):
		self.binary = data

	def __iter__(self):
		return iter([json.dumps(self.binary)])

class BinaryJSONWrapper(object):
	"""Simple wrapper to convert binary data returned by a lower-level
	function into a BinaryJSONIterator.
	"""
	def __init__(self, app):
		self.app = app

	def __call__(self, environ, start_response):
		return BinaryJSONIterator(self.app(environ, start_response))

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

			return traceback.format_exc()

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
