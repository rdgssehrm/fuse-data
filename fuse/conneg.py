# -*- coding: utf-8 -*-

"""Content negotiation middleware: determine the requested content
type (via any of a number of methods), and transform the content
returned from the upstream responder into a suitable format.

The flow of control here is a bit complicated. The Conneg class is a
WSGI middleware: it takes a WSGI application as a parameter to a
function-call-like syntax (i.e. the constructor of the class), and
returns something (in this case a Conneg object) which behaves as a
WSGI application.

The Conneg object acts as a WSGI application: it takes a WSGI
environment and a start_response function, calls the start_response
function, and returns an iterable of data to be returned. Since it was
generated as part of a middleware, its job is simply to transform the
output of the lower-level WSGI application (here, the self.fn member).

We give the subordinate application a specially-constructed
start_response function for it to call (Conneg.new_start_response).
This function is ultimately called by the bottom-level application to
indicate the HTTP response code and the headers that should be
returned.
"""

import logging
import json
import csv

import fuse.muddleware as muddleware

log = logging.getLogger()

class _State(object):
	"""A simple container object for passing around bound state within
	the Conneg handler.
	"""
	def __init__(self, env, sr):
		self.env = env
		self.start_response = sr
		self.transformer = None

class Conneg(object):
	"""Middleware which attempts to identify the requested output
	format, and converts the data being returned from below into a
	suitable format. Conversion methods are supplied by the caller in
	the environment, as a dict called "transformers".

	The transformers dict contains mappings between the 'type'
	parameter of the query and a callable that is used to create a
	transformation object. The transformation object should implement
	the API exemplified by the Transformer class.
	"""
	def __init__(self, fn):
		self.fn = fn

	def __call__(self, environ, start_response):
		state = _State(environ, start_response)

		# Bind the current environment and upstream start_response
		# function into a callable we can pass down
		def new_start_response(res, hdr, exc=None):
			return self.start_response_impl(state, res, hdr, exc)
		# Call the downstream middleware (which will call our
		# new_start_response function at some point)
		data = self.fn(environ, new_start_response)

		if state.result != "200":
			return data

		# data is an iterator of strings (plus, we hope, some binary
		# data) that we can process further
		if hasattr(data, "binary"):
			data = state.transformer.transform(data.binary, environ)
		# If not, we simply pass the data straight through

		# Render the result
		return data

	def start_response_impl(self, state, res, hdr, exc_info):
		"""Wrapper function for start_response, that captures the
		downstream object's intentions for the response, and allows us
		to modify it (e.g. by adding headers).
		"""
		state.result = res.split(" ")[0]

		# Work out from the request environment what output format we
		# want to use, and select it
		state.transformer = self.get_transformer(state.env)

		# Modify the existing headers: drop any content-type or
		# content-length headers
		new_hdr = []
		for name, value in hdr:
			lname = name.lower()
			if lname == "content-type":
				continue
			if lname == "content-length":
				continue
			new_hdr.append((name, value))

		# Add in suitable headers for the transformed output
		state.transformer.http_headers(new_hdr)

		# Continue with the original function call as if nothing has
		# happened
		write = state.start_response(res, new_hdr)
		def new_write(data):
			log.error("Deprecated write function called! Data not written.")
			write(state.transformer.write(data))

		return new_write

	def get_transformer(self, environ):
		"""Examine the WSGI environment to work out what format type
		was requested.
		"""
		typ = None

		# FIXME: Use a caller-provided parameter name and abbrev->MIME
		# mapping here

		if "QUERY_STRING" in environ and environ["QUERY_STRING"] != "":
			for p in environ["QUERY_STRING"].split("&"):
				key, value = p.split("=", 1)
				if key.lower() == "type":
					typ = value.lower()

		if typ is None:
			try:
				typ = environ["wsgiorg.routing_args"][1]["extension"].lower()
			except KeyError:
				pass

		# FIXME: Check the HTTP headers for acceptable MIME types if
		# no explicit type is given

		try:
			return environ["transformers"][typ]()
		except:
			pass

		try:
			return environ["transformers"]["default"]()
		except:
			pass

		return JSONTransformer()

class Transformer(object):
	"""Base class providing stub implementations of a minimal
	transformer class
	"""
	def __init__(self, mime_type="application/json"):
		self.mime_type = mime_type

	def http_headers(self, headers):
		"""Modify the HTTP headers as appropriate for this renderer.
		"""
		headers.append(("Content-Type", self.mime_type))

	def write(self, data):
		log.error("Deprecated write function called!")

	def transform(self, binary, environ):
		pass

class JSONTransformer(Transformer):
	def __init__(self):
		Transformer.__init__(self)

	def transform(self, binary, environ):
		return [json.dumps(binary, cls=muddleware.JSONDateEncoder).encode("utf8")]

class CSVDataTransformer(Transformer):
	class _OutHandler(object):
		"""Class implementing an iterator which uses a CSV writer
		object to generate its output.

		This class also implements the write() method so that the CSV
		writer can actually write a line to it.
		"""
		def __init__(self, data):
			self.value = []
			self.data = data

		def write(self, x):
			self.value.append(x)

		def __iter__(self):
			"""Return an iterator based on this object: actually a
			generator function
			"""
			fmt = csv.writer(self, dialect="excel")
			for line in self.data:
				fmt.writerow(line)
				yield "".join(self.value).encode("utf8")
				self.value = []

	def transform(self, binary, environ):
		# Process just a time/value pair into a CSV output
		return self._OutHandler(binary)
