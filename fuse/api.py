"""Linked-data series API. See 
http://openorg.ecs.soton.ac.uk/wiki/Metering for specifications.
"""

import logging
log = logging.getLogger("api")
import json
import datetime
import urllib.parse

from fuse.conneg import JSONTransformer
import fuse.muddleware as muddleware
import fuse.conneg as conneg

BJI = muddleware.BinaryJSONIterator
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
STD_TRANSFORMERS = { 'json': JSONTransformer }

"""
REST API structure:
/api/series/        GET full list of series IDs, POST to add series
    {seriesid}/     GET for series metadata,
	                PUT to alter series metadata,
		data/		GET for series data,
					POST to add/modify data records
"""

# Helper functions
def fail_as(res, result, message, data):
	ellip = ""
	if len(data) > 80:
		ellip = "[...]"
	log.warn("%s: '%s%s'", message, data[:80], ellip)
	res.result = result
	res.data = (message.encode("utf8"),)


def get_json(req, res):
	# Check what data type we've been passed: it should be
	# application/json
	# FIXME: Move this check to a method decorator
	ct = req.get("CONTENT_TYPE", "")
	if ct != "application/json":
		log.warn("Incorrect content type (%s) %s given", type(ct), ct)

	try:
		clen = int(req["CONTENT_LENGTH"])
	except ValueError:
		clen = 0 # FIXME: We could just return HTTP 411 here "Length required"
	except KeyError:
		clen = 0 # FIXME: We could just return HTTP 411 here "Length required"

	inp = req["wsgi.input"].read(clen)
	# FIXME: Use the Content-Encoding(?) header to work out what
	# we should be decoding this as?
	# FIXME: Add these checks as decorators from the muddleware
	#  -- or just a helper function
	try:
		request_text = inp.decode("utf8")
	except UnicodeDecodeError as ex:
		fail_as(res, "400 Badly-encoded data",
				"The request data sent was not in UTF-8", inp)
		return None, None

	try:
		struct = json.loads(request_text)
	except ValueError as ex:
		fail_as(res, "400 Not readable JSON",
				"The request data sent was not readable as JSON",
				request_text)
		return None, None

	return struct, request_text


def parse_timestamp(ts):
	# FIXME: Allow TZ-free input (under protest) as well.
	for fmt in (DATE_FORMAT,
				"%Y-%m-%dT%H:%M:%S%z",
				#"%Y-%m-%dT%H:%M:%S",
				#"%Y-%m-%dT%H:%M:%S.%f"
				):
		try:
			return datetime.datetime.strptime(ts, fmt)
		except ValueError:
			pass
	raise ValueError("time data '{0}' cannot be parsed".format(ts))

class APIWrapper(object):
	def __init__(self, config, db, mapper):
		mapper.wrap = muddleware.compose(
			[conneg.Conneg,
			 muddleware.CORS(),
			 muddleware.AccessFunctionWrapper])

		mapper.prefix = "/api"
		mapper.add("/series[/]",
				   GET=self.get_series_list,
				   POST=self.add_series)
		mapper.add("/series/{series_id:digits}[/]",
				   GET=self.get_series_info,
				   PUT=self.alter_series,
				   POST=self.add_data)
		mapper.add("/series/{series_id:digits}/data[/]",
				   GET=self.get_data,
				   POST=self.add_data)
		self.db = db

	def get_series_list(self, req, res):
		"""Retrieve and return a list of all series
		"""
		# FIXME: Check the query string for search parameters
		req["transformers"] = STD_TRANSFORMERS
		res.data = BJI(self.db.list_series())

	def add_series(self, req, res):
		"""Add a new series (data in JSON), returning the ID of the
		newly-created series"""
		# FIXME: Also check the query string for parameters
		req["transformers"] = STD_TRANSFORMERS
		desc, request_text = get_json(req, res)
		if desc is None: return

		if not isinstance(desc, dict):
			fail_as(res, "400 Bad request",
					"The request data was not a JSON dictionary",
					request_text)
			return

		if "period" not in desc:
			fail_as(res, "400 Missing parameter",
					"The request data was missing a required parameter",
					"period")
			return

		# FIXME: Parse and accept other options here
		res.data = BJI(
			self.db.create_series(
				datetime.timedelta(seconds=desc['period'])
				)
			)

	def get_series_info(self, req, res):
		"""Retrieve information for a single series"""
		req["transformers"] = STD_TRANSFORMERS
		sid = int(req["wsgiorg.routing_args"][1]["series_id"])
		if not self.db.is_series(sid):
			fail_as(res, "404 Not found", "Series not found", str(sid))
			return

		res.data = BJI(self.db.list_series(sid=sid))

	def alter_series(self, req, res):
		pass

	def get_data(self, req, res):
		"""Retrieve data from a series, filtered by date range, and
		(optionally) processed with temporal quanta and
		min/max/mean/stdev filters.
		"""
		req["transformers"] = STD_TRANSFORMERS
		sid = int(req["wsgiorg.routing_args"][1]["series_id"])
		if not self.db.is_series(sid):
			fail_as(res, "404 Not found", "Series not found", str(sid))
			return

		kwargs = {}
		# Get query-string arguments
		try:
			qstring = urllib.parse.parse_qs(req["QUERY_STRING"])
		except KeyError:
			qstring = {}
		# Sanitise parameters
		for k, v in qstring.items():
			lk = k.lower()
			if lk == "startdate":
				try:
					kwargs["from_ts"] = parse_timestamp(v[0])
				except ValueError:
					fail_as(res, "400 Unparsable parameter",
							"Start date was not parsable", v[0])
					return
			if lk == "enddate":
				try:
					kwargs["to_ts"] = parse_timestamp(v[0])
				except ValueError:
					fail_as(res, "400 Unparsable parameter",
							"End date was not parsable", v[0])
					return
			# FIXME: Further processing of other parameters here.
			# No parameter should be passed unvalidated or without a
			# definite key.

		res.data = BJI(list(self.db.get_values(sid, **kwargs)))

	def add_data(self, req, res):
		"""Add data to a series. Data format is an array of (time,
		value) tuple.
		"""
		req["transformers"] = STD_TRANSFORMERS
		sid = int(req["wsgiorg.routing_args"][1]["series_id"])
		# Check the series ID matches an existing series
		if not self.db.is_series(sid):
			fail_as(res, "404 Not found", "Series not found", str(sid))
			return

		desc, request_text = get_json(req, res)
		if desc is None: return

		if not isinstance(desc, list):
			fail_as(res, "400 Bad request",
					"The request data was not a JSON array",
					request_text)
			return

		errors = []
		for line in desc:
			try:
				ts, value = line
				ts = parse_timestamp(ts)
				rv = self.db.add_value(sid, ts, value)
			except ValueError:
				rv = False

			if not rv:
				res.result = "206 Partial update"
				errors.append([ts.strftime(DATE_FORMAT), value])

		res.data = BJI(errors)
