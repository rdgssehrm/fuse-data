"""Linked-data series API. See 
http://openorg.ecs.soton.ac.uk/wiki/Metering for specifications.
"""

import logging
log = logging.getLogger("api")
import json
import datetime

from fuse.conneg import JSONTransformer
import fuse.muddleware as muddleware
import fuse.conneg as conneg

BJI = muddleware.BinaryJSONIterator

"""
REST API structure:
/api/series/         GET full list of series IDs, POST to add series
    {seriesid}/      GET for series metadata or data,
	                 PUT to alter series metadata,
					 POST to add data records
"""

class APIWrapper(object):
	def __init__(self, config, db, mapper):
		mapper.wrap = muddleware.compose(
			[conneg.Conneg,
			 muddleware.AccessFunctionWrapper])

		mapper.prefix = "/api"
		mapper.add("/series[/]",
				   GET=self.get_series_list,
				   POST=self.add_series)
		mapper.add("/series/{series_id:digits}[/]",
				   GET=self.get_series_info,
				   PUT=self.alter_series,
				   POST=self.add_data)
		self.db = db

	def get_series_list(self, req, res):
		"""Retrieve and return a list of all series
		"""
		req["transformers"] = { 'json': JSONTransformer }
		res.data = BJI(self.db.list_series())

	def add_series(self, req, res):
		req["transformers"] = { 'json': JSONTransformer }
		# Check what data type we've been passed: it should be
		# application/json
		# FIXME: Move this check to a method decorator
		ct = req.get("CONTENT_TYPE", "")
		if ct != "application/json":
			log.warn("Incorrect content type (%s) %s given", type(ct), ct)

		log.info("Content type is %s", ct)

		try:
			clen = int(req["CONTENT_LENGTH"])
		except ValueError:
			clen = 0 # FIXME: We could just return HTTP 411 here "Length required"

		log.info("Content length is %s", clen)

		inp = req["wsgi.input"].read(clen)
		# FIXME: Use the Content-Encoding(?) header to work out what
		# we should be decoding this as?
		# FIXME: Add these checks as decorators from the muddleware
		#  -- or just a helper function
		try:
			request_text = inp.decode("utf8")
		except UnicodeDecodeError as ex:
			ellip = ""
			if len(inp) > 80:
				ellip = "[...]"
			log.warn("Passed incorrectly-encoded data: '%s%s'", inp[:80], ellip)
			res.result = "400 Badly-encoded data"
			res.data = (b"The request data sent was not in UTF-8",)
			return

		log.info("Content is", request_text)

		try:
			desc = json.loads(request_text)
		except ValueError as ex:
			ellip = ""
			if len(request_text) > 80:
				ellip = "[...]"
			log.warn("Bad JSON input in add_series: '%s%s'", request_text[:80], ellip)
			res.result = "400 Not readable JSON"
			res.data = (b"The request data sent was not readable as JSON",)
			return

		log.debug(desc)
		if not isinstance(desc, dict):
			log.warn("Data was not a dictionary: '%s'", request_text[:80])
			res.result = "400 Bad request"
			res.data = (b"The request data was not a JSON dictionary",)
			return

		if "period" not in desc:
			log.warn("Missing required parameter: period")
			res.result = "400 Missing parameter"
			res.data = (b"The request data was missing a required parameter: period",)
			return

		# FIXME: Parse and accept other options here
		res.data = BJI(
			self.db.create_series(
				datetime.timedelta(seconds=desc['period'])
				)
			)

	def get_series_info(self, req, res):
		pass

	def alter_series(self, req, res):
		pass

	def add_data(self, req, res):
		pass

#		sid = int(req['wsgiorg.routing_args'][1]['series_id'])
#		tuner = _lazy_get_tuner(tid)
#		if tuner is None:
#			res.result = "404 Not found"
#			res.data = "Not found"
#		else:
#			res.headers["Last-Modified"] = time.strftime(muddleware.RFC_2822_DATE,
#														 time.gmtime(tuner.last_update))
#			if muddleware.change_test(req, tuner.last_update):
#				res.data = tuner.get_data()
#			else:
#				res.result = "304 Not changed"
#				res.data = "Not changed"

