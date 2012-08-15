"""Linked-data series API. See 
http://openorg.ecs.soton.ac.uk/wiki/Metering for specifications.
"""

import logging
log = logging.getLogger("api")
import json

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
		res.data = self.db.create_series()

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

