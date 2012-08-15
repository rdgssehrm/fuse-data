"""Unit testing
"""

import unittest
import datetime

from mock import Mock

import test.test_config as config
import fuse.api

_UTC = datetime.timezone.utc

class TestAPI(unittest.TestCase):
	def setUp(self):
		mapper = Mock()
		self.db = Mock()
		self.input = Mock()
		self.input.read.return_value = b'{"period":1800}'
		self.req = { "CONTENT_TYPE": "application/json",
					 "CONTENT_LENGTH": len(self.input.read()),
					 "wsgi.input": self.input, }
		self.res = Mock()
		self.api = fuse.api.APIWrapper(config, self.db, mapper)

class TestAPI_NoSeries(TestAPI):
	def test_GetEmptySeries(self):
		self.db.list_series = Mock(return_value=[150])
		self.api.get_series_list(self.req, self.res)
		self.assertSequenceEqual(list(self.res.data), ['[150]'])

	def test_CreateSeries(self):
		self.db.create_series = Mock(return_value=130)
		self.api.add_series(self.req, self.res)
		self.assertSequenceEqual(list(self.res.data), ["130"])

if __name__ == '__main__':
	unittest.main()

