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
		self.req = {}
		self.res = Mock()
		self.api = fuse.api.APIWrapper(config, self.db, mapper)

class TestAPI_NoSeries(TestAPI):
	def test_GetEmptySeries(self):
		self.db.list_series = Mock(return_value=[])
		self.api.get_series_list(self.req, self.res)
		self.assertEqual(self.res.data, [])
		
	def test_CreateSeries(self):
		self.db.create_series = Mock(return_value=130)
		self.api.add_series(self.req, self.res)
		self.assertEqual(self.res.data, 130)

if __name__ == '__main__':
	unittest.main()
