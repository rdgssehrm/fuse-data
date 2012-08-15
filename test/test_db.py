"""Unit testing
"""

import unittest
import datetime

import fuse.db as db
import test.test_config as config

_UTC = datetime.timezone.utc

class TestDBCreate(unittest.TestCase):
	def test_Create(self):
		self.db = db.get_database(config)
		# Fail only if this raises an exception
		self.db._wipe()
		db._DB = None

class TestDBCommon(unittest.TestCase):
	def setUp(self):
		self.db = db.get_database(config)

	def tearDown(self):
		self.db._wipe()
		db._DB = None

class TestDBCreateSeries(TestDBCommon):
	def test_CreateSeries(self):
		sid = self.db.create_series(datetime.timedelta(seconds=1800))
		serlist = self.db.list_series()
		self.assertIn(sid, serlist)

class TestDBWithSeries(TestDBCommon):
	def setUp(self):
		TestDBCommon.setUp(self)
		self.sid = self.db.create_series(datetime.timedelta(seconds=1800))

	def test_DeleteSeries(self):
		self.db.drop_series(self.sid)
		serlist = self.db.list_series()
		self.assertNotIn(self.sid, serlist)

	def test_ListSeries(self):
		serlist = self.db.list_series()
		self.assertIn(self.sid, serlist)
		self.assertEqual(len(serlist), 1)

	def test_AddData(self):
		stamp = datetime.datetime(2010, 2, 14, 12, 00, 30, tzinfo=_UTC)
		self.db.add_value(self.sid, stamp, 134.6)
		d = list(self.db.get_values(self.sid))
		self.assertEqual(len(d), 1)
		self.assertEqual(d[0][0], stamp)
		self.assertAlmostEqual(d[0][1], 134.6)

if __name__ == '__main__':
	unittest.main()
