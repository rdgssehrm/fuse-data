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

class TestDBWithSeriesCommon(TestDBCommon):
	def setUp(self):
		TestDBCommon.setUp(self)
		self.sid = self.db.create_series(datetime.timedelta(seconds=1800))

class TestDBWithMultiSeriesCommon(TestDBWithSeriesCommon):
	def setUp(self):
		TestDBWithSeriesCommon.setUp(self)
		self.sid2 = self.db.create_series(
			datetime.timedelta(seconds=900),
			ts_type="pointless")
		self.sid3 = self.db.create_series(datetime.timedelta(seconds=600))


class TestDBCreateSeries(TestDBCommon):
	def test_CreateSeries(self):
		sid = self.db.create_series(datetime.timedelta(seconds=1800))
		serlist = self.db.list_series()
		self.assertIn(sid, serlist)

	def test_CreateSeriesParams(self):
		sid = self.db.create_series(
			datetime.timedelta(seconds=1000),
			epoch=datetime.datetime(1970, 1, 1, 0, 8, 20, tzinfo=_UTC),
			ts_type="test",
			get_limit=12)
		serlist = self.db.list_series()
		self.assertIn(sid, serlist)

	def test_CreateSeriesWithFailure1(self):
		sid = self.db.create_series(
			datetime.timedelta(seconds=1800),
			epoch="colin")
		serlist = self.db.list_series()
		self.assertIsNone(sid)
		self.assertEqual(len(serlist), 0)

	def test_CreateSeriesWithFailure2(self):
		sid = self.db.create_series("colin")
		serlist = self.db.list_series()
		self.assertIsNone(sid)
		self.assertEqual(len(serlist), 0)

class TestDBWithSeries(TestDBWithSeriesCommon):
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

	def test_IsSeriesPositive(self):
		self.assertTrue(self.db.is_series(self.sid))

	def test_IsSeriesNegative(self):
		self.assertFalse(self.db.is_series(-35))

class TestDBWithMultiSeries(TestDBWithMultiSeriesCommon):
	def test_ListSeriesByID(self):
		serlist = self.db.list_series(sid=self.sid2)
		self.assertCountEqual((self.sid2,), serlist)

	def test_ListSeriesByIDFail(self):
		serlist = self.db.list_series(sid=-35)
		self.assertCountEqual((), serlist)

	def test_ListSeriesByPeriodSingle(self):
		serlist = self.db.list_series(period=datetime.timedelta(seconds=1800))
		self.assertCountEqual((self.sid,), serlist)

	def test_ListSeriesByPeriodFail(self):
		serlist = self.db.list_series(period=datetime.timedelta(seconds=1900))
		self.assertCountEqual((), serlist)

	def test_ListSeriesByPeriodRangeTuple(self):
		serlist = self.db.list_series(period=(datetime.timedelta(seconds=500),
											  datetime.timedelta(seconds=1000)))
		self.assertCountEqual((self.sid2, self.sid3), serlist)

	def test_ListSeriesByPeriodRangeList(self):
		serlist = self.db.list_series(period=[datetime.timedelta(seconds=500),
											  datetime.timedelta(seconds=1000)])
		self.assertCountEqual((self.sid2, self.sid3), serlist)

	def test_ListSeriesByType(self):
		serlist = self.db.list_series(ts_type="point")
		self.assertCountEqual((self.sid, self.sid3), serlist)


if __name__ == '__main__':
	unittest.main()
