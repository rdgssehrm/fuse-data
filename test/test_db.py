# coding: utf-8
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
		self.sid = self.db.create_series(
			"convergent", datetime.timedelta(seconds=1800),
			unit="°C")

class TestDBWithMultiSeriesCommon(TestDBWithSeriesCommon):
	def setUp(self):
		TestDBWithSeriesCommon.setUp(self)
		self.sid2 = self.db.create_series(
			"convergent",
			datetime.timedelta(seconds=900),
			ts_type="mean",
			unit="°C")
		self.sid3 = self.db.create_series(
			"divergent",
			datetime.timedelta(seconds=600),
			unit="°C/m²")


class TestDBCreateSeries(TestDBCommon):
	def test_CreateSeries(self):
		sid = self.db.create_series("test1", datetime.timedelta(seconds=1800))
		serlist = self.db.list_series()
		self.assertIn(sid, serlist)

	def test_CreateSeriesParams(self):
		sid = self.db.create_series(
			"test2",
			datetime.timedelta(seconds=1000),
			epoch=datetime.datetime(1970, 1, 1, 0, 8, 20, tzinfo=_UTC),
			ts_type="point",
			get_limit=12,
			description="This is a new test series",
			unit="°C/m²")
		serlist = self.db.list_series()
		self.assertIn(sid, serlist)

	def test_CreateSeriesWithFailure1(self):
		sid = self.db.create_series(
			"test3",
			datetime.timedelta(seconds=1800),
			epoch="colin")
		serlist = self.db.list_series()
		self.assertIsNone(sid)
		self.assertEqual(len(serlist), 0)

	def test_CreateSeriesWithFailure2(self):
		sid = self.db.create_series("test4", "colin")
		serlist = self.db.list_series()
		self.assertIsNone(sid)
		self.assertEqual(len(serlist), 0)

	def test_CreateSeriesWithFailure3(self):
		sid = self.db.create_series(
			"test5",
			datetime.timedelta(seconds=1800),
			ts_type="foo")
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
		v = self.db.add_value(self.sid, stamp, 134.6)
		d = list(self.db.get_values(self.sid))

		self.assertTrue(v)
		self.assertEqual(len(d), 1)
		self.assertEqual(d[0][0], stamp)
		self.assertAlmostEqual(d[0][1], 134.6)

	def test_AddData_FailDataRange(self):
		stamp = datetime.datetime(2010, 2, 14, 12, 00, 30, tzinfo=_UTC)
		v = self.db.add_value(self.sid, stamp, "James di Griz")
		self.assertFalse(v)
		d = list(self.db.get_values(self.sid))
		self.assertEqual(len(d), 0)

	def test_UpdateData(self):
		stamp = datetime.datetime(2010, 2, 14, 12, 00, 30, tzinfo=_UTC)
		v = self.db.add_value(self.sid, stamp, 134.6)
		v = self.db.add_value(self.sid, stamp, 218.2)
		d = list(self.db.get_values(self.sid))

		self.assertTrue(v)
		self.assertEqual(len(d), 1)
		self.assertEqual(d[0][0], stamp)
		self.assertAlmostEqual(d[0][1], 218.2)

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
		serlist = self.db.list_series(ts_type=["point"])
		self.assertCountEqual((self.sid, self.sid3), serlist)

	def test_ListSeriesByTypeMulti(self):
		serlist = self.db.list_series(ts_type=["point", "mean"])
		self.assertCountEqual((self.sid, self.sid2, self.sid3), serlist)

	def test_ListSeriesByUnit(self):
		serlist = self.db.list_series(unit=["°c/m²"])
		self.assertCountEqual((self.sid3,), serlist)

	def test_ListSeriesByUnitMulti(self):
		serlist = self.db.list_series(unit=["°c/m²", "°c"])
		self.assertCountEqual((self.sid, self.sid2, self.sid3), serlist)

	def test_GetFacet(self):
		res = self.db.facet_summary("period")
		self.assertCountEqual(((datetime.timedelta(seconds=900), 1),
							   (datetime.timedelta(seconds=600), 1),
							   (datetime.timedelta(seconds=1800), 1)),
							  res)

	def test_GetFacet2(self):
		res = self.db.facet_summary("units")
		self.assertCountEqual((("°C", 2),("°C/m²",1)), res)

	def test_GetFacetFail(self):
		# Note: we use Exception here, because (at least in psycopg2)
		# it's the first non-library-specific exception in the MRO for
		# the ProgrammingError exception that we're expecting. This is
		# far too generic for my liking, but switching to something
		# more specific would either need help from the DB
		# implementation code (which is ugly), or to put in a bunch of
		# different tests, one for each implementation (which is
		# worse).
		with self.assertRaises(Exception):
			res = self.db.facet_summary("diamond")


if __name__ == '__main__':
	unittest.main()
