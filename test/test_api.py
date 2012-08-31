"""Unit testing
"""

import unittest
import datetime

from mock import Mock, ANY, call

import test.test_config as config
import fuse.api

_UTC = datetime.timezone.utc
_P15 = datetime.timezone(datetime.timedelta(0, 900))

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
		fuse.api.log = Mock()
		self.api = fuse.api.APIWrapper(config, self.db, mapper)


class TestAPI_NoSeries(TestAPI):
	def test_GetFullSeriesList(self):
		self.db.list_series = Mock(return_value={
			150: { "id": 150,
				   "period": 900,
				   "epoch": datetime.datetime(2012, 8, 28, 16, 30, 0, 0, _P15),
				   "type": "period",
				   "limit": 1000
				   }})
		self.api.get_series_list(self.req, self.res)
		# This test is dependent on the serialisation order of the
		# JSON library, and may fail
		self.assertSequenceEqual(
			list(self.res.data),
			[b'{"150": {"limit": 1000, '
			 b'"epoch": "2012-08-28T16:30:00.000000+0015", "type": "period", '
			 b'"id": 150, "period": 900}}'])

	def test_CreateSeries(self):
		self.db.create_series = Mock(return_value=130)
		self.api.add_series(self.req, self.res)
		self.assertSequenceEqual(list(self.res.data), [b"130"])


class TestAPI_WithSeries(TestAPI):
	def setUp(self):
		TestAPI.setUp(self)
		self.req["wsgiorg.routing_args"] = [None, {"series_id": "19"}]
		self.db.is_series.return_value = True

	def _set_input(self, txt):
		"""Helper function to set the text in the request.
		"""
		self.input.read.return_value = txt
		self.req["CONTENT_LENGTH"] = len(txt)


class TestAPI_AddData(TestAPI_WithSeries):
	def test_AddData_NotSeries(self):
		self.db.is_series.return_value = False
		self.api.add_data(self.req, self.res)
		self.assertEqual(self.res.result, "404 Not found")

	def test_AddData_NotArray1(self):
		self._set_input(b'{"foo":"bar"}')
		self.api.add_data(self.req, self.res)
		self.assertEqual(self.res.result, "400 Bad request")

	def test_AddData_NotArray2(self):
		self._set_input(b'"This is a string"')
		self.api.add_data(self.req, self.res)
		self.assertEqual(self.res.result, "400 Bad request")

	def test_AddData_BadJSON(self):
		self._set_input(b"Ceci n'est pas un string")
		self.api.add_data(self.req, self.res)
		fuse.api.log.warn.assert_called_once_with(ANY, ANY, "Ceci n'est pas un string", "")
		self.assertEqual(self.res.result, "400 Not readable JSON")

	def test_AddData_Single(self):
		self._set_input(b'[["2012-08-28T12:00:00+0015", 42]]')
		self.api.add_data(self.req, self.res)
		self.db.add_value.assert_called_once_with(
			19, datetime.datetime(2012, 8, 28, 12, 0, 0, 0, _P15), 42)
		self.assertEqual(self.res.data.binary, [])

	def test_AddData_Multiple(self):
		self._set_input(b'[["2012-08-28T13:00:00+0015", 42],'
						b'["2012-08-28T13:30:00+0015", 28]]')
		self.api.add_data(self.req, self.res)
		self.db.add_value.assert_has_calls(
			[call(19, datetime.datetime(2012, 8, 28, 13, 0, 0, 0, _P15), 42),
			 call(19, datetime.datetime(2012, 8, 28, 13, 30, 0, 0, _P15), 28)],
			any_order=True)
		self.assertEqual(self.res.data.binary, [])


class TestAPI_WithSeriesAndData(TestAPI_WithSeries):
	def setUp(self):
		TestAPI_WithSeries.setUp(self)
		d = datetime.timedelta(0, 1800)
		bd = datetime.datetime(2012, 8, 28, 12, 0, 0, 0, _P15)
		self.dataset = [
			(bd, 33), (bd+d, 35.5), (bd+d*2, 34.0), (bd+d*3, 31.9),
			(bd+d*4, 33), (bd+d*5, 35.5), (bd+d*6, 34.0), (bd+d*7, 31.9),
			]
		self.db.get_values.return_value = iter(self.dataset)

	def testAPI_GetSeriesData_NoFilter(self):
		self.api.get_data(self.req, self.res)
		self.db.get_values.assert_called_once_with(19)
		self.assertCountEqual(list(self.res.data.binary), self.dataset)

	def testAPI_GetSeriesData_BadSeries(self):
		self.db.is_series.return_value = False
		self.api.get_data(self.req, self.res)
		self.assertEqual(self.res.result.split()[0], "404")

	def testAPI_GetSeriesData_UnknownParam(self):
		self.req["QUERY_STRING"] = "captain=Nemo"
		self.api.get_data(self.req, self.res)
		# We should ignore the string and return the original data set
		self.db.get_values.assert_called_once_with(19)
		self.assertCountEqual(list(self.res.data.binary), self.dataset)

	def testAPI_GetSeriesData_BadParam1(self):
		self.req["QUERY_STRING"] = "startDate=tomorrow"
		with self.assertRaises(ValueError):
			self.api.get_data(self.req, self.res)

	def testAPI_GetSeriesData_BadParam2(self):
		self.req["QUERY_STRING"] = "endDate=tomorrow"
		with self.assertRaises(ValueError):
			self.api.get_data(self.req, self.res)

	def testAPI_GetSeriesData_StartOnly(self):
		self.req["QUERY_STRING"] = "STARTDATE=2012-08-28T13:30:00%2b0000"
		self.api.get_data(self.req, self.res)
		self.db.get_values.assert_called_once_with(
			19,
			from_ts=datetime.datetime(2012, 8, 28, 13, 30, 0, 0, datetime.timezone.utc))
		self.assertCountEqual(list(self.res.data.binary), self.dataset)

	def testAPI_GetSeriesData_EndOnly(self):
		self.req["QUERY_STRING"] = "eNdDaTe=2012-08-28T13:30:00%2b0000"
		self.api.get_data(self.req, self.res)
		self.db.get_values.assert_called_once_with(
			19,
			to_ts=datetime.datetime(2012, 8, 28, 13, 30, 0, 0, datetime.timezone.utc))
		self.assertCountEqual(list(self.res.data.binary), self.dataset)

	def testAPI_GetSeriesData_StartEnd(self):
		self.req["QUERY_STRING"] = "STARTDATE=2012-08-28T13:30:00%2b0000&enddate=2012-08-28T14:30:00%2b0000"
		self.api.get_data(self.req, self.res)
		self.db.get_values.assert_called_once_with(
			19,
			from_ts=datetime.datetime(2012, 8, 28, 13, 30, 0, 0, datetime.timezone.utc),
			to_ts=datetime.datetime(2012, 8, 28, 14, 30, 0, 0, datetime.timezone.utc))
		self.assertCountEqual(list(self.res.data.binary), self.dataset)

class TestAPI_GetSeriesInfo(TestAPI_WithSeries):
	def test_GetInfo_NotSeries(self):
		self.db.is_series.return_value = False
		self.api.get_series_info(self.req, self.res)
		self.assertEqual(self.res.result.split()[0], "404")

	def test_GetInfo_Series(self):
		self.api.get_series_info(self.req, self.res)
		self.db.list_series.assert_called_once_with(sid=19)


class TestAPI_FailAs(unittest.TestCase):
	def setUp(self):
		fuse.api.log = Mock()
		self.res = Mock()

	def test_FailAsShort(self):
		fuse.api.fail_as(self.res, "418 I'm a teapot", "Coffee protocol error", "Wurble")
		self.assertEqual(self.res.result, "418 I'm a teapot")
		self.assertSequenceEqual(self.res.data, [b"Coffee protocol error"])
		fuse.api.log.warn.assert_called_once_with("%s: '%s%s'", "Coffee protocol error", "Wurble", "")

	def test_FailAsLong(self):
		fuse.api.fail_as(self.res, "418 I'm a teapot", "Coffee protocol long error", "Wurble"*20)
		self.assertEqual(self.res.result, "418 I'm a teapot")
		self.assertSequenceEqual(self.res.data, [b"Coffee protocol long error"])
		fuse.api.log.warn.assert_called_once_with("%s: '%s%s'", "Coffee protocol long error", "WurbleWurbleWurbleWurbleWurbleWurbleWurbleWurbleWurbleWurbleWurbleWurbleWurbleWu", "[...]")


class TestAPI_GetJSON(TestAPI):
	def test_GetJSON_ContentType_Missing(self):
		del self.req["CONTENT_TYPE"]
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [{"period": 1800}, '{"period":1800}'])

	def test_GetJSON_ContentType_Wrong(self):
		self.req["CONTENT_TYPE"] = "teleport/sentient-inanimate"
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [{"period": 1800}, '{"period":1800}'])

	def test_GetJSON_ContentLength_Missing(self):
		del self.req["CONTENT_LENGTH"]
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [{"period": 1800}, '{"period":1800}'])

	def test_GetJSON_ContentLength_Wrong(self):
		self.req["CONTENT_LENGTH"] += 10
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [{"period": 1800}, '{"period":1800}'])

	def test_GetJSON_NotUTF8(self):
		self.input.read.return_value = b"\xfd\xfe\xff\xf8\xb5"
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [None, None])

	def test_GetJSON_NotJSON(self):
		self.input.read.return_value = b"Beam me up, Scotty"
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [None, None])

	def test_GetJSON_OK(self):
		r = fuse.api.get_json(self.req, self.res)
		self.assertSequenceEqual(r, [{"period": 1800}, '{"period":1800}'])

class TestAPI_ParseTimestamp(TestAPI):
	def test_SuccessWithFractions(self):
		r = fuse.api.parse_timestamp("2012-08-28T12:00:13.546+0015")
		self.assertEqual(r, datetime.datetime(2012, 8, 28, 12, 0, 13, 546000, _P15))

	def test_Success(self):
		r = fuse.api.parse_timestamp("2012-08-28T15:00:13+0015")
		self.assertEqual(r, datetime.datetime(2012, 8, 28, 15, 0, 13, 0, _P15))

	def test_BadFormat(self):
		with self.assertRaises(ValueError):
			fuse.api.parse_timestamp("Ceci n'est pas un horloge")

if __name__ == '__main__':
	unittest.main()
