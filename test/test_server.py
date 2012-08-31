"""High-level / integration testing. This should not mock out
anything, but instead test the full end-to-end operation of the
server.
"""

import time
import os
import unittest
import multiprocessing
import json
import datetime
import wsgiref.simple_server

import requests

import test.test_config as config
import fuse.app
import fuse.db

_P15 = datetime.timezone(datetime.timedelta(0, 900))
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

class TestServer_Base(unittest.TestCase):
	def setUp(self):
		def gen_server():
			srv = wsgiref.simple_server.make_server(
				"", config.port, fuse.app.get_app(config))
			srv.serve_forever()

		self.srv_process = multiprocessing.Process(target=gen_server)
		self.srv_process.start()
		time.sleep(0.5)
		self.db = fuse.db.get_database(config) # Grab the database
		self.base_uri = "http://localhost:{0}/api/".format(config.port)

	def tearDown(self):
		self.srv_process.terminate()
		self.srv_process.join()
		self.db._wipe()
		fuse.db._DB = None


class TestServer_WithoutSeries(TestServer_Base):
	def test_CreateSeries(self):
		series = { "period": 1800,
				   "type": "point",
				   "offset": 0,
				   "label": "Test series" }
		r = requests.post(self.base_uri + "series/",
						  data=json.dumps(series).encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.ok)
		s = self.db.list_series()
		self.assertEqual(len(s), 1)
		keys = list(s.keys())
		rec = s[keys[0]]
		self.assertIn("id", rec)
		self.assertEqual(rec["id"], int(keys[0]))

	def test_BadEncoding(self):
		r = requests.post(self.base_uri + "series/",
						  data=b"\xff\xfe\xfe\xff\xff\xff\xfe\xfe\xfe")
		# This text is not UTF-8. Not in the slightest.
		
		self.assertEqual(r.status_code, requests.codes.bad_request)


class TestServer_WithSeries_Base(TestServer_Base):
	def setUp(self):
		TestServer_Base.setUp(self)
		self.sid = self.db.create_series(datetime.timedelta(seconds=1800))

	def tearDown(self):
		self.db.drop_series(self.sid)
		TestServer_Base.tearDown(self)


class TestServer_WithSeries(TestServer_WithSeries_Base):
	def test_AddData_BadJSON(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid) + "/data/",
						  data="Ceci n'est pas un string".encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.bad_request)

	def test_AddData_WrongStructure(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid) + "/data/",
						  data='{"foo":12}'.encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.bad_request)

	def test_AddData_WrongType(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid) + "/data/",
						  data='[["2012-08-28T12:00:00+0015", "Thirty-three"]]'.encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.partial_content)
		self.assertSequenceEqual(r.json, [["2012-08-28T12:00:00.000000+0015", "Thirty-three"]])

	def test_AddData_NonSeries(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid+1) + "/data/",
						  data='[["2012-08-28T12:00:00+0015", 33]]'.encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.not_found)

	def test_AddData_PartialWrongType(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid) + "/data/",
						  data='[["2012-08-28T12:00:00+0015", "Thirty-three"],'
								'["2012-08-28T12:30:00+0015", 35]]'.encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.partial_content)
		self.assertSequenceEqual(
			r.json,
			[["2012-08-28T12:00:00.000000+0015", "Thirty-three"]])
		v = self.db.get_values(self.sid)
		self.assertSequenceEqual(
			[(ts.astimezone(_P15), val) for ts, val in v],
			[(datetime.datetime(2012, 8, 28, 12, 30, 0, 0, _P15), 35)])

	def test_AddData_Single(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid) + "/data/",
						  data='[["2012-08-28T12:00:00+0015", 33]]'.encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.ok)
		v = self.db.get_values(self.sid)
		self.assertSequenceEqual(
			[(ts.astimezone(_P15), val) for ts, val in v],
			[(datetime.datetime(2012, 8, 28, 12, 0, 0, 0, _P15), 33)])

	def test_AddData_Multiple(self):
		r = requests.post(self.base_uri + "series/" + str(self.sid) + "/data/",
						  data='[["2012-08-28T12:00:00+0015", 33],'
								'["2012-08-28T12:30:00+0015", 37]]'.encode("utf8"))
		self.assertEqual(r.status_code, requests.codes.ok)
		v = self.db.get_values(self.sid)
		self.assertSequenceEqual(
			[(ts.astimezone(_P15), val) for ts, val in v],
			[(datetime.datetime(2012, 8, 28, 12, 0, 0, 0, _P15), 33),
			 (datetime.datetime(2012, 8, 28, 12, 30, 0, 0, _P15), 37)])

	def test_GetSeries_NonSeries(self):
		r = requests.get(self.base_uri + "series/" + str(self.sid+1))
		self.assertEqual(r.status_code, requests.codes.not_found)

	def test_GetSeries(self):
		r = requests.get(self.base_uri + "series/" + str(self.sid))
		self.assertEqual(r.status_code, requests.codes.ok)
		#self.assertEqual(r.json, {})


class TestServer_WithData_Base(TestServer_WithSeries_Base):
	def setUp(self):
		TestServer_WithSeries_Base.setUp(self)
		ts = datetime.datetime(2012, 8, 28, 12, 0, 0, 0, _P15)
		d = datetime.timedelta(0, 1800)
		self.data = []
		for i, v in enumerate((35, 33.7, 30.9, 39.0, 41, 36.3, 37.4, 31)):
			self.db.add_value(self.sid, ts + d*i, v)
			self.data.append((ts + d*i, v))
		self.data_uri = self.base_uri + "series/" + str(self.sid) + "/data/"

	def tearDown(self):
		TestServer_WithSeries_Base.tearDown(self)


class TestServer_WithData(TestServer_WithData_Base):
	def test_GetData_NonSeries(self):
		r = requests.get(self.base_uri + "series/" + str(self.sid+1) + "/data")
		self.assertEqual(r.status_code, requests.codes.not_found)

	def test_GetData_All(self):
		r = requests.get(self.data_uri)
		self.assertEqual(r.status_code, requests.codes.ok)
		self.assertSequenceEqual(r.json, self.data)

	def test_GetData_BadStart(self):
		r = requests.get(self.data_uri + "?startdate=Christmas")
		self.assertEqual(r.status_code, requests.codes.bad_request)

	def test_GetData_BadEnd(self):
		r = requests.get(self.data_uri + "?enddate=wednesday%20week")
		self.assertEqual(r.status_code, requests.codes.bad_request)

	def test_GetData_All(self):
		r = requests.get(self.data_uri)
		self.assertEqual(r.status_code, requests.codes.ok)
		self.assertSequenceEqual(
			[(datetime.datetime.strptime(ts, DATE_FORMAT), v) for ts, v in r.json],
			self.data)

	def test_GetData_StartOnly(self):
		r = requests.get(self.data_uri + "?startDate=2012-08-28T13:30:00%2b0000")
		self.assertEqual(r.status_code, requests.codes.ok)
		self.assertSequenceEqual(
			[(datetime.datetime.strptime(ts, DATE_FORMAT), v) for ts, v in r.json],
			self.data[4:])

	def test_GetData_EndOnly(self):
		r = requests.get(self.data_uri + "?eNdDaTe=2012-08-28T14:30:00%2b0000")
		self.assertEqual(r.status_code, requests.codes.ok)
		self.assertSequenceEqual(
			[(datetime.datetime.strptime(ts, DATE_FORMAT), v) for ts, v in r.json],
			self.data[:6])

	def test_GetData_StartAndEnd(self):
		r = requests.get(self.data_uri + "?eNdDaTe=2012-08-28T14:30:00%2b0000&STARTDATE=2012-08-28T13:30:00%2b0000")
		self.assertEqual(r.status_code, requests.codes.ok)
		self.assertSequenceEqual(
			[(datetime.datetime.strptime(ts, DATE_FORMAT), v) for ts, v in r.json],
			self.data[4:6])


if __name__ == '__main__':
	unittest.main()
