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


if __name__ == '__main__':
	unittest.main()
