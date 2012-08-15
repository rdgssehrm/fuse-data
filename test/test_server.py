"""High-level / integration testing
"""

import time
import os
import unittest
import multiprocessing
import json
import wsgiref.simple_server

from mock import Mock
import requests

import test.test_config as config
import fuse.app
import fuse.db

class TestServer(unittest.TestCase):
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

if __name__ == '__main__':
	unittest.main()
