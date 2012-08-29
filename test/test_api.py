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
		fuse.api.log = Mock()
		self.api = fuse.api.APIWrapper(config, self.db, mapper)


class TestAPI_NoSeries(TestAPI):
	def test_GetEmptySeries(self):
		self.db.list_series = Mock(return_value=[150])
		self.api.get_series_list(self.req, self.res)
		self.assertSequenceEqual(list(self.res.data), [b'[150]'])

	def test_CreateSeries(self):
		self.db.create_series = Mock(return_value=130)
		self.api.add_series(self.req, self.res)
		self.assertSequenceEqual(list(self.res.data), [b"130"])


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

if __name__ == '__main__':
	unittest.main()
