"""Unit testing
"""

import unittest

from mock import Mock

import fuse.conneg as cn
import fuse.muddleware as mw

class TestConnegBase(unittest.TestCase):
	def setUp(self):
		self.app = Mock(side_effect=self._app)
		self.sr = Mock()
		self.jsonxfm = Mock()
		self.xmlxfm = Mock()
		self.req = { }
		self.transformers = { "json": self.jsonxfm,
							  "xml": self.xmlxfm,
							  "default": self.jsonxfm, }
		self.cn = cn.Conneg(self.app)

	def _app(self, env, sr):
		env["transformers"] = self.transformers
		sr("200 OK", [("X-Header", "Empty")])
		return mw.BinaryJSONIterator(["Sample"])


class TestConnegTransformers(TestConnegBase):
	def setUp(self):
		TestConnegBase.setUp(self)
		self.req["transformers"] = self.transformers

	def test_GetTransformerDefault(self):
		xfm = self.cn.get_transformer(self.req)

		self.assertEqual(xfm, self.jsonxfm())

	def test_GetTransformerDefaultNoJSON(self):
		self.req["transformers"] = { "xml": self.xmlxfm }
		xfm = self.cn.get_transformer(self.req)

		self.assertIsInstance(xfm, cn.JSONTransformer)

	def test_GetTransformerSpecified1(self):
		"""Check that we get the right transformer with a single type=
		parameter
		"""
		self.req["QUERY_STRING"] = "type=json"
		xfm = self.cn.get_transformer(self.req)

		self.assertEqual(xfm, self.jsonxfm())

	def test_GetTransformerSpecified2(self):
		"""Check that we get the right transformer with other
		parameters as well
		"""
		self.req["QUERY_STRING"] = "foo=bar&type=xml&bar=foo"
		xfm = self.cn.get_transformer(self.req)

		self.assertEqual(xfm, self.xmlxfm())

	def test_GetTransformerSpecified3(self):
		"""Check for case-insensitivity in parameter names and values
		"""
		self.req["QUERY_STRING"] = "foo=bar&TyPe=xMl&bar=foo"
		xfm = self.cn.get_transformer(self.req)

		self.assertEqual(xfm, self.xmlxfm())


class TestConneg(TestConnegBase):
	def test_ConnegDefault(self):
		res = self.cn({}, self.sr)
		self.assertEqual(self.jsonxfm().transform.call_count, 1)
		args, kwargs = self.jsonxfm().transform.call_args
		self.assertEqual(args[0], ["Sample"])

	def test_ConnegByType1(self):
		res = self.cn({"QUERY_STRING": "type=json"}, self.sr)
		self.assertEqual(self.jsonxfm().transform.call_count, 1)
		args, kwargs = self.jsonxfm().transform.call_args
		self.assertEqual(args[0], ["Sample"])

	def test_ConnegByType2(self):
		res = self.cn({"QUERY_STRING": "type=xml"}, self.sr)
		self.assertEqual(self.xmlxfm().transform.call_count, 1)
		args, kwargs = self.xmlxfm().transform.call_args
		self.assertEqual(args[0], ["Sample"])


if __name__ == '__main__':
	unittest.main()
