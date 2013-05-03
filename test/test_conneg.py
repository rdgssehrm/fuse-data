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
		self.data = ["Sample"]

	def _app(self, env, sr):
		env["transformers"] = self.transformers
		sr("200 OK", [("X-Header", "Empty")])
		return mw.BinaryJSONIterator(self.data)


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

class TestConneg_CSV(unittest.TestCase):
	"""Validating the CSV transformer -- making sure that the
	resulting output is the expected CSV format.
	"""
	def setUp(self):
		self.data = {
			"meta": [
				{ "name": "Name", "units": "string" },
				{ "name": "Born", "units": "date" },
				{ "name": "Died", "units": "date" } ],
			"data": (("Bogart", 1899, 1957),
					 ("Lorre", 1904, 1964),
					 ("Greenstreet", 1879, 1954)),}
		self.xfm = cn.CSVDataTransformer()
		self.environ = {}

	def test_CSV(self):
		res = self.xfm.transform(self.data, self.environ)
		self.assertEqual(b"".join(list(res)),
b"""Name,Born,Died\r
string,date,date\r
Bogart,1899,1957\r
Lorre,1904,1964\r
Greenstreet,1879,1954\r
""")

if __name__ == '__main__':
	unittest.main()
