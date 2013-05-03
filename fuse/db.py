"""Database connection
"""

import importlib

_impl = None
_DB = None

def get_database(config):
	"""Return a database based on the passed config object, or a
	previously-cached database.
	"""
	global _impl, _DB
	if _impl is None:
		_impl = importlib.import_module("fuse.db_" + config.db_type)
	if _DB is None:
		_DB = _impl.Database(config)
	return _DB

def _parse_data(data, meta):
	"""This generator converts a sorted sequence of (stamp, sid,
	value) tuples into a crosstab of values by stamp (records) and sid
	(fields). It is a helper for the crosstab nature of the
	get_values() method."""
	metamap = { m[0]: i for i, m in enumerate(meta) }
	it = iter(data)
	line = next(it)
	while True:
		try:
			accum = [line[0]] + [None]*len(meta)
			while line[0] == accum[0]:
				sid = line[1]
				col = metamap[sid]+1
				accum[col] = line[2]
				line = next(it)
		except StopIteration:
			yield accum
			raise StopIteration
		yield accum
