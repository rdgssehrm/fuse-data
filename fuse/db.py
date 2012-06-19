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
