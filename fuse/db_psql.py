"""Postgresql database interface object
"""

import logging
import datetime

import psycopg2

import fuse.db

CURRENT_VERSION = 1
_UTC = datetime.timezone.utc

log = logging.getLogger("db_psql")

class Database(object):
	def __init__(self, conf):
		self.db = psycopg2.connect(**conf.db_params)
		self.db.autocommit = True # Default to autocommit on
		ver = self._db_version()
		if ver != CURRENT_VERSION:
			self._upgrade(ver)

	def create_series(self,
					  name,
					  period,
					  epoch=datetime.datetime(1970, 1, 1, tzinfo=_UTC),
					  ts_type="point",
					  unit="",
					  get_limit=1000,
					  description=""):
		"""Create a time-series. Return the ID of the series created.
		"""
		if ts_type not in ("point", "mean", "stdev", "count"):
			log.error("Series creation failed: type \"%s\" not recognised", ts_type)
			return None

		self.db.commit()
		self.db.autocommit = False
		try:
			self._query(
				"""
				insert into series (name, description, units, period, epoch,
									ts_type, get_limit)
				values (%s, %s, %s, %s, %s, %s, %s)
				""", (name, description, unit, period, epoch,
					  ts_type, get_limit))
			rv = self._query("select lastval()").fetchone()[0]
			self.db.commit()
		except psycopg2.DatabaseError as ex:
			self.db.rollback()
			log.error("Series creation failed: name=%s, units=%s, period=%s,"
					  + " epoch=%s, ts_type=%s, limit=%s",
					  name, unit, period, epoch, ts_type, get_limit,
					  exc_info=ex)
			rv = None

		self.db.autocommit = True
		return rv

	def drop_series(self, sid):
		"""Drop a time-series with the given series ID.
		"""
		self._query("delete from series where id=%s", (sid,))

	def list_series(self, sid=None, period=None, ts_type=None, name=None,
					unit=None):
		"""List the available time-series
		"""
		sql = "select id, name, description, period, epoch, ts_type, "
		sql += " get_limit, units from series where 1=1"
		params = []
		if sid is not None:
			sql += " and id=%s"
			params.append(sid)
		if period is not None:
			try:
				l = period[0:2]
				sqladd = " and period >= %s and period < %s"
			except TypeError:
				l = [period]
				sqladd = " and period = %s"
			except IndexError:
				l = period[0:1]
				sqladd = " and period = %s"
			params += l
			sql += sqladd
		if ts_type is not None:
			sql += " and (" + " or ".join(["ts_type = %s"]*len(ts_type)) + ")"
			params += ts_type
		if name is not None:
			sql += " and name ilike %s"
			params.append("%{0}%".format(name))
		if unit is not None:
			sql += " and (" + " or ".join(["lower(units) = %s"]*len(unit)) + ")"
			params += unit
			# FIXME: We really need a lookup table from unique
			# sanitised unit IDs to unit names, rather than this hack
			# with lower()

		cur = self._query(sql, params)
		return { r[0]: { "id": r[0],
						 "name": r[1],
						 "description": r[2],
						 "period": r[3],
						 "epoch": r[4],
						 "ts_type": r[5],
						 "limit": r[6],
						 "units": r[7],
						 }
				 for r in cur }

	def is_series(self, sid):
		"""Check whether sid is a series
		"""
		cur = self._query("select count(id) from series where id=%s", [sid])
		line = cur.fetchone()
		return line[0] > 0

	def add_value(self, sid, ts, value):
		# FIXME: We should check for data points falling on
		# appropriate times for the epoch/period for this data point

		# FIXME: This is not an ideal interface, as we start/stop a
		# transaction for every single data point. There should be a
		# bulk upload interface as well. See (e.g.)
		# http://stackoverflow.com/questions/1109061/insert-on-duplicate-update-postgresql
		# for bulk-upload solutions.

		# Ideally, we should use MERGE here, but we can't because psql
		# doesn't support it yet. So instead, we use a predefined SQL
		# function as found in the manual: http://www.postgresql.org/docs/current/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
		now = datetime.datetime.now(_UTC)
		self.db.commit()
		self.db.autocommit = False
		try:
			self._query("select upsert_data(%s, %s, %s, %s)",
						(sid, ts, now, value))
			self.db.commit()
			rv = True
		except psycopg2.DatabaseError as ex:
			log.error("Failed to insert/update data: id=%s, time=%s, value=%s",
					  sid, ts, value, exc_info=ex)
			self.db.rollback()
			rv = False

		self.db.autocommit = True
		return rv

	def get_values(self, sids, from_ts=None, to_ts=None):
		"""Return a sorted iterator of (ts, value, value...) tuples
		from the given list of series
		"""
		# Get the relevant series metadata
		qry = "select id, name, ts_type, units from series where "
		qry += "(" + " or ".join(["id = %s"]*len(sids)) + ")"
		params = sids[:]
		meta = list(self._query(qry, params))

		qry = "select stamp, series_id, value from data where "
		qry += "(" + " or ".join(["series_id = %s"]*len(sids)) + ")"
		params = sids[:]
		if from_ts is not None:
			qry += " and stamp >= %s"
			params.append(from_ts)
		if to_ts is not None:
			qry += " and stamp < %s"
			params.append(to_ts)
		qry += " order by stamp, series_id"

		cur = self._query(qry, params)

		return {
			"meta": [{"name": "Date", "units": "date/time"}]
					+ [{"id": m[0], "name": m[1], "ts_type": m[2], "units": m[3]}
					   for m in meta],
			"data": list(fuse.db._parse_data(cur, meta)), }

	def facet_summary(self, facet_type):
		"""Return a list of (label, total) pairs for the facet requested
		"""
		qry = "select " + facet_type + ", count(*)" + \
			  "from series group by " + facet_type
		params = [facet_type,]
		cur = self._query(qry, params)
		return ((row[0], row[1]) for row in cur)

	def _wipe(self):
		"""Internal method used by test suite
		"""
		self._query("drop table data")
		self._query("drop table series")
		self._query("drop table version")
		# For psql < 9.1
		#self._query("drop language plpgsql cascade")
		# For psql >= 9.1
		##self._query("drop extension if exists plgqsql casscade")

	def _db_version(self):
		"""Check and return the current version of this DB. If the
		version is older than our expected one, upgrade automatically.
		"""
		res = self._query(
			"""SELECT COUNT(tablename) AS nvers
			FROM pg_tables
			WHERE schemaname='public'
			  AND tablename='version'
			""")
		row = res.fetchone()
		if row[0] == 0:
			return 0
		res = self._query("""SELECT version FROM version""")
		row = res.fetchone()
		return row[0]

	def _query(self, sql, params=[]):
		"""Perform a query, returning the cursor with results in it.
		"""
		cur = self.db.cursor()
		cur.execute(sql, params)
		return cur

	def _upgrade(self, from_ver):
		"""Upgrade a database from an earlier version of the DB
		structure. If from_ver is 0, create the structure from
		scratch.
		"""
		log.info("Upgrade required from %s to %s", from_ver, CURRENT_VERSION)
		if from_ver <= 0:
			log.info("Creating new database structure")
			self.db.autocommit = False
			try:
				cur = self.db.cursor()
				cur.execute("create table version (version integer)")
				cur.execute("insert into version values (1)")
				# For psql < 9.1
				#cur.execute("create or replace language plpgsql")
				# For psql >= 9.1
				##cur.execute("create extension if not exists plpgsql")
				cur.execute(
					"""
					create table series (
					  id serial primary key,
					  name varchar,
					  description varchar,
					  units varchar(20),
					  period interval,
					  epoch timestamp with time zone,
					  ts_type varchar(10),
					  get_limit integer)
					""")
				cur.execute(
					"""
					create table data (
					  series_id integer references series (id)
					                    on delete cascade
										on update cascade,
					  stamp timestamp with time zone,
					  ingest timestamp with time zone,
					  value double precision,
					  primary key (series_id, stamp))
					""")
				cur.execute(
					"""
					create or replace function upsert_data(
						sid integer,
						datatime timestamp with time zone,
						ingesttime timestamp with time zone,
						datavalue double precision)
					returns void as
					$$
					begin
						loop
							update data set ingest=ingesttime, value=datavalue
								where series_id=sid and stamp=datatime;
							if found then
								return;
							end if;
							begin
								insert into data (series_id, stamp,
												  ingest, value)
									values (sid, datatime,
											ingesttime, datavalue);
								return;
							exception when unique_violation then
							end;
						end loop;
					end;
					$$
					language plpgsql;
					""")

				self.db.commit()
			except psycopg2.DatabaseError as ex:
				log.error("Failed to create database structure", exc_info=ex)
				self.db.rollback()

			self.db.autocommit = True

			return 0

		#if from_ver <= 1:
		#	"""Upgrade from version 1 tables to (current|next) version
		#	"""
		#	from_ver += 1
		#
		#if from_ver <= 2:
		#	pass
		# etc...
