"""Postgresql database interface object
"""

import logging
import datetime

import psycopg2

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
					  period,
					  epoch=datetime.datetime(1970, 1, 1, tzinfo=_UTC),
					  ts_type="point",
					  get_limit=1000):
		"""Create a time-series. Return the ID of the series created.
		"""
		self.db.commit()
		self.db.autocommit = False
		try:
			self._query(
				"""
				insert into series (period, epoch, ts_type, get_limit)
				values (%s, %s, %s, %s)
				""", (period, epoch, ts_type, get_limit))
			rv = self._query("select lastval()").fetchone()[0]
			self.db.commit()
		except psycopg2.DatabaseError as ex:
			self.db.rollback()
			log.error("Series creation failed: period=%s, epoch=%s, type=%s, limit=%s", period, epoch, ts_type, get_limit, exc_info=ex)
			rv = None

		self.db.autocommit = True
		return rv

	def drop_series(self, sid):
		"""Drop a time-series with the given series ID.
		"""
		self._query("delete from series where id=%s", (sid,))

	def list_series(self, sid=None, period=None, ts_type=None):
		"""List the available time-series
		"""
		sql = "select id, period, epoch, ts_type, get_limit from series where 1=1"
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
			sql += " and ts_type = %s"
			params.append(ts_type)

		cur = self._query(sql, params)
		return { r[0]: { "id": r[0],
						 "period": r[1],
						 "epoch": r[2],
						 "type": r[3],
						 "limit": r[4], }
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

	def get_values(self, sid, from_ts=None, to_ts=None):
		"""Return a sorted iterator of (ts, value) pairs from the given series
		"""
		qry = "select stamp, value from data where series_id = %s"
		params = [sid,]
		if from_ts is not None:
			qry += " and stamp >= %s"
			params.append(from_ts)
		if to_ts is not None:
			qry += " and stamp < %s"
			params.append(to_ts)
		qry += " order by stamp"

		cur = self._query(qry, params)
		return ((row[0], row[1]) for row in cur)

	def _wipe(self):
		"""Internal method used by test suite
		"""
		self._query("drop table data")
		self._query("drop table series")
		self._query("drop table version")
		self._query("drop language plpgsql cascade")

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
			log.info("Creating new database")
			self.db.autocommit = False
			try:
				cur = self.db.cursor()
				cur.execute("create table version (version integer)")
				cur.execute("insert into version values (1)")
				cur.execute("create language plpgsql")
				#cur.execute("create extension if not exists plpgsql")
				cur.execute(
					"""
					create table series (
					  id serial primary key,
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
					create function upsert_data(
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
