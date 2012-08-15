"""WSGI entry point"""

import logging
import logging.config

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logging.config.fileConfig("logging.conf") # Logger configs are *not*
                                          # retroactive. This should
                                          # happen as early as
                                          # possible.

from selector import Selector

import fuse.config
import fuse.muddleware as muddleware
import fuse.api
import fuse.db

def get_app(conf=fuse.config):
	# Create the mapper object: the core decision-maker
	mapper = Selector()

	# Middleware called in this order, before the mapper
	application = muddleware.ExceptionHandler(mapper)

	# Set up the various components
	fuse.api.APIWrapper(conf, fuse.db.get_database(conf), mapper)

	return application
