"""setting module for your project"""
import os
import logging
import asyncio

import httpx

from ant_nest.exceptions import ExceptionFilter

# your ant`s class modules or packages
ANT_PACKAGES = ["ants"]
ANT_ENV = os.getenv("ANT_ENV", "development")


# httpx config, see httpx.Client.__init__ for more detail
HTTPX_CONFIG = {
    "timeout": 5.0,
    "max_redirects": 20,
    "limits": httpx.Limits(max_connections=100, max_keepalive_connections=20),
    "trust_env": True,
    "proxies": None,
    "auth": None,
    "headers": None,
    "cookies": None,
}


POOL_CONFIG = {"limit": 100, "timeout": 60}
REPORTER = {
    "slot": 60,
}


# ANT config
HTTP_RETRIES = 0
HTTP_RETRY_DELAY = 0.1


# Persistent state for crash recovery (disabled by default).
# - enabled: switch the stateful pipelines on/off
# - dir: directory holding state.db, state.db.bak and the items/ json-lines
#   output files; deleting it or calling StateStorage.reset() starts clean
# - mode: "resume" restores the last good checkpoint, "full" wipes all
#   historical fingerprints, items and checkpoints before crawling
# - checkpoint_interval: durable writes between two full checkpoint backups
PERSISTENT_STATE = {
    "enabled": False,
    "dir": os.path.join(os.getcwd(), ".ant_nest_state"),
    "mode": "resume",
    "checkpoint_interval": 100,
}


if ANT_ENV in ("development", "testing"):
    logging.basicConfig(level=logging.DEBUG)
    asyncio.get_event_loop().set_debug(True)
else:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().addFilter(ExceptionFilter())


# custom setting, eg:
# MYSQL_HOST = '127.0.0.1'
