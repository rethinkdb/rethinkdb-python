import sys

collect_ignore = []

if sys.version_info < (3, 6):
    collect_ignore += ["integration/test_asyncio.py",
                       "integration/test_tornado.py"]

if sys.version_info < (3, 4):
    collect_ignore += ["integration/test_asyncio_coroutine.py"]
