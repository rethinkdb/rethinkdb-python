import sys

collect_ignore = []

if sys.version_info < (3, 6):
    collect_ignore += [
        "integration/test_asyncio.py",
        "integration/test_asyncio_coroutine.py",
        "integration/test_tornado.py",
        "integration/test_trio.py",
    ]
