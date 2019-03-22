import sys

collect_ignore = []
if sys.version_info < (3, 4):
    collect_ignore.extend([
        "integration/test_asyncio_coroutine.py",
        "integration/test_dump.py",
        "integration/test_restore.py",
    ])
if sys.version_info < (3, 6):
    collect_ignore.append("integration/test_asyncio.py")
