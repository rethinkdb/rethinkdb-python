"""
Test configuration for pytest.
"""

import pytest

# Configure pytest-asyncio for tornado tests
pytest_plugins = ["pytest_asyncio"]


# Mark all tests in test_net_tornado.py as asyncio tests (except unit tests)
# Don't mark twisted tests as asyncio (they use inlineCallbacks)
def pytest_collection_modifyitems(config, items):
    for item in items:
        if "test_net_tornado" in item.nodeid:
            # Don't mark unit tests as asyncio
            if not any(marker.name == "unit" for marker in item.iter_markers()):
                item.add_marker(pytest.mark.asyncio)
