import multiprocessing
import os
import signal
import time

import pytest
from mock import Mock

from rethinkdb._import import reap_worker


def _sleep():
    time.sleep(30)


@pytest.mark.unit
class TestReapWorker(object):
    def test_clean_exit_reports_nothing(self):
        error_queue = Mock()
        worker = Mock(exitcode=0)
        worker.name = "table reader 0"

        reap_worker(worker, error_queue)

        error_queue.put.assert_not_called()

    @pytest.mark.parametrize("exitcode", [1, 2])
    def test_worker_that_raised_is_left_to_report_itself(self, exitcode):
        error_queue = Mock()
        worker = Mock(exitcode=exitcode)
        worker.name = "table reader 0"

        reap_worker(worker, error_queue)

        error_queue.put.assert_not_called()

    @pytest.mark.parametrize("signal_number", [signal.SIGKILL, signal.SIGTERM])
    def test_killed_worker_is_reported(self, signal_number):
        error_queue = Mock()
        worker = Mock(exitcode=-signal_number)
        worker.name = "table writer 1"

        reap_worker(worker, error_queue)

        error_queue.put.assert_called_once()
        error = error_queue.put.call_args[0][0]
        assert "table writer 1" in error.message
        assert str(signal_number) in error.message

    def test_killed_process_is_not_alive_but_is_reported(self):
        error_queue = Mock()
        worker = multiprocessing.Process(target=_sleep, name="table reader 0")
        worker.start()
        os.kill(worker.pid, signal.SIGKILL)
        worker.join()

        assert worker.is_alive() is False

        reap_worker(worker, error_queue)

        error_queue.put.assert_called_once()
