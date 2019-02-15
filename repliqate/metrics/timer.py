import time


class DurationTimerContextManager(object):
    """
    Context manager for timing an execution duration.
    """

    def __init__(self, duration_cb):
        """
        Create a context manager instance.

        :param duration_cb: Callback function invoked with the duration, in milliseconds, of the
                            context manager body when complete.
        """
        self.duration_cb = duration_cb

    def __enter__(self):
        self.start_ms = 1000.0 * time.time()

    def __exit__(self, *args, **kwargs):
        end_ms = 1000.0 * time.time()

        self.duration_cb(end_ms - self.start_ms)


class ExecutionTimer(object):
    """
    Usage abstraction for a stateful execution duration timer.
    """

    def __init__(self):
        """
        Create a timer factory.
        """
        self.last_duration = -1

    def duration(self):
        """
        Retrieve the duration of the most recent timer.

        :return: Recorded duration of the most recent completed timer if available; -1 otherwise.
        """
        return self.last_duration

    def timer(self):
        """
        Factory for creating a duration timer context manager.

        :return: Context manager instance.
        """
        def callback(duration):
            self.last_duration = duration

        return DurationTimerContextManager(callback)
