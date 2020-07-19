import logging


class Loggable(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(
            kwargs.get('log_name', getattr(self, '_log_name', __name__))
        )
        self._logger.setLevel(
            kwargs.get('log_level', getattr(self, '_log_level', logging.INFO))
        )

    def log(self, *args, **kwargs):
        method = 'info' if self.verbose else 'debug'
        log = getattr(self._logger, method)
        return log(*args, **kwargs) if log else None
