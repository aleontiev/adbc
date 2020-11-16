import logging


class Loggable(object):
    def __init__(self, *args, **kwargs):
        logger = kwargs.pop('logger', None)
        name = getattr(self, 'name', '')
        name = f'{self.__class__.__module__}:{name}'
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger(
                kwargs.get('log_name', getattr(self, '_log_name', name))
            )
            self._logger.setLevel(
                kwargs.get('log_level', getattr(self, '_log_level', logging.INFO))
            )

    def log(self, *args, **kwargs):
        if self.verbose:
            print(*args)
        else:
            self._logger.debug(*args, **kwargs)
