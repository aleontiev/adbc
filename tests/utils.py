import unittest
import os
import uuid
from adbc.store import Database

TEST_DATABASE_NAME = os.environ.get(
    'TEST_DATABASE_NAME',
    'postgres'
)
USER = os.environ.get('USER')
TEST_DATABASE_HOST = os.environ.get(
    'TEST_DATABASE_HOST',
    f'postgres://{USER}:{USER}@localhost'
)
PROMPT = os.environ.get(
    'TEST_DATABASE_PROMPT',
    '0'
) == '1'

TEST_DATABASE_URL = f'{TEST_DATABASE_HOST}/{TEST_DATABASE_NAME}'
test_database = Database(url=TEST_DATABASE_URL, prompt=PROMPT)


class setup_test_database(object):
    def __init__(self, name=None, verbose=False):
        self.name = name
        self.verbose = verbose

    async def __aenter__(self):
        self.uid = str(uuid.uuid4())[0:6]
        name = f'{self.name}_{self.uid}'
        self.full_name = name
        await test_database.create_database(name)
        # TODO: more robust DB name replacement
        url = f'{TEST_DATABASE_HOST}/{name}'
        self.db = Database(url=url, prompt=PROMPT, verbose=self.verbose)
        return self.db

    async def __aexit__(self, *args):
        await self.db.close()
        await test_database.drop_database(self.full_name)


class TestCase(object):
    """
    A fake TestCase which allows the user to use assert*
    methods without subclassing `unittest.TestCase`.
    """
    __unittest = None

    def __getattr__(self, k):
        if k.startswith("assert"):
            if self.__unittest is None:
                self.__unittest = unittest.TestCase()
            return getattr(self.__unittest, k)
        raise AttributeError(k)
