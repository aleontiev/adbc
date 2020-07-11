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


class setup_test_database(object):
    def __init__(self, name=None, verbose=False):
        self.name = name
        self.verbose = verbose

    async def __aenter__(self):
        self.uid = str(uuid.uuid4())[0:12].replace('-', '')
        name = f'{self.name}_{self.uid}'
        self.full_name = name
        test_database = Database(url=TEST_DATABASE_URL, prompt=PROMPT)
        self.root = test_database
        await self.root.create_database(name)
        # TODO: more robust DB name replacement
        url = f'{TEST_DATABASE_HOST}/{name}'
        self.db = Database(url=url, prompt=PROMPT, verbose=self.verbose, tag=self.name)
        return self.db

    async def __aexit__(self, *args):
        await self.db.close()
        await self.root.drop_database(self.full_name)
