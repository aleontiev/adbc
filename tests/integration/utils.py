import os
import uuid
from adbc.store import Database

PROMPT = os.environ.get(
    'ADBC_TEST_PROMPT',
    '0'
) == '1'
USER = os.environ.get('ADBC_TEST_USER',
    os.environ.get('USER')
)
PASS = os.environ.get('ADBC_TEST_PASS', USER)
URLS = {
    'postgres': f'postgres://{USER}:{PASS}@localhost:5432/postgres',
    'mysql': f'mysql://{USER}:{PASS}@localhost/mysql',
    'sqlite': 'file:test.sqlite'
}

class setup_test_database(object):
    def __init__(self, name=None, type='postgres', verbose=False):
        self.type = type
        assert type in URLS
        self.name = name
        self.verbose = verbose

    def get_url(self):
        return URLS[self.type]

    async def __aenter__(self):
        self.uid = str(uuid.uuid4())[0:3].replace('-', '')
        name = f'{self.name}_{self.uid}'
        url = self.get_url()
        self.full_name = name
        self.root = Database(
            url=url,
            prompt=PROMPT
        )
        self.host = self.root.host
        if self.host.file:
            # for file hosts (sqlite), use this same database
            self.db = self.root
            self.db.prompt = PROMPT
            self.db.verbose = self.verbose
            self.db.tag = self.name
        else:
            # for network hosts (mysql/postgres), create a new database
            await self.root.create_database(name)
            url = '/'.join(url.split('/')[:-1]) + f'/{name}'
            self.db = Database(url=url, prompt=PROMPT, verbose=self.verbose, tag=self.name)
        return self.db

    async def __aexit__(self, *args):
        await self.db.close()
        if self.host.file:
            if os.path.exists(self.host.name):
                os.remove(self.host.name)
        else:
            await self.root.drop_database(self.full_name)
            await self.root.close()
