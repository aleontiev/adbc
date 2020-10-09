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
    'sqlite': 'file:test.db'
}

def get_uid(size=3):
    return str(uuid.uuid4())[0:size].replace('-', '')


class setup_test_database(object):
    def __init__(self, name=None, type='postgres', verbose=False, url=None):
        self.type = type
        assert type in URLS
        self.name = name
        self.url = url
        self.verbose = verbose

    def get_url(self):
        return self.url if self.url else URLS[self.type]

    async def __aenter__(self):
        self.uid = get_uid()
        name = f'{self.name}_{self.uid}'
        url = self.get_url()
        self.full_name = name
        if url.startswith('file:'):
            # for file hosts (sqlite), make a temp copy of this database
            # do this by appending the uid to the base name
            # ... but only if url was not passed explicitly
            url = url if self.url else f'{url}-{self.uid}'
            self.root = self.db = Database(
                url=url, prompt=PROMPT, verbose=self.verbose, tag=self.name
            )
        else:
            # for network hosts (mysql/postgres), create a new database
            self.root = Database(
                url=url,
                prompt=PROMPT
            )
            await self.root.create_database(name)
            url = '/'.join(url.split('/')[:-1]) + f'/{name}'
            self.db = Database(url=url, prompt=PROMPT, verbose=self.verbose, tag=self.name)
        return self.db

    async def __aexit__(self, *args):
        if self.db:
            await self.db.close()
            self.db = None

        if self.root:
            if self.root.host.file:
                if os.path.exists(self.root.host.name):
                    os.remove(self.root.host.name)
            else:
                await self.root.drop_database(self.full_name)
                await self.root.close()
            self.root = None
