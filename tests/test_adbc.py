from adbc import __version__
from .utils import setup_test_database


def test_version():
    assert __version__ == '0.1.0'

def test_postgres_integration():
    async with setup_test_database('source') as source:
        async with setup_test_database('target') as target:

            target = setup_test_database('target')
