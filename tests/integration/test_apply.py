import pytest
import os
from adbc.testing import setup_test_database
from copy import deepcopy
from adbc.generators import G


VERBOSE = os.environ.get('TEST_VERBOSE', False)


@pytest.mark.asyncio
async def test_apply():
    # 0. define constants
    timestamp_column = G('column', type='timestamp with time zone', null=True)
    unique_name = G(
        'constraint',
        type='unique',
        columns=['name']
    )
    table_definition = {
        "columns": {
            "id": G('column', type='integer', primary='test__id__pk', sequence=True),
            "name": G('column', type='text', null=True)
        },
        "constraints": {
            "test__id__pk": G('constraint',
                type='primary',
                columns=['id']
            )
        }
    }
    schema = {
        "main": {
            "test": table_definition
        }
    }
    # setting scope to "main" ensures we do not try to remove
    # any of the special schemas in MySQL/Postgres (e.g. information_schema)
    # this is not necessary for SQLite
    scope = {
        "schemas": {
            "main": True
        }
    }
    # 1. setup test database
    for type in ('postgres', 'sqlite'):
        async with setup_test_database("source", type=type, verbose=VERBOSE) as source:
            # 2. create test schematic elements using Database.apply

            await source.apply(schema, scope=scope)
            # 3. read the table back out and verify schema
            model = await source.get_model("test", schema='main')
            table = model.table

            assert table is not None
            assert table.pk == 'id'
            assert len(table.pks) == 1
            assert next(iter(table.pks)) == 'id'
            columns = table_definition['columns']
            if type == 'postgres':
                columns = deepcopy(columns)
                columns['id']['sequence'] = 'main.test__id__seq'
                columns['id']['default'] = {
                    'nextval': "'main.test__id__seq'"
                }
            elif type == 'sqlite':
                columns = deepcopy(columns)
                columns['id']['primary'] = True
            assert table.columns == columns
