import pytest
import copy
from .utils import setup_test_database
from adbc.generators import G

from jsondiff.symbols import delete, insert


@pytest.mark.asyncio
async def test_copy():
    # 0. define constants
    timestamp_column = G('column', type='timestamp with time zone')
    unique_constraint = G('constraint', type='unique', columns=['name'])
    test_size = 100
    table_definition = G(
        'table',
        schema={
            'columns': {'id': G('column', type='integer'), 'name': G('column', type='text')},
            'constraints': {'test_id': G('constraint', type='primary', columns=['id'])}
        }
    )
    copy_definition = G(
        'table',
        schema={
            'columns': {'id': G('column', type='integer'), 'name': G('column', type='text')},
            'constraints': {'copy_id': G('constraint', type='primary', columns=['id'])}
        }
    )
    unique_index = G(
        'index',
        columns=['name'],
        type='btree',
        unique=True
    )
    scope = {"schemas": {"main": {"source": "public", "target": "testing"}}}

    # 1. setup test databases
    async with setup_test_database("source", verbose=True) as source:
        async with setup_test_database("target", verbose=True) as target:
            # 2. create test schematic elements: table in source and target
            # the table called "copy" will remain the same on both source/target after initial setup
            # the table called "test" will change on source during the test
            await source.create_table("test", table_definition)
            await source.create_table("copy", copy_definition)

            # 3. add data
            source_model = await source.get_model("test", schema='public')
            source_copy = await source.get_model('public.copy')
            source_table = source_model.table

            # add (INSERT)
            for model in (source_model, source_copy):
                # bypass model interface for generate series
                table = source.F.table(model.table.name)
                query = f"INSERT INTO {table} (id, name) SELECT S, concat('name', S) FROM generate_series(1, {test_size}) S"
                await source.execute(query)

            # 4. run copy to populate target
            data = await source.copy(target, scope=scope)
            assert data == {}

            # 5. make schema and data changes in source
            await source.create_column(
                "test", "created", timestamp_column
            )
            await source.create_constraint(
                "test", "name_unique", unique_constraint
            )
            await source.alter_column(
                'test', 'name', patch={'null': False}
            )
            await source_model.key(1).body({'name': 'changed-1'}).set()
            await source_model.key(3).delete()
            await source_model.body({'id': test_size + 1, 'name': 'new'}).add()
            await source.create_column(
                'test', 'updated', timestamp_column, schema='testing'
            )

            # 6. run copy again
            data = await source.copy(target, scope=scope)

            assert data == {}
