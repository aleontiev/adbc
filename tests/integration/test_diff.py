import pytest
import copy
from .utils import setup_test_database
from adbc.generators import G

from jsondiff.symbols import delete, insert


@pytest.mark.asyncio
async def test_diff():
    # 0. define constants
    timestamp_column = G('column', type='timestamp with time zone')
    unique_constraint = G('constraint', type='unique', columns=['name'])
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
            # 2. create test schematic elements: table in both source and target
            # the table called "copy" will remain the same on both source/target after initial setup
            # the table called "test" will change on both source/target throughout the test
            await source.create_table("test", table_definition)
            await source.create_table("copy", copy_definition)
            await target.create_schema('testing')
            await target.create_table('test', table_definition, schema='testing')
            await target.create_table('copy', copy_definition, schema='testing')

            # 3. add data
            source_model = await source.get_model("test", schema='public')
            source_copy = await source.get_model('public.copy')
            source_table = source_model.table

            target_model = await target.get_model('testing.test')
            target_copy = await target.get_model('testing.copy')
            target_table = target_model.table

            # add (INSERT)
            for model in (source_model, target_model, source_copy, target_copy):
                await model.body({"id": 1, "name": "Jay"}).take("*").add()
                await model.body({"id": 2, "name": "Quinn"}).add()
                await model.body({"id": 3, "name": "Hu"}).add()

            # 4. check diff -> expect none
            diff = await source.diff(target, scope=scope)
            assert diff == {}

            # 5. make changes in source
            await source.create_column(
                "test", "created", timestamp_column
            )
            await source.create_constraint(
                "test", "name_unique", unique_constraint
            )

            # 6. make changes in target
            await target_model.body([{"id": 6, "name": "Jim"}, {"id": 5, "name": "Jane"}]).add()
            await target.create_column(
                'test', 'updated', timestamp_column, schema='testing'
            )
            await target.alter_column('test', 'name', patch={'null': False}, schema='testing')

            # 7. diff and validate changes
            diff = await source.diff(target, scope=scope)

            assert diff == {
                'main': {
                    'test': {
                        'data': {
                            'range': {
                                'id': {
                                    'max': [3, 6]
                                }
                            },
                            'count': [3, 5]
                        },
                        'schema': {
                            'columns': {
                                insert: {
                                    'updated': timestamp_column
                                },
                                delete: {
                                    'created': timestamp_column
                                },
                                'name': {
                                    'null': [True, False]
                                }
                            },
                            'constraints': {
                                delete: {
                                    'name_unique': unique_constraint
                                }
                            },
                            'indexes': {
                                delete: {
                                    'name_unique': unique_index
                                }
                            }
                        }
                    }
                }
            }
