import pytest
import copy
import os
from datetime import datetime

from adbc.testing import setup_test_database

from adbc.generators import G
from adbc.symbols import delete, insert

VERBOSE = os.environ.get('TEST_VERBOSE', False)

@pytest.mark.asyncio
async def test_copy():
    # 0. define constants
    timestamp_column = G('column', type='timestamp with time zone', null=True)
    test_size = 1000
    copy_size = 1000
    other_size = 100
    table_definition = G(
        'table',
        columns={
            'id': G('column', type='integer', primary='test_id'),
            'slug': G('column', type='text', null=True),
            'name': G('column', type='text')
        },
        constraints={
            'test_id': G('constraint', type='primary', columns=['id'])
        },
        indexes={
            'test_id': G('index', type='btree', primary=True, unique=True, columns=['id'])
        }
    )
    target_definition = G(
        'table',
        columns={
            'id': G('column', type='integer', primary='test_id'),
            'slug': G('column', type='text', null=True),
            'anombre': G('column', type='text')
        },
        constraints={'test_id': G('constraint', type='primary', columns=['id'])},
        indexes={'test_id': G('index', type='btree', primary=True, unique=True, columns=['id'])}
    )
    copy_definition = G(
        'table',
        columns={'id': G('column', type='integer', primary='copy_id'), 'name': G('column', type='text')},
        constraints={'copy_id': G('constraint', type='primary', columns=['id'])},
        indexes={'copy_id': G('index', type='btree', primary=True, unique=True, columns=['id'])}
    )
    other_definition = G(
        'table',
        columns={
            'id': G('column', type='integer', primary='other_id'), 'name': G('column', type='text')
        },
        constraints={
            'other_id': G('constraint', type='primary', columns=['id'])
        },
        indexes={
            'other_id': G('index', type='btree', primary=True, unique=True, columns=['id'])
        }
    )
    unique_constraint = G('constraint', type='unique', columns=['slug'])
    unique_index = G('index', type='btree', unique=True, columns=['slug'])

    # scope includes "copy" and "test" but not "other"
    scope = {
        "schemas": {
            "main": {
                "source": "public",
                "target": "testing",
                "tables": {
                    "copy": True,
                    "test": {
                        "target": "prueba",  # renamed from "test" to "prueba" in "target"
                        "columns": {
                            "*": True,
                            "name": {
                                "target": "anombre"  # renamed from "name" to "anombre"
                            }
                        }
                    }
                }
            }
        }
    }

    # 1. setup test databases
    async with setup_test_database("source", verbose=VERBOSE) as source:
        async with setup_test_database("target", verbose=VERBOSE) as target:
            # 2. create test schematic elements: table in source and target
            # the table called "copy" will remain the same on both source/target after initial setup
            # the table called "test" will change on source during the test
            await source.create_table("test", table_definition)
            await source.create_table("copy", copy_definition)
            await source.create_table("other", other_definition)

            # 3. add data
            source_model = await source.get_model("test")
            source_copy = await source.get_model('copy')
            source_other = await source.get_model('other')
            source_table = source_model.table

            # add (INSERT)
            for model, size in (
                (source_model, test_size),
                (source_copy, copy_size),
                (source_other, other_size)
            ):
                # PreQL to generate series (cannot use model interface to do this)
                query = {
                    'insert': {
                        'table': model.table.full_name,
                        'columns': ['id', 'name'],
                        'values': {
                            'select': {
                                'data': ['S', {'name': {'concat': ['`name`', 'S']}}],
                                'from': {
                                    'S': {
                                        'generate_series': [1, size]
                                    }
                                }
                            }
                        }
                    }
                }
                await source.execute(query)

            # create a target entry in a different schema
            await target.create_schema('other')
            await target.create_table("test", target_definition, schema='other')
            other_model = await target.get_model('test', schema='other')
            await other_model.values({'id': 1, 'anombre': 'other-1'}).add()

            # 4. run copy to populate target
            data = await source.copy(target, scope=scope)
            assert data == {
                'schema_changes': {
                    insert: {
                        'testing': {
                            'prueba': target_definition,
                            'copy': copy_definition
                        }
                    }
                },
                'data_changes': {
                    'testing': {
                        'copy': {
                            'copied': copy_size,
                            'skipped': 0
                        },
                        'prueba': {
                            'copied': test_size,
                            'skipped': 0
                        }
                    }
                },
                'final_diff': {}
            }

            # check to make sure "test" was moved to "prueba"
            target_model = await target.get_model('prueba', schema='testing')
            assert target_model is not None
            target_count = await target_model.count()
            assert target_count == test_size

            # check to make sure "other" was not moved
            target_other = None
            try:
                target_other = await target.get_model('other', schema='testing')
            except Exception:
                pass

            assert target_other is None

            # 5. make schema and data changes in source
            await source.create_column(
                "test", "created", timestamp_column
            )
            await source.create_constraint(
                "test", "slug_unique", unique_constraint
            )
            await source.alter_column(
                'test', 'name', patch={'null': True}
            )
            await source_model.key(1).values({'name': 'changed-1'}).set()
            await source_model.key(3).delete()
            await source_model.values({'id': test_size + 1, 'name': 'new'}).add()
            await source.create_column(
                'test', 'updated', timestamp_column
            )

            # reset both databases to re-trigger schema queries
            # before next copy
            source.reset()

            # 6. run copy again
            data = await source.copy(target, scope=scope)

            data_changes = data.pop('data_changes')
            assert data_changes['testing']['copy'] == {
                'copied': 0,
                'skipped': copy_size
            }
            assert len(data_changes.keys()) == 1

            # TODO: validate proper renaming of columns
            # referenced inside of constraints and indexes...

            assert data == {
                'final_diff': {},
                'schema_changes': {
                    'testing': {
                        'prueba': {
                            'columns': {
                                insert: {
                                    'created': timestamp_column,
                                    'updated': timestamp_column
                                },
                                'anombre': {
                                    'null': [False, True]
                                }
                            },
                            'constraints': {
                                insert: {
                                    'slug_unique': unique_constraint
                                }
                            },
                            'indexes': {
                                insert: {
                                    'slug_unique': unique_index
                                }
                            }
                        }
                    }
                },
            }
            # make sure our other model in a different schema is unchanged
            other_count = await other_model.count()
            assert other_count == 1
            record = await other_model.key(1).get()
            assert dict(record) == {'id': 1, 'anombre': 'other-1', 'slug': None}
