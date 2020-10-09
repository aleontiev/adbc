import pytest
import copy
from adbc.generators import G
from adbc.symbols import insert, delete
from adbc.testing import setup_test_database


@pytest.mark.asyncio
async def test_diff():
    # 0. define constants
    timestamp_column = G("column", type="timestamp with time zone", null=True)
    unique_constraint = G("constraint", type="unique", columns=["name"])
    source_definition = G(
        "table",
        columns={
            "id": G("column", type="integer"),
            "name": G("column", type="text", null=True),
            "bleh": G("column", type="text", null=True)
        },
        constraints={"test_id": G("constraint", type="primary", columns=["id"])},
    )
    target_definition = G(
        "table",
        columns={
            "id": G("column", type="integer"),
            "name": G("column", type="text", null=True),
            "meh": G("column", type="text", null=True)
        },
        constraints={"test_id": G("constraint", type="primary", columns=["id"])}
    )
    copy_definition = G(
        "table",
        columns={
            "id": G("column", type="integer"),
            "name": G("column", type="text", null=True),
        },
        constraints={"copy_id": G("constraint", type="primary", columns=["id"])}
    )
    unique_index = G("index", columns=["name"], type="btree", unique=True)
    scope = {
        "schemas": {
            "main": { # alias schema name: main
                "source": "public",  # actually refers to "public" schema in source
                "target": "testing", # actually refers to "testing" schema in target
                "tables": {
                    "*": True,
                    "test": { # alias table name: test
                        "target": "test2",  # actually refers to "test2" in target
                        "columns": {
                            "*": True,
                            "log": { # alias column name: log
                                "source": "bleh",  # actually refers to "meh" in target
                                "target": "meh"   # actually refers to "bleh" in source
                            }
                        }
                    }
                }
            }
        }
    }

    # 1. setup test databases
    async with setup_test_database("source", verbose=True) as source:
        async with setup_test_database("target", verbose=True) as target:
            # 2. create test schematic elements: table in both source and target
            # the table called "copy" will remain the same on both source/target after initial setup
            # the table called "test" will change on both source/target throughout the test
            await source.create_table("test", source_definition)
            await source.create_table("copy", copy_definition)
            await target.create_schema("testing")
            await target.create_table("test2", target_definition, schema="testing")
            await target.create_table("copy", copy_definition, schema="testing")

            # 3. add data
            source_model = await source.get_model("test")
            source_copy = await source.get_model("copy")
            source_table = source_model.table

            target_model = await target.get_model("test2", schema='testing')
            target_copy = await target.get_model("copy", schema='testing')
            target_table = target_model.table

            # add (INSERT)
            for model, field in (
                (source_model, 'bleh'),
                (source_copy, None),
                (target_model, 'meh'),
                (target_copy, None)
            ):
                values = {'id': 1, 'name': 'Jay'}
                if field:
                    values[field] = 'test'

                await model.values(values).take("*").add()
                values['id'] = 2
                values['name'] = 'Quinn'
                await model.values(values).add()
                values['id'] = 3
                values['name'] = 'Hu'
                await model.values(values).add()

            # 4. check diff -> expect none
            diff = await source.diff(target, scope=scope, hashes=True)
            assert diff == {}

            # 5. make changes in source
            await source.create_column("test", "created", timestamp_column)
            await source.create_constraint("test", "name_unique", unique_constraint)

            # 6. make changes in target
            await target_model.values(
                [{"id": 10, "name": "Jim"}, {"id": 9, "name": "Jane"}]
            ).add()
            await target.create_column(
                "test2", "updated", timestamp_column, schema="testing"
            )
            await target.alter_column(
                "test2", "name", patch={"null": False}, schema="testing"
            )

            # 7. diff and validate changes

            # reset caches
            target.reset()
            source.reset()

            diff = await source.diff(target, scope=scope, hashes=True)

            assert diff == {
                "main": {
                    "test": {
                        "rows": {
                            "range": {"id": {"max": [3, 10]}},
                            "count": [3, 5],
                            "hashes": {
                                1: ['ccbc0c558e02b662d1d413571332de21', '9e6a96b10278b39d6988001d11a936fb']
                            }
                        },
                        "columns": {
                            insert: {"updated": timestamp_column},
                            delete: {"created": timestamp_column},
                            "name": {
                                "null": [True, False],
                                "unique": ['name_unique', False]
                            },
                        },
                        "constraints": {delete: {"name_unique": unique_constraint}},
                        "indexes": {delete: {"name_unique": unique_index}},
                    }
                }
            }
