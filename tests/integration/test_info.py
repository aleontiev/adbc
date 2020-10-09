import pytest
import json
from .utils import setup_test_database
from copy import deepcopy
from adbc.generators import G


@pytest.mark.asyncio
async def test_info():
    # 0. define constants
    timestamp_column = G('column', type='timestamp with time zone', null=True)
    unique_name = G(
        'constraint',
        type='unique',
        columns=['name']
    )
    related_definition = {
        "columns": {
            "id": G('column', type='integer', primary=True, sequence=True)
        }
    }
    table_definition_ = {
        "columns": {
            "id": G('column', type='integer', primary='test__id__pk', sequence=True),
            "related_id": G('column', type='integer', null=True, related={'to': 'main.related', 'by': ['id']}),
            "name": G('column', type='text', null=True)
        },
        "constraints": {
            "test__id__pk": G('constraint',
                type='primary',
                columns=['id']
            )
            # FK for "related_id" intentionally omitted
        }
    }
    scope = {"schemas": {"main": True}}
    alias_scope = {"schemas": {"main": {"source": "main"}}}
    # 1. setup test database
    for type in ('postgres', 'sqlite'):
        async with setup_test_database("source", type=type, verbose=True) as source:
            table_definition = deepcopy(table_definition_)
            # 2. create test schematic elements: schema and table
            if type != 'sqlite':
                # sqlite does not support create schema and has a default schema "main"
                await source.create_schema("main")

            await source.create_table("related", related_definition, schema="main")
            await source.create_table("test", table_definition, schema="main")

            # 3. get/add/edit/delete data
            model = await source.get_model("test", schema='main')
            table = model.table

            assert table is not None
            assert table.pk == 'id'

            table_definition['columns']['related_id']['related']['name'] = 'test__related_id__fk'
            if type == 'postgres':
                table_definition['columns']['id']['sequence'] = 'main.test__id__seq'
                table_definition['columns']['id']['default'] = {
                    'nextval': "'main.test__id__seq'"
                }
            if type == 'sqlite':
                table_definition['columns']['id']['primary'] = True
                table_definition['columns']['related_id']['related']['to'] = 'related'

            assert table.columns == table_definition['columns']
            # add (INSERT)

            await model.values({"name": "Jay"}).add()
            await model.values({"name": "Quinn"}).add()
            await model.add()

            # count/get (SELECT)

            # simple count
            count = await model.count()
            assert count == 3
            # query count
            query = model.where({
                "or": [
                    {"icontains": ["name", "'ja'"]},
                    {"in": ['id', [3, 999]]}
                ]
            })
            count = await query.count()
            assert count == 2
            results = await query.sort("id").get()
            assert len(results) == 2
            assert dict(results[0]) == {"id": 1, "name": "Jay", 'related_id': None}
            assert dict(results[1]) == {"id": 3, "name": None, 'related_id': None}

            json_results = await query.sort('id').get(json=True)
            # cast to normal dict to ensure json.dumps can handle
            assert json.dumps(json.loads(json_results)) == json.dumps([dict(r) for r in results])

            json_result = await model.key(3).one(json=True)
            assert json.loads(json_result) == {'id': 3, 'name': None, 'related_id': None}

            # UPDATE
            # TODO: use where(id=3) to test where in set
            updated = await model.key(3).values({"name": "Ash"}).set()
            assert updated == 1

            # DELETE
            deleted = await model.key(2).delete()
            assert deleted == 1

            # 5. get database statistics
            info = await source.get_info(scope=scope)
            expect_schema = table_definition
            actual_schema = info["main"]["test"]
            expect_schema['constraints']['test__related_id__fk'] = G(
                'constraint',
                type='foreign',
                columns=['related_id'],
                related_name='main.related' if type != 'sqlite' else 'related',
                related_columns=['id']
            )
            if type == 'postgres':
                seq = info['main']['test__id__seq']
                assert seq == {'value': 3, 'type': 'sequence'}
            actual_data = actual_schema['rows']
            assert expect_schema["columns"] == actual_schema["columns"]
            assert expect_schema["constraints"] == actual_schema["constraints"]
            assert actual_data["count"] == 2
            assert actual_data["range"] == {"id": {"min": 1, "max": 3}}

            # 6. add new schema elements
            table_definition["columns"]["created"] = timestamp_column
            await source.create_column(
                "test", "created", timestamp_column, schema="main"
            )
            if type != 'sqlite':
                table_definition["constraints"]["unique_name"] = unique_name
                await source.create_constraint(
                    "test", "unique_name", unique_name, schema="main"
                )

            # 7. add new data
            await model.values([
                {"id": 6, "name": "Jim"},
                {"id": 5, "name": "Jane"}
            ]).add()

            # 8. get database statistics again with an aliased scope
            # alias scope supports translation during diff/copy
            info = await source.get_info(scope=alias_scope, hashes=True)

            actual_schema = info["main"]["test"]
            actual_data = actual_schema["rows"]
            if type != 'sqlite':
                # expect unique to be set 
                expect_schema['columns']['name']['unique'] = 'unique_name'
            assert expect_schema["columns"] == actual_schema["columns"]
            assert expect_schema["constraints"] == actual_schema["constraints"]
            assert actual_data["count"] == 4
            assert actual_data["range"] == {"id": {"min": 1, "max": 6}}
            assert actual_data['hashes'] == {1: '79bf96eadff2c10ee9c81356d53ef51e'}

            # 9. test exclusion: ignore certain fields
            excludes = ['unique', 'primary', 'related', 'type']
            info = await source.get_info(
                scope=alias_scope,
                hashes=True,
                exclude={'columns': excludes}
            )
            actual_schema = info['main']['test']
            expected = deepcopy(expect_schema)
            for name, column in expected['columns'].items():
                for exclude in excludes:
                    column.pop(exclude)
            assert expected['columns'] == actual_schema['columns']
