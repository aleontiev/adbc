import pytest
from .utils import setup_test_database
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
    table_definition = {
        "columns": {
            "id": G('column', type='integer', primary='id_primary', sequence=True),
            "name": G('column', type='text', null=True)
        },
        "constraints": {
            "id_primary": G('constraint',
                type='primary',
                columns=['id']
            )
        }
    }
    scope = {"schemas": {"testing": True}}
    alias_scope = {"schemas": {"main": {"source": "testing"}}}
    # 1. setup test database
    async with setup_test_database("source", verbose=True) as source:
        # 2. create test schematic elements: schema and table
        await source.create_schema("testing")
        await source.create_table("test", table_definition, schema="testing")

        # 3. get/add/edit/delete data
        model = await source.get_model("test", schema='testing')
        table = model.table

        assert table is not None
        assert table.pks == {"id": "id_primary"}
        table_definition['columns']['id']['sequence'] = 'testing.test__id__seq'
        table_definition['columns']['id']['default'] = {
            'nextval': "'testing.test__id__seq'"
        }
        assert table.columns == table_definition["columns"]

        # add (INSERT)
        jay = await model.values({"name": "Jay"}).take("*").add()
        await model.values({"name": "Quinn"}).add()
        await model.add()

        # count/get (SELECT)
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
        assert dict(results[0]) == {"id": 1, "name": "Jay"}
        assert dict(results[1]) == {"id": 3, "name": None}

        # UPDATE
        # TODO: use where(id=3) to test where in set
        updated = await model.key(3).values({"name": "Ash"}).set()
        assert updated == 1

        # DELETE
        deleted = await model.key(2).take("name").delete()
        assert len(deleted) == 1
        assert deleted[0]["name"] == "Quinn"

        # 5. get database statistics
        info = await source.get_info(scope=scope)
        expect_schema = table_definition
        actual_schema = info["testing"]["test"]
        seq = info['testing']['test__id__seq']
        assert seq == {'value': 3, 'type': 'sequence'}
        actual_data = actual_schema['rows']
        assert expect_schema["columns"] == actual_schema["columns"]
        assert expect_schema["constraints"] == actual_schema["constraints"]
        assert actual_data["count"] == 2
        assert actual_data["range"] == {"id": {"min": 1, "max": 3}}

        # 6. add new schema elements
        table_definition["columns"]["created"] = timestamp_column
        table_definition["constraints"]["unique_name"] = unique_name
        await source.create_column(
            "test", "created", timestamp_column, schema="testing"
        )
        await source.create_constraint(
            "test", "unique_name", unique_name, schema="testing"
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
        # expect unique to be set 
        expect_schema['columns']['name']['unique'] = 'unique_name'
        assert expect_schema["columns"] == actual_schema["columns"]
        assert expect_schema["constraints"] == actual_schema["constraints"]
        assert actual_data["count"] == 4
        assert actual_data["range"] == {"id": {"min": 1, "max": 6}}
        assert actual_data['hashes'] == {1: '566991d4b9cf37367cab89ab93b74a3d'}

        # 9. test exclusion: ignore certain fields
        excludes = ['unique', 'primary', 'related']
        info = await source.get_info(
            scope=alias_scope,
            hashes=True,
            exclude={'columns': excludes}
        )
        actual_schema = info['main']['test']
        for name, column in expect_schema['columns'].items():
            for exclude in excludes:
                column.pop(exclude)
        assert expect_schema['columns'] == actual_schema['columns']
