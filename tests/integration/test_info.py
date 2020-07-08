import pytest
from .utils import setup_test_database
from adbc.generators import G


@pytest.mark.asyncio
async def test_info():
    # 0. define constants
    timestamp_column = G('column', type='timestamp with time zone')
    unique_name = G(
        'constraint',
        type='unique',
        columns=['name']
    )
    table_definition = {
        "schema": {
            "columns": {
                "id": G('column', type='integer', null=False),
                "name": G('column', type='text')
            },
            "constraints": {
                "id_primary": G('constraint',
                    type='primary',
                    columns=['id']
                )
            },
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
        model = await source.get_model("testing.test")
        table = model.table

        assert table is not None
        assert table.pks == ["id"]
        assert table.columns == table_definition["schema"]["columns"]

        # add (INSERT)
        jay = await model.body({"id": 1, "name": "Jay"}).take("*").add()
        await model.body({"id": 2, "name": "Quinn"}).add()
        await model.body({"id": 3}).add()

        # count/get (SELECT)
        query = model.where({".or": [{"name": {"contains": "ay"}}, {"id": 3}]})
        count = await query.count()
        assert count == 2
        results = await query.sort("id").get()
        assert len(results) == 2
        assert dict(results[0]) == {"id": 1, "name": "Jay"}
        assert dict(results[1]) == {"id": 3, "name": None}

        # UPDATE
        updated = await model.key(3).body({"name": "Ash"}).set()
        assert updated == 1

        # DELETE
        deleted = await model.where({"id": {"=": 2}}).take("name").delete()
        assert len(deleted) == 1
        assert deleted[0]["name"] == "Quinn"

        # 5. get database statistics
        info = await source.get_info(scope=scope)
        expect_schema = table_definition["schema"]
        actual_schema = info["testing"]["test"]["schema"]
        test_data = info["testing"]["test"]["data"]
        assert expect_schema["columns"] == actual_schema["columns"]
        assert expect_schema["constraints"] == actual_schema["constraints"]
        assert test_data["count"] == 2
        assert test_data["range"] == {"id": {"min": 1, "max": 3}}

        # 6. add new schema elements
        table_definition["schema"]["columns"]["created"] = timestamp_column
        table_definition["schema"]["constraints"]["unique_name"] = unique_name
        await source.create_column(
            "test", "created", timestamp_column, schema="testing"
        )
        await source.create_constraint(
            "test", "unique_name", unique_name, schema="testing"
        )

        # 7. add new data
        await model.body([{"id": 6, "name": "Jim"}, {"id": 5, "name": "Jane"}]).add()

        # 8. get database statistics again with an aliased scope
        # alias scope supports translation during diff/copy
        info = await source.get_info(scope=alias_scope)
        actual_schema = info["main"]["test"]["schema"]
        test_data = info["main"]["test"]["data"]
        assert expect_schema["columns"] == actual_schema["columns"]
        assert expect_schema["constraints"] == actual_schema["constraints"]
        assert test_data["count"] == 4
        assert test_data["range"] == {"id": {"min": 1, "max": 6}}
