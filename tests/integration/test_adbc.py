import pytest
from ..utils import setup_test_database



@pytest.mark.asyncio
async def test_create_and_query_table():
    table_definition = {
        'schema': {
            'columns': {
                'id': {
                    'type': 'integer',
                    'default': None,
                    'null': False
                },
                'name': {
                    'type': 'text',
                    'default': None,
                    'null': True
                }
            },
            'constraints': {
                'id_primary': {
                    'type': 'p',
                    'columns': ['id'],
                    'deferrable': False,
                    'deferred': False
                }
            }
        }
    }
    scope = {
        'schemas': {
            'testing': True
        }
    }

    async with setup_test_database('source', verbose=True) as source:
        await source.create_schema('testing')
        await source.create_table(
            'test',
            table_definition,
            schema='testing'
        )
        model = await source.get_model('testing.test')
        table = model.table

        assert table is not None
        assert table.pks == ['id']
        assert table.columns == table_definition['schema']['columns']

        # verify add (INSERT)
        jay = await model.body({'id': 1, 'name': 'Jay'}).take('*').add()
        await model.body({'id': 2, 'name': 'Quinn'}).add()
        await model.body({'id': 3}).add()

        # verify count/get (SELECT)
        query = model.where({
            '.or': [{
                'name': {'contains': 'ay'}
            }, {
                'id': 3
            }]
        })
        count = await query.count()
        assert count == 2
        results = await query.sort('id').get()
        assert len(results) == 2
        assert dict(results[0]) == {'id': 1, 'name': 'Jay'}
        assert dict(results[1]) == {'id': 3, 'name': None}

        # verify UPDATE
        updated = await model.key(3).body({'name': 'Ash'}).set()
        assert(updated == 1)

        # verify DELETE
        deleted = await model.where({'id': {'=': 2}}).take('name').delete()
        assert len(deleted) == 1
        assert deleted[0]['name'] == 'Quinn'

        info = await source.get_info(scope=scope)
        import pprint
        pprint.pprint(info)
        # assert info == None

