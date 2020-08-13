import pytest
import copy

from .utils import setup_test_database

from adbc.generators import G
from adbc.workflow import WorkflowEngine
from adbc.symbols import delete, insert


@pytest.mark.asyncio
async def test_workflow():
    # 0. define constants
    user_definition = G(
        'table',
        columns={
            'id': G('column', type='integer', primary=True),
            'name': G('column', type='text', null=True),
            'email': G('column', type='text')
        },
        constraints={
            'user_id': G('constraint', type='primary', columns=['id'])
        },
        indexes={
            'user_id': G('index', primary=True, unique=True, type='btree', columns=['id'])
        }
    )
    action_definition = G(
        'table',
        columns={
            'id': G('column', type='integer', primary=True),
            'data': G('column', type='text'),
            'user_email': G('column', type='text'),
            'user_name': G('column', type='text', null=True)
        },
        constraints={
            'action_id': G('constraint', type='primary', columns=['id'])
        }
    )
    async with setup_test_database('main') as main:
        async with setup_test_database('aux') as aux:
            migrate = (
                'WITH users AS (select * from migrate.user) '
                "UPDATE action "
                "SET user_name = users.name "
                "FROM users "
                "WHERE upper(user_email) = upper(users.email)"
            )
            cleanup = "DROP SCHEMA migrate CASCADE"
            config = {
                'adbc': {'version': '0.0.1'},
                'databases': {
                    'main': main.url,
                    'aux': aux.url
                },
                'workflows': {
                    'migrate-actions': {
                        'steps': [{
                            'type': 'copy',
                            'source': 'main',
                            'target': 'aux',
                            'scope': {
                                'schemas': {
                                    'public': {
                                        'target': 'migrate',
                                        'tables': {
                                            'user': True
                                        }
                                    }
                                }
                            }
                        }, {
                            'type': 'query',
                            'source': 'aux',
                            'query': migrate
                        }, {
                            'type': 'query',
                            'source': 'aux',
                            'query': cleanup
                        }]
                    }
                },
            }

            # setup main database: users
            # setup aux database: actions

            await main.create_table('user', user_definition)
            user = await main.get_model('user')
            users = [{
                'id': 1,
                'name': 'Alf',
                'email': 'alf@test.com'
            }]
            await user.values(users).add()
            await aux.create_table('action', action_definition)
            action = await aux.get_model('action')
            actions = [{
                'id': 1,
                'data': 'logged in',
                'user_email': 'ALF@TEST.COM',
                'user_name': None
            }, {
                'id': 2,
                'data': 'logged in',
                'user_email': 'alf@test.com',
                'user_name': 'alf'
            }, {
                'id': 4,
                'data': 'logged in',
                'user_name': 'old',
                'user_email': 'bet@test.com'
            }]
            await action.values(actions).add()

            engine = WorkflowEngine(config)
            results = await engine.run('migrate-actions')
            copy_result = {
                "data_changes": {
                    "migrate": {
                        "user": {
                            "copied": 1,
                            "skipped": 0
                        }
                    }
                },
                "schema_changes": {
                    "+": {
                        "migrate": {
                            "user": user_definition
                        }
                    }
                },
                "final_diff": {}
            }
            migrate_result = 'UPDATE 2'
            cleanup_result = 'DROP SCHEMA'
            results[0].pop('duration')
            assert results == [
                copy_result,
                migrate_result,
                cleanup_result
            ]

            name = await action.key(1).field('user_name').get()
            assert name == 'Alf'
