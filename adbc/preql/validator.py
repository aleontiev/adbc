class Validator:
    schema = {
        # data
        'select': True,
        'insert': True,
        'update': True,
        'delete': True,
        'truncate': True,
        'explain': True,
        # meta-data
        'describe': {},
        'show': {
            "type": ["string", "object"],
            "pattern": "^[A-Za-z_]+$",
            "properties": {
                "tables": {},
                "variables": {},
                "columns": {},
                "databases": {},
                "indexes": {},
                "schemas": {}
            },
            "maxProperties": 1,
            "additionalProperties": False
        },
        'create': {
            "type": "object",
            "maxProperties": 1,
            "properties": {
                "ifNotExists": {},
                "view": {
                },
                "table": {
                },
                "column": {
                },
                "index": {
                },
                "constraint": {
                },
                "sequence": {}
            },
            "additionalProperties": False
        },
        "alter": {
            "type": "object",
            "properties": {
                "ifExists": {},
                "table": {},
                "column": {},
                "constraint": {},
                "sequence": {}
            },
            "required": ["table"]
        },
        "drop": {
            "type": "object",
            "maxProperties": 1,
            "properties": {
                "view": {
                },
                "table": {
                },
                "column": {
                },
                "index": {
                },
                "constraint": {
                },
                "sequence": {}
            },
        },
    }
