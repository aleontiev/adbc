from .store import Store
from deepdiff import DeepHash
from cached_property import cached_property


def get_by_name(l, name, key):
    value = next((x for x in l if x['name'] == name), None)
    if value:
        return value[key]
    else:
        return None


CREATE_TABLE_MD5_FUNCTION = """CREATE OR REPLACE FUNCTION table_md5(table_name CHARACTER VARYING, VARIADIC order_key_columns CHARACTER VARYING [])
RETURNS CHARACTER VARYING as $$
DECLARE
  order_key_columns_list CHARACTER VARYING;
  query CHARACTER VARYING;
  first BOOLEAN;
  i SMALLINT;
  working_cursor REFCURSOR;
  working_row_md5 CHARACTER VARYING;
  partial_md5_so_far CHARACTER VARYING;
BEGIN
  order_key_columns_list := '';
  first := TRUE;
  FOR i IN 1..array_length(order_key_columns, 1) LOOP
    IF first THEN
      first := FALSE;
    ELSE
      order_key_columns_list := order_key_columns_list || ', ';
    END IF;
    order_key_columns_list := order_key_columns_list || order_key_columns[i];
  END LOOP;
  query := (
    'SELECT ' ||
      'md5(CAST(t.* AS TEXT)) ' ||
    'FROM (' ||
      'SELECT * FROM ' || table_name || ' ' ||
      'ORDER BY ' || order_key_columns_list ||
    ') t');
  OPEN working_cursor FOR EXECUTE (query);
  first := TRUE;
  LOOP
    FETCH working_cursor INTO working_row_md5;
    EXIT WHEN NOT FOUND;
    IF first THEN
      SELECT working_row_md5 INTO partial_md5_so_far;
    ELSE
      SELECT md5(working_row_md5 || partial_md5_so_far)
      INTO partial_md5_so_far;
    END IF;
  END LOOP;
  RETURN partial_md5_so_far :: CHARACTER VARYING;
END;
$$ LANGUAGE plpgsql"""

DROP_TABLE_MD5_FUNCTION = """
    DROP FUNCTION IF EXISTS table_md5()
"""


class Table(Store):
    def __init__(
        self,
        name,
        namespace=None,
        attributes=None,
        constraints=None,
        indexes=None
    ):
        assert(namespace)
        self.name = name
        self.namespace = namespace
        self.database = namespace.database
        self.attributes = list(sorted(attributes or [], key=lambda c: c['order']))
        self.constraints = list(sorted(constraints or [], key=lambda c: c['name']))
        self.indexes = list(sorted(indexes or [], key=lambda c: c['name']))
        self.pks = next(
            (
                get_by_name(self.indexes, c['index_name'], 'keys')
                for c in self.constraints if c['type'] == 'p'
            ),
            None
        )

    async def get_schema_hash(self):
        schema = {
            'attributes': self.attributes,
            'constraints': self.constraints,
            'indexes': self.indexes
        }
        return DeepHash(schema)[schema]

    def get_data_hash_query(self):
        return [
            "SELECT table_md5('{}.{}', {})".format(
                self.namespace.name,
                self.name,
                ', '.join([
                    "'{}'".format(x) for x in self.pks
                ])
            )
        ]

    def get_count_query(self):
        return ['SELECT COUNT(*) FROM "{}"."{}"'.format(
            self.namespace.name,
            self.name
        )]

    async def get_data_hash(self):
        pool = await self.database.pool
        async with pool.acquire() as connection:
            try:
                await connection.execute(CREATE_TABLE_MD5_FUNCTION)
                return await connection.fetchval(*self.get_data_hash_query())
            finally:
                pass # await connection.execute(DROP_TABLE_MD5_FUNCTION)

    async def get_count(self):
        pool = await self.database.pool
        async with pool.acquire() as connection:
            return await connection.fetchval(*self.get_count_query())

    @cached_property
    async def count(self):
        return await self.get_count()
