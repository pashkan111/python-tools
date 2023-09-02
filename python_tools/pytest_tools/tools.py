from python_tools.postgres_tools import PostgresConnection


async def create_tables(pool: PostgresConnection):
    with open('database/schema.sql', 'r') as sql_file:
        sql_statements = sql_file.read()
        await pool.execute(sql_statements)


async def drop_tables(pool: PostgresConnection):
    with open('database/drop_schema.sql', 'r') as sql_file:
        sql_statements = sql_file.read()
        await pool.execute(sql_statements)
