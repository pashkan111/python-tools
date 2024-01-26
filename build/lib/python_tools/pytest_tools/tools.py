from python_tools.postgres_tools import PostgresConnection


async def create_tables(pool: PostgresConnection, db_schema_path: str):
    with open(db_schema_path, "r") as sql_file:
        sql_statements = sql_file.read()
        await pool.execute(sql_statements)


async def drop_tables(pool: PostgresConnection, db_drop_schema_path: str):
    with open(db_drop_schema_path, "r") as sql_file:
        sql_statements = sql_file.read()
        await pool.execute(sql_statements)
