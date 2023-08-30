from postgres_tools import Pg


pg = Pg(dsn=dsn)


async def get_data():
    await pg.get().fetch('SELECT * FROM table')