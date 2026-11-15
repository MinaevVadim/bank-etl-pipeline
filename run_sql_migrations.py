import asyncpg
from env_config import db_settings as db


async def run_sql_file(conn: asyncpg.Connection, filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    await conn.execute(sql)
    print(f"Выполнен: {filepath}")


async def main():
    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        await run_sql_file(conn, "sql/ddl/001_create_dm_tables_1_2.sql")
    finally:
        await conn.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
