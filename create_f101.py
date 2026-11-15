import asyncio
import asyncpg
from env_config import db_settings as db


async def create_f101():
    """Создание таблицы и процедуры для формы 101"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        with open("sql/ddl/003_create_dm_f101_round_f.sql", "r") as f:
            await conn.execute(f.read())
        print("Создана таблица dm.dm_f101_round_f")

        with open("sql/procedures/003_fill_f101_round_f.sql", "r") as f:
            await conn.execute(f.read())
        print("Создана процедура dm.fill_f101_round_f")

    finally:
        await conn.close()


async def run_f101():
    """Запуск расчета формы 101 за январь 2018"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        print("Расчет формы 101 за январь 2018...")

        await conn.execute("CALL dm.fill_f101_round_f('2018-02-01')")

        count = await conn.fetchval("SELECT COUNT(*) FROM dm.dm_f101_round_f")
        print(f"Форма 101 рассчитана! Записей: {count}")

        sample = await conn.fetchrow("SELECT * FROM dm.dm_f101_round_f LIMIT 1")
        if sample:
            print("Пример записи:")
            for key, value in dict(sample).items():
                print(f"   {key}: {value}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(create_f101())
    asyncio.run(run_f101())
