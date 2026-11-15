import sys

import asyncpg
import os

sys.path.append(os.getcwd())

from env_config import db_settings as db


async def run_sql_file(conn: asyncpg.Connection, filepath: str):
    """Выполняет SQL файл"""
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    await conn.execute(sql)
    print(f"✅ Создана процедура из: {filepath}")


async def create_procedures():
    """Создание хранимых процедур в БД"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        # Создаем процедуры
        procedures_dir = "sql/procedures"
        for file in sorted(os.listdir(procedures_dir)):
            if file.endswith(".sql"):
                await run_sql_file(conn, os.path.join(procedures_dir, file))

        print("✅ Все процедуры созданы!")

        # Проверяем создание
        procedures = await conn.fetch(
            """
            SELECT routine_name 
            FROM information_schema.routines 
            WHERE routine_schema = 'ds'
            AND routine_type = 'PROCEDURE'
        """
        )

        print("📋 Созданные процедуры:")
        for proc in procedures:
            print(f"   - {proc['routine_name']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(create_procedures())
