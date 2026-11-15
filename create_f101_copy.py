import asyncpg
from env_config import db_settings as db


async def create_f101_copy_table():
    """Создание копии таблицы для импорта"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        print("Создание копии таблицы...")

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (
                from_date DATE,
                to_date DATE,
                chapter CHAR(1),
                ledger_account CHAR(5),
                characteristic CHAR(1),
                balance_in_rub NUMERIC(23,8),
                r_balance_in_rub NUMERIC(23,8),
                balance_in_val NUMERIC(23,8),
                r_balance_in_val NUMERIC(23,8),
                balance_in_total NUMERIC(23,8),
                r_balance_in_total NUMERIC(23,8),
                turn_deb_rub NUMERIC(23,8),
                r_turn_deb_rub NUMERIC(23,8),
                turn_deb_val NUMERIC(23,8),
                r_turn_deb_val NUMERIC(23,8),
                turn_deb_total NUMERIC(23,8),
                r_turn_deb_total NUMERIC(23,8),
                turn_cre_rub NUMERIC(23,8),
                r_turn_cre_rub NUMERIC(23,8),
                turn_cre_val NUMERIC(23,8),
                r_turn_cre_val NUMERIC(23,8),
                turn_cre_total NUMERIC(23,8),
                r_turn_cre_total NUMERIC(23,8),
                balance_out_rub NUMERIC(23,8),
                r_balance_out_rub NUMERIC(23,8),
                balance_out_val NUMERIC(23,8),
                r_balance_out_val NUMERIC(23,8),
                balance_out_total NUMERIC(23,8),
                r_balance_out_total NUMERIC(23,8),
                PRIMARY KEY (from_date, ledger_account)
            )
        """
        )

        await conn.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")

        print("Создана таблица: dm.dm_f101_round_f_v2")

    finally:
        await conn.close()
