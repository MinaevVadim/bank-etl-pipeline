import asyncio
import os
import sys

import asyncpg
from datetime import date, timedelta

sys.path.append(os.getcwd())

from env_config import db_settings as db


async def run_january_2018():
    """Запуск процедур расчета за январь 2018"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        print("Запуск процедур расчета за январь 2018...")

        print("1. Заполняем начальные остатки за 31.12.2017...")
        await conn.execute(
            """
            DELETE FROM dm.dm_account_balance_f WHERE on_date = '2017-12-31';
            INSERT INTO dm.dm_account_balance_f 
            SELECT 
                b.on_date, b.account_rk, b.balance_out,
                b.balance_out * COALESCE(er.reduced_cource, 1) as balance_out_rub
            FROM ds.ft_balance_f b
            LEFT JOIN ds.md_account_d acc ON b.account_rk = acc.account_rk
            LEFT JOIN ds.md_exchange_rate_d er ON acc.currency_rk = er.currency_rk 
                AND er.data_actual_date = '2017-12-31'
            WHERE b.on_date = '2017-12-31'
        """
        )

        start_date = date(2018, 1, 1)
        end_date = date(2018, 1, 31)

        current_date = start_date
        while current_date <= end_date:
            print(f"Расчет за {current_date}...")

            await conn.execute("CALL ds.fill_account_turnover_f($1)", current_date)
            await conn.execute("CALL ds.fill_account_balance_f($1)", current_date)

            current_date += timedelta(days=1)

        print("Расчет января 2018 завершен!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_january_2018())
