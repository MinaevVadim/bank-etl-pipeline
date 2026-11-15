import asyncpg
import csv
import os
from datetime import datetime
from env_config import db_settings as db


async def export_f101_to_csv():
    """Экспорт данных из dm.dm_f101_round_f в CSV файл"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        print("Экспорт формы 101 в CSV...")

        records = await conn.fetch(
            "SELECT * FROM dm.dm_f101_round_f ORDER BY ledger_account"
        )

        if not records:
            print("Нет данных для экспорта")
            return

        os.makedirs("export", exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"export/dm_f101_round_f_{timestamp}.csv"

        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = list(records[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for record in records:
                writer.writerow(dict(record))

        print(f"Данные экспортированы в: {csv_filename}")
        print(f"Количество записей: {len(records)}")

        await conn.execute(
            """
            INSERT INTO logs.etl_log (process_name, start_time, end_time, status, message)
            VALUES ($1, $2, $3, $4, $5)
        """,
            "export_f101_to_csv",
            datetime.now(),
            datetime.now(),
            "SUCCESS",
            f"Экспортировано {len(records)} записей в {csv_filename}",
        )

        return csv_filename

    except Exception as e:
        error_msg = f"Ошибка экспорта: {str(e)}"
        print(f"{error_msg}")
        await conn.execute(
            """
            INSERT INTO logs.etl_log (process_name, start_time, status, message)
            VALUES ($1, $2, $3, $4)
        """,
            "export_f101_to_csv",
            datetime.now(),
            "ERROR",
            error_msg,
        )
        raise

    finally:
        await conn.close()
