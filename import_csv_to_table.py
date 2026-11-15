import asyncio
import asyncpg
import csv
from datetime import datetime, date
from env_config import db_settings as db


async def import_csv_to_table(
    csv_filename: str, target_table: str = "dm.dm_f101_round_f_v2"
):
    """Импорт данных из CSV файла в таблицу"""

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        print(f"Импорт данных из {csv_filename} в {target_table}...")

        with open(csv_filename, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            records = list(reader)

        if not records:
            print("CSV файл пустой")
            return

        columns = list(records[0].keys())
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)

        query = f"INSERT INTO {target_table} ({columns_str}) VALUES ({placeholders})"

        inserted_count = 0
        for record in records:
            values = []
            for col in columns:
                value = record[col]

                if value == "":
                    values.append(None)
                elif col in ["from_date", "to_date"]:
                    try:
                        values.append(datetime.strptime(value, "%Y-%m-%d").date())
                    except ValueError:
                        values.append(value)
                elif col == "ledger_account":
                    values.append(str(value).split(".")[0])  # Убираем .0 если есть
                elif col in ["chapter", "characteristic"]:
                    values.append(str(value) if value else "")
                elif any(field in col for field in ["balance", "turn", "r_"]):
                    try:
                        clean_value = (
                            value.rstrip("0").rstrip(".") if "." in value else value
                        )
                        values.append(float(clean_value) if clean_value else 0.0)
                    except (ValueError, TypeError):
                        values.append(0.0)
                else:
                    values.append(value)

            await conn.execute(query, *values)
            inserted_count += 1

        print(f"Импортировано записей: {inserted_count}")

        await conn.execute(
            """
            INSERT INTO logs.etl_log (process_name, start_time, end_time, status, message)
            VALUES ($1, $2, $3, $4, $5)
        """,
            "import_csv_to_table",
            datetime.now(),
            datetime.now(),
            "SUCCESS",
            f"Импортировано {inserted_count} записей в {target_table} из {csv_filename}",
        )

        return inserted_count

    except Exception as e:
        error_msg = f"Ошибка импорта: {str(e)}"
        print(f"{error_msg}")
        await conn.execute(
            """
            INSERT INTO logs.etl_log (process_name, start_time, status, message)
            VALUES ($1, $2, $3, $4)
        """,
            "import_csv_to_table",
            datetime.now(),
            "ERROR",
            error_msg,
        )
        raise

    finally:
        await conn.close()
