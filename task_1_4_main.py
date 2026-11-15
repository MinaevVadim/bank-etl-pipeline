import asyncio
from export_f101_to_csv import export_f101_to_csv
from create_f101_copy import create_f101_copy_table
from import_csv_to_table import import_csv_to_table
from modify_csv import modify_csv_file


async def main():
    """Главный скрипт для задачи 1.4"""

    print("Задача 1.4: Экспорт/Импорт CSV")
    print("=" * 50)

    try:
        print("\n1.Экспорт данных...")
        csv_filename = await export_f101_to_csv()

        print("\n2.Создание копии таблицы...")
        await create_f101_copy_table()

        print("\n3.Модификация CSV...")
        modified_filename = modify_csv_file(csv_filename)

        print("\n4.Импорт модифицированных данных...")
        imported_count = await import_csv_to_table(modified_filename)

        print("\n5.Проверка результатов...")
        await check_results()

        print("\nЗадача 1.4 завершена успешно!")

    except Exception as e:
        print(f"\n❌ Ошибка выполнения: {e}")
        raise


async def check_results():
    """Проверка результатов импорта"""
    import asyncpg
    from env_config import db_settings as db

    conn = await asyncpg.connect(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    try:
        count_original = await conn.fetchval("SELECT COUNT(*) FROM dm.dm_f101_round_f")
        count_copy = await conn.fetchval("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2")

        print(f"   Исходная таблица: {count_original} записей")
        print(f"   Копия таблицы: {count_copy} записей")

        differences = await conn.fetch(
            """
            SELECT 
                o.ledger_account,
                o.balance_in_rub as original_balance,
                c.balance_in_rub as copy_balance
            FROM dm.dm_f101_round_f o
            JOIN dm.dm_f101_round_f_v2 c ON o.ledger_account = c.ledger_account
            WHERE o.balance_in_rub != c.balance_in_rub
            LIMIT 5
        """
        )

        if differences:
            print(f"   Обнаружено различий: {len(differences)}")
            for diff in differences:
                print(
                    f"      Счет {diff['ledger_account']}: {diff['original_balance']} -> {diff['copy_balance']}"
                )
        else:
            print("   Различий не обнаружено (возможно ошибка в модификации)")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
