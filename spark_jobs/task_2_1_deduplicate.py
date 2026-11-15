from pyspark.sql import Window
from pyspark.sql.functions import (
    row_number,
    col,
    count,
    date_format,
    concat,
    lit,
    from_utc_timestamp,
    expr,
)
from spark_connector import (
    create_spark_session,
    read_table_from_postgres,
    write_table_to_postgres,
)
from env_config import db_settings
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def deduplicate_client_table():
    """
    Основная функция для выполнения задания 2.1
    """
    print("=" * 70)
    print("ЗАДАНИЕ 2.1: Устранение дублей в витрине dm.client")
    print("=" * 70)

    spark = None
    try:
        spark = create_spark_session("DeduplicateClient_v2.1")

        print("\nЧитаю данные из dm.client...")
        client_df = read_table_from_postgres(spark, "client", "dm", db_settings)

        print("Кэширую данные для безопасной обработки...")
        client_df_cached = client_df.cache()
        total_count = client_df_cached.count()
        print(f"Всего записей: {total_count}")

        print("\nСхема таблицы:")
        client_df_cached.printSchema()

        print("\nПервые 5 записей:")
        client_df_cached.show(5, truncate=False)

        print("\nАнализирую дубликаты...")

        unique_count = (
            client_df_cached.select("client_rk", "effective_from_date")
            .distinct()
            .count()
        )
        print(
            f"Уникальных комбинаций (client_rk + effective_from_date): {unique_count}"
        )

        duplicates_count = total_count - unique_count
        print(f"Дубликатов для удаления: {duplicates_count}")

        if duplicates_count == 0:
            print("\nДубликатов не найдено!")
            client_df_cached.unpersist()
            return True

        print("\nВыполняю дедупликацию...")

        window_spec = Window.partitionBy("client_rk", "effective_from_date").orderBy(
            col("effective_to_date").desc_nulls_last()
        )

        df_with_rownum = client_df_cached.withColumn(
            "row_num", row_number().over(window_spec)
        )
        deduplicated_df = df_with_rownum.filter(col("row_num") == 1).drop("row_num")

        final_count = deduplicated_df.count()
        print(f"\nРезультат дедупликации:")
        print(f"   Было записей: {total_count}")
        print(f"   Стало записей: {final_count}")
        print(f"   Удалено дубликатов: {duplicates_count}")

        print("\nСохраняю результат в PostgreSQL...")

        write_table_to_postgres(
            deduplicated_df,
            "client",
            "dm",
            mode="overwrite",
            db_config=db_settings,
        )

        client_df_cached.unpersist()

        print("\nДедупликация завершена успешно!")

        check_df = read_table_from_postgres(spark, "client", "dm", db_settings)
        print(f"Проверка: в таблице осталось {check_df.count()} записей")

        return True

    except Exception as e:
        print(f"\nОшибка при выполнении задания 2.1: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        if spark:
            spark.stop()
            print("\nSpark сессия остановлена")


if __name__ == "__main__":
    success = deduplicate_client_table()

    if success:
        print("\n" + "=" * 70)
        print("ЗАДАНИЕ 2.1 ВЫПОЛНЕНО УСПЕШНО!")
        print("=" * 70)
        exit(0)
    else:
        print("\n" + "=" * 70)
        print("ЗАДАНИЕ 2.1 ЗАВЕРШИЛОСЬ С ОШИБКОЙ!")
        print("=" * 70)
        exit(1)
