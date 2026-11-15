import findspark

findspark.init()

from pyspark.sql import SparkSession
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def create_spark_session(app_name="BankDataProcessing"):
    """
    Создает и возвращает Spark сессию
    """
    try:
        spark = (
            SparkSession.builder.appName("Task_2_1_Deduplicate_Client")
            .master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .getOrCreate()
        )

        print(f"Spark сессия создана: {app_name}")
        return spark

    except Exception as e:
        print(f"Ошибка создания Spark сессии: {e}")
        raise


def get_postgres_properties(db_config):
    """
    Создает свойства для подключения к PostgreSQL из вашего config
    """
    return {
        "url": f"jdbc:postgresql://{db_config.db_host}:{db_config.db_port}/{db_config.db_database_dwh}",
        "user": db_config.db_user,
        "password": db_config.db_password,
        "driver": "org.postgresql.Driver",
    }


def read_table_from_postgres(spark, table_name, schema="dm", db_config=None):
    """
    Читает таблицу из PostgreSQL в Spark DataFrame
    """
    if db_config is None:
        from env_config import db_settings

        db_config = db_settings

    props = get_postgres_properties(db_config)

    df = (
        spark.read.format("jdbc")
        .option("url", props["url"])
        .option("dbtable", f"{schema}.{table_name}")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .load()
    )

    print(f"Прочитана таблица {schema}.{table_name}: {df.count()} записей")
    return df


def write_table_to_postgres(
    df, table_name, schema="dm", mode="overwrite", db_config=None
):
    """
    Записывает Spark DataFrame в PostgreSQL
    """
    try:
        if db_config is None:
            from env_config import db_settings

            db_config = db_settings

        props = get_postgres_properties(db_config)

        df.write.format("jdbc").option("url", props["url"]).option(
            "dbtable", f"{schema}.{table_name}"
        ).option("user", props["user"]).option("password", props["password"]).option(
            "driver", props["driver"]
        ).mode(
            mode
        ).save()

        print(f"Таблица {schema}.{table_name} записана успешно (mode: {mode})")
        return True

    except Exception as e:
        print(f"Ошибка при записи в {schema}.{table_name}: {e}")
        import traceback

        traceback.print_exc()

        return False
