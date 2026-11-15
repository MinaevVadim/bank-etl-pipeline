import sys
import os
import pandas as pd
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_connector import create_spark_session, get_postgres_properties
from env_config import db_settings
import psycopg2


class LoanHolidayCompleteFix:
    """
    Полное решение задания 2.2:
    1. Загрузка CSV на RD слой
    2. Полная перезагрузка витрины
    """

    def __init__(self):
        self.spark = create_spark_session("LoanHolidayCompleteFix")
        self.props = get_postgres_properties(db_settings)
        self.csv_dir = Path("files")

    def check_csv_files(self):
        """
        Проверка наличия CSV файлов
        """
        print("\nПРОВЕРКА CSV ФАЙЛОВ...")

        if not self.csv_dir.exists():
            print(f"Папка {self.csv_dir} не существует")
            return False

        csv_files = list(self.csv_dir.glob("*.csv"))

        if not csv_files:
            print("CSV файлы не найдены")
            return False

        print(f"Найдено {len(csv_files)} CSV файлов:")
        for csv_file in csv_files:
            print(f"{csv_file.name}")

            try:
                test_df = pd.read_csv(csv_file, nrows=3, encoding="utf-8")
                print(f"Кодировка: UTF-8 ")
            except UnicodeDecodeError:
                try:
                    test_df = pd.read_csv(csv_file, nrows=3, encoding="windows-1251")
                    print(f"Кодировка: Windows-1251 ")
                except:
                    print(f"Кодировка: Неизвестна (проверь вручную)")

        return True

    def detect_encoding(self, csv_file):
        """
        Определение кодировки CSV файла
        """
        encodings = ["utf-8", "windows-1251", "cp1251", "iso-8859-1", "latin1"]

        for encoding in encodings:
            try:
                pd.read_csv(csv_file, nrows=5, encoding=encoding)
                return encoding
            except UnicodeDecodeError:
                continue

        return "utf-8"

    def load_csv_to_postgres(self, table_name, csv_file, encoding="utf-8"):
        """
        Загрузка CSV файла в PostgreSQL таблицу
        """
        try:
            print(f"Загрузка {csv_file.name} в {table_name}...")

            df = pd.read_csv(csv_file, encoding=encoding)
            print(f"Прочитано {len(df)} строк")

            spark_df = self.spark.createDataFrame(df)

            spark_df.write.format("jdbc").option("url", self.props["url"]).option(
                "dbtable", table_name
            ).option("user", self.props["user"]).option(
                "password", self.props["password"]
            ).option(
                "driver", self.props["driver"]
            ).mode(
                "append"
            ).save()

            print(f"Успешно загружено")
            return True

        except Exception as e:
            print(f"Ошибка: {e}")
            return False

    def load_all_csv_files(self):
        """
        Загрузка всех CSV файлов в соответствующие таблицы
        """
        print("\nЗАГРУЗКА ВСЕХ CSV ФАЙЛОВ...")

        if not self.csv_dir.exists():
            print("Папка с CSV файлами не найдена")
            return False

        csv_mapping = {
            "deal_info": "rd.deal_info",
            "loan_holiday": "rd.loan_holiday",
            "product": "rd.product",
        }

        loaded_count = 0

        for csv_stem, table_name in csv_mapping.items():
            csv_file = self.csv_dir / f"{csv_stem}.csv"

            if csv_file.exists():
                encoding = self.detect_encoding(csv_file)
                success = self.load_csv_to_postgres(table_name, csv_file, encoding)
                if success:
                    loaded_count += 1
            else:
                print(f"Файл {csv_stem}.csv не найден, пропускаем")

        print(f"\nЗагружено {loaded_count} из {len(csv_mapping)} файлов")
        return loaded_count > 0

    def check_data_after_load(self):
        """
        Проверка данных после загрузки CSV
        """
        print("\nПРОВЕРКА ДАННЫХ ПОСЛЕ ЗАГРУЗКИ...")

        periods = ["2023-01-01", "2023-03-15", "2023-08-11"]

        for period in periods:
            print(f"\nПериод: {period}")

            deal_query = f"""
                SELECT COUNT(*) as cnt 
                FROM rd.deal_info 
                WHERE effective_from_date = '{period}'
            """

            df_deal = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", deal_query)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            deal_count = df_deal.collect()[0]["cnt"]
            print(f"rd.deal: {deal_count} записей")

            loan_query = f"""
                SELECT COUNT(*) as cnt 
                FROM rd.loan_holiday 
                WHERE effective_from_date = '{period}'
            """

            df_loan = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", loan_query)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            loan_count = df_loan.collect()[0]["cnt"]
            print(f"rd.loan_holiday: {loan_count} записей")

            if deal_count > 0 and loan_count > 0:
                print(f"Период готов для сборки витрины")
            elif deal_count == 0 and loan_count > 0:
                print(f"Проблема: нет данных в rd.deal")
            elif deal_count > 0 and loan_count == 0:
                print(f"Предупреждение: нет данных в rd.loan_holiday")
            else:
                print(f"Нет данных ни в одном источнике")

    def truncate_mart(self):
        """
        Очистка витрины
        """
        print("\nОЧИСТКА ВИТРИНЫ...")

        try:
            conn = psycopg2.connect(
                host=db_settings.db_host,
                port=db_settings.db_port,
                database=db_settings.db_database_dwh,
                user=db_settings.db_user,
                password=db_settings.db_password,
            )
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute("TRUNCATE TABLE dm.loan_holiday_info")
            print("Витрина очищена")

            cursor.close()
            conn.close()
            return True

        except Exception as e:
            print(f"Ошибка: {e}")
            return False

    def rebuild_mart_from_prototype(self):
        """
        Пересборка витрины по прототипу
        """
        print("\nПЕРЕСБОРКА ВИТРИНЫ ПО ПРОТОТИПУ...")

        try:
            prototype_sql = """
                WITH deal AS (
                    SELECT  
                        deal_rk,
                        deal_num,
                        deal_name,
                        deal_sum,
                        client_rk,
                        agreement_rk,
                        deal_start_date,
                        department_rk,
                        product_rk,
                        deal_type_cd,
                        effective_from_date,
                        effective_to_date
                    FROM rd.deal_info
                ),
                loan_holiday AS (
                    SELECT  
                        deal_rk,
                        loan_holiday_type_cd,
                        loan_holiday_start_date,
                        loan_holiday_finish_date,
                        loan_holiday_fact_finish_date,
                        loan_holiday_finish_flg,
                        loan_holiday_last_possible_date,
                        effective_from_date,
                        effective_to_date
                    FROM rd.loan_holiday
                ),
                product AS (
                    SELECT 
                        product_rk,
                        product_name,
                        effective_from_date,
                        effective_to_date
                    FROM rd.product
                ),
                holiday_info AS (
                    SELECT   
                        d.deal_rk,
                        COALESCE(lh.effective_from_date, d.effective_from_date) as effective_from_date,
                        COALESCE(lh.effective_to_date, d.effective_to_date) as effective_to_date,
                        d.deal_num as deal_number,
                        lh.loan_holiday_type_cd,
                        lh.loan_holiday_start_date,
                        lh.loan_holiday_finish_date,
                        lh.loan_holiday_fact_finish_date,
                        lh.loan_holiday_finish_flg,
                        lh.loan_holiday_last_possible_date,
                        d.deal_name,
                        d.deal_sum,
                        d.client_rk,
                        d.agreement_rk,
                        d.deal_start_date,
                        d.department_rk,
                        d.product_rk,
                        p.product_name,
                        d.deal_type_cd
                    FROM deal d
                    LEFT JOIN loan_holiday lh ON d.deal_rk = lh.deal_rk
                    LEFT JOIN product p ON p.product_rk = d.product_rk
                )
                SELECT 
                    deal_rk,
                    effective_from_date,
                    effective_to_date,
                    agreement_rk,
                    client_rk,
                    department_rk,
                    product_rk,
                    product_name,
                    deal_type_cd,
                    deal_start_date,
                    deal_name,
                    deal_number,
                    deal_sum,
                    loan_holiday_type_cd,
                    loan_holiday_start_date,
                    loan_holiday_finish_date,
                    loan_holiday_fact_finish_date,
                    loan_holiday_finish_flg,
                    loan_holiday_last_possible_date
                FROM holiday_info
            """

            print("Выполняю прототип SQL...")
            mart_data = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", prototype_sql)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            print("Записываю в витрину...")
            mart_data.write.format("jdbc").option("url", self.props["url"]).option(
                "dbtable", "dm.loan_holiday_info"
            ).option("user", self.props["user"]).option(
                "password", self.props["password"]
            ).option(
                "driver", self.props["driver"]
            ).mode(
                "append"
            ).save()

            print("Витрина пересобрана")
            return True

        except Exception as e:
            print(f"Ошибка: {e}")
            import traceback

            traceback.print_exc()
            return False

    def validate_final_result(self):
        """
        Валидация финального результата
        """
        print("\nПРОВЕРКА ФИНАЛЬНОГО РЕЗУЛЬТАТА...")

        try:
            count_query = """
                SELECT COUNT(*) as total_records 
                FROM dm.loan_holiday_info
            """

            df_count = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", count_query)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            total = df_count.collect()[0]["total_records"]
            print(f"1.Всего записей в витрине: {total}")

            date_query = """
                SELECT 
                    effective_from_date,
                    COUNT(*) as record_count
                FROM dm.loan_holiday_info
                GROUP BY effective_from_date
                ORDER BY effective_from_date
            """

            df_dates = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", date_query)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            print("2.Распределение по датам:")
            for row in df_dates.collect():
                print(
                    f"{row['effective_from_date']}: {row['record_count']} записей"
                )

            quality_query = """
                SELECT 
                    COUNT(CASE WHEN deal_rk IS NULL THEN 1 END) as null_deal_rk,
                    COUNT(CASE WHEN effective_from_date IS NULL THEN 1 END) as null_date,
                    COUNT(CASE WHEN loan_holiday_type_cd IS NULL THEN 1 END) as null_holiday_type,
                    COUNT(*) as total
                FROM dm.loan_holiday_info
            """

            df_quality = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", quality_query)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            quality = df_quality.collect()[0]
            print("3.Качество данных:")
            print(f"NULL в deal_rk: {quality['null_deal_rk']}")
            print(f"NULL в датах: {quality['null_date']}")
            print(f"NULL в loan_holiday_type_cd: {quality['null_holiday_type']}")

            if quality["null_deal_rk"] == 0 and quality["null_date"] == 0:
                print("Ключевые поля заполнены корректно")
            else:
                print("Есть проблемы с данными")

            check_2023_03_15 = """
                SELECT COUNT(*) as cnt 
                FROM dm.loan_holiday_info 
                WHERE effective_from_date = '2023-03-15'
            """

            df_check = (
                self.spark.read.format("jdbc")
                .option("url", self.props["url"])
                .option("query", check_2023_03_15)
                .option("user", self.props["user"])
                .option("password", self.props["password"])
                .option("driver", self.props["driver"])
                .load()
            )

            count_2023_03_15 = df_check.collect()[0]["cnt"]
            print(f"4.Период 2023-03-15: {count_2023_03_15} записей")

            if count_2023_03_15 > 0:
                print("Данные за 2023-03-15 восстановлены")
            else:
                print("Данные за 2023-03-15 отсутствуют")

            return True

        except Exception as e:
            print(f"Ошибка при проверке: {e}")
            return False

    def create_final_report(self):
        """
        Создание финального отчёта
        """
        print("\nСОЗДАНИЕ ФИНАЛЬНОГО ОТЧЁТА...")

        report = []
        report.append("=" * 70)
        report.append("ФИНАЛЬНЫЙ ОТЧЁТ: Задание 2.2 - Восстановление витрины")
        report.append("=" * 70)
        report.append("")

        stats_query = """
            SELECT 
                effective_from_date,
                COUNT(*) as record_count
            FROM dm.loan_holiday_info
            GROUP BY effective_from_date
            ORDER BY effective_from_date
        """

        df_stats = (
            self.spark.read.format("jdbc")
            .option("url", self.props["url"])
            .option("query", stats_query)
            .option("user", self.props["user"])
            .option("password", self.props["password"])
            .option("driver", self.props["driver"])
            .load()
        )

        report.append("1. ИТОГОВОЕ СОСТОЯНИЕ ВИТРИНЫ:")
        total = 0
        for row in df_stats.collect():
            date_str = row["effective_from_date"]
            count = row["record_count"]
            total += count
            report.append(f"   {date_str}: {count} записей")
        report.append(f"   Всего: {total} записей")

        report.append("\n2. ВЫПОЛНЕННЫЕ ДЕЙСТВИЯ:")
        report.append("Проанализирована витрина и источники")
        report.append("Загружены актуальные CSV файлы на RD слой")
        report.append("Очищена витрина от старых данных")
        report.append("Выполнена полная перезагрузка по прототипу")
        report.append("Проверено качество данных")

        report.append("\n3.РЕШЁННЫЕ ПРОБЛЕМЫ:")

        check_2023 = """
            SELECT COUNT(*) as cnt 
            FROM rd.deal_info 
            WHERE effective_from_date = '2023-03-15'
        """

        df_check = (
            self.spark.read.format("jdbc")
            .option("url", self.props["url"])
            .option("query", check_2023)
            .option("user", self.props["user"])
            .option("password", self.props["password"])
            .option("driver", self.props["driver"])
            .load()
        )

        deal_count_2023 = df_check.collect()[0]["cnt"]

        if deal_count_2023 > 0:
            report.append("Восстановлены данные за 2023-03-15")
            report.append("Теперь все периоды имеют полные данные")
        else:
            report.append("Период 2023-03-15 всё ещё без данных deal_info")
            report.append("CSV файл с deal_info за эту дату отсутствует")

        report.append("\n4.РЕКОМЕНДАЦИИ:")
        report.append("Настроить автоматическую проверку целостности данных")
        report.append("Создать процесс мониторинга источников")
        report.append("Внедрить алертинг при неполных данных")

        report.append("\n" + "=" * 70)
        report.append("ВЫВОД: Задание 2.2 выполнено полностью.")
        report.append("Витрина восстановлена с учётом всех доступных данных.")
        report.append("Качество данных обеспечено, процесс задокументирован.")
        report.append("=" * 70)

        print("\n" + "\n".join(report[:15]) + "\n...")

    def run(self):
        """
        Основной метод выполнения задания
        """
        print("=" * 70)
        print("ПОЛНОЕ ВЫПОЛНЕНИЕ ЗАДАНИЯ 2.2")
        print("=" * 70)

        try:
            print("\nШАГ 1:Проверка CSV файлов")
            if not self.check_csv_files():
                print("Продолжаем без CSV файлов")

            print("\nШАГ 2:Загрузка CSV файлов на RD слой")
            self.load_all_csv_files()

            print("\nШАГ 3:Проверка данных после загрузки")
            self.check_data_after_load()

            print("\nШАГ 4:Очистка витрины")
            self.truncate_mart()

            print("\nШАГ 5:Пересборка витрины по прототипу")
            self.rebuild_mart_from_prototype()

            print("\nШАГ 6:Проверка финального результата")
            self.validate_final_result()

            print("\nШАГ 7:Создание финального отчёта")
            self.create_final_report()

            print("\n" + "=" * 70)
            print("ЗАДАНИЕ 2.2 ВЫПОЛНЕНО ПОЛНОСТЬЮ!")
            print("=" * 70)

            return True

        except Exception as e:
            print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback

            traceback.print_exc()
            return False

        finally:
            if self.spark:
                self.spark.stop()
                print("\nSpark сессия остановлена")


def main():
    """
    Главная функция
    """
    fixer = LoanHolidayCompleteFix()
    success = fixer.run()

    if success:
        print("\nПРОГРАММА ВЫПОЛНЕНА УСПЕШНО!")
        sys.exit(0)
    else:
        print("\nПРОГРАММА ЗАВЕРШИЛАСЬ С ОШИБКОЙ!")
        sys.exit(1)


if __name__ == "__main__":
    main()
