import asyncio
import time
from datetime import datetime
import aiofiles
import asyncpg
import csv
from dataclasses import dataclass
from typing import List, Any
from config import BASE_DIR
from env_config import db_settings as db
from etl_logger import log_start, log_error, log_success
from validation import DataValidator


@dataclass
class DatabaseConfig:
    """Configuration for database."""

    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class ETLConfig:
    """Configuration for ETL process."""

    pause_seconds: int = 5
    enable_validation: bool = True
    stop_on_validation_error: bool = False
    files_to_load: List[str] = None

    def __post_init__(self):
        """Set default files if not provided."""
        if self.files_to_load is None:
            self.files_to_load = [
                "ft_balance_f.csv",
                "ft_posting_f.csv",
                "md_account_d.csv",
                "md_currency_d.csv",
                "md_exchange_rate_d.csv",
                "md_ledger_account_s.csv",
            ]


class CSVLoader:
    """Handles loading and processing of CSV files into the database."""

    def __init__(self) -> None:
        self.table_keys = {
            "ds.ft_balance_f": ["on_date", "account_rk"],
            "ds.md_account_d": ["data_actual_date", "account_rk"],
            "ds.md_currency_d": ["currency_rk", "data_actual_date"],
            "ds.md_exchange_rate_d": ["data_actual_date", "currency_rk"],
            "ds.md_ledger_account_s": ["ledger_account", "start_date"],
        }

    @staticmethod
    def _transform(value: str) -> Any:
        """Convert string values to appropriate Python types."""

        value = value.strip()

        if value.startswith("."):
            return float("0" + value)
        if "." in value and value.replace(".", "", 1).isdigit():
            return float(value)

        if len(value) == 3 and not value.startswith("30"):
            return value
        if len(value) == 20 and value.startswith("3"):
            return value

        if value.isdigit():
            return int(value)

        for date_format in ("%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                continue

        return value

    async def _extract(self, filename: str) -> tuple[List[str], List[List[Any]]]:
        """Read CSV file and prepare data for insertion."""

        encoding = "cp866" if filename == "md_currency_d.csv" else "utf-8"
        filepath = f"{BASE_DIR}/files/{filename}"

        async with aiofiles.open(filepath, encoding=encoding) as file:
            header_line = await file.readline()
            columns = [col.strip() for col in header_line.strip().split(";")]

            data_lines = await file.read()
            reader = csv.reader(data_lines.splitlines(), delimiter=";")
            data = [[self._transform(value) for value in row] for row in reader]

            return columns, data

    async def load_csv(
        self, conn: asyncpg.Connection, filename: str, table_name: str
    ) -> int:
        """Load CSV data into the specified database table. Returns number of records processed."""

        columns, data = await self._extract(filename)

        if not data:
            print(f"Файл {filename} пустой или не содержит данных")
            return 0

        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])
        conflict_columns = self.table_keys.get(table_name)

        if conflict_columns:
            conflict_str = ", ".join(conflict_columns)
            set_columns = [col for col in columns if col not in conflict_columns]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in set_columns])

            query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str}) DO UPDATE 
                SET {set_clause}
            """
        else:
            if table_name == "ds.ft_posting_f":
                await conn.execute(f"TRUNCATE TABLE {table_name}")

            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
            """

        await conn.executemany(query, data)
        print(f"Успешно загружено {len(data)} записей в {table_name}")
        return len(data)


class ETLProcessor:
    """Main ETL processor that orchestrates the loading of multiple files."""

    def __init__(
        self, loader: CSVLoader, db_config, etl_config: ETLConfig = None
    ) -> None:
        self.db_config = db_config
        self.loader = loader
        self.etl_config = etl_config or ETLConfig()

    async def _get_conn(self):
        """Establish database connection."""

        return await asyncpg.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password,
            database=self.db_config.database,
        )

    async def process_files(self) -> tuple:
        """Process all CSV files and return execution time."""

        start_time = time.perf_counter()
        data_conn = await self._get_conn()
        log_conn = await self._get_conn()

        try:
            await log_start(
                log_conn,
                "CSV_LOADER",
                f"Начало загрузки {len(self.etl_config.files_to_load)} файлов",
            )

            print(f"Пауза {self.etl_config.pause_seconds} секунд...")
            await asyncio.sleep(self.etl_config.pause_seconds)

            total_records = 0
            file_details = []

            async with data_conn.transaction():
                for file in self.etl_config.files_to_load:
                    table_name = f"ds.{file[:-4]}"
                    records_count = await self.loader.load_csv(
                        data_conn, file, table_name
                    )
                    total_records += records_count
                    file_details.append(f"{file}: {records_count} записей")
                    print(f"Загружено {records_count} записей в {table_name}")

                if self.etl_config.enable_validation:
                    validator = DataValidator(self.etl_config.stop_on_validation_error)
                    await validator.validate_all(data_conn)

                detail_message = f"Загружено {total_records} записей. Файлы: {', '.join(file_details)}"

                await log_success(
                    data_conn, "CSV_LOADER", total_records, detail_message
                )

        except Exception as e:
            error_msg = f"Ошибка: {str(e)}"
            print(error_msg)
            await log_error(log_conn, "CSV_LOADER", error_msg)
            raise
        finally:
            await data_conn.close()
            await log_conn.close()

        return total_records, time.perf_counter() - start_time


async def main() -> None:
    db_config = DatabaseConfig(
        host=db.db_host,
        port=db.db_port,
        user=db.db_user,
        password=db.db_password,
        database=db.db_database,
    )

    etl_config = ETLConfig(
        pause_seconds=5,
        enable_validation=True,
        stop_on_validation_error=False,
    )

    loader = CSVLoader()
    etl_processor = ETLProcessor(loader, db_config, etl_config)

    total_records, execution_time = await etl_processor.process_files()
    print(f"ETL process completed in {execution_time:.2f} seconds")
    return total_records
