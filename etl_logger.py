from datetime import datetime

import asyncpg


async def log_start(
    conn: asyncpg.Connection, process_name: str, message: str = ""
) -> None:
    await conn.execute(
        "INSERT INTO logs.etl_log (process_name, start_time, status, message) VALUES ($1, $2, $3, $4)",
        process_name,
        datetime.now(),
        "STARTED",
        message,
    )


async def log_success(
    conn: asyncpg.Connection,
    process_name: str,
    records_processed: int = 0,
    message: str = "",
) -> None:
    start_log = await conn.fetchrow(
        "SELECT start_time FROM logs.etl_log WHERE process_name = $1 AND status = 'STARTED' ORDER BY log_id DESC LIMIT 1",
        process_name,
    )

    start_time = start_log["start_time"] if start_log else datetime.now()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    await conn.execute(
        """INSERT INTO logs.etl_log 
        (process_name, start_time, end_time, status, records_processed, message, duration_seconds) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)""",
        process_name,
        start_time,
        end_time,
        "SUCCESS",
        records_processed,
        message,
        round(duration, 2),
    )


async def log_error(
    conn: asyncpg.Connection, process_name: str, error_message: str
) -> None:
    await conn.execute(
        "INSERT INTO logs.etl_log (process_name, start_time, status, message) VALUES ($1, $2, $3, $4)",
        process_name,
        datetime.now(),
        "ERROR",
        f"Ошибка: {error_message}",
    )
