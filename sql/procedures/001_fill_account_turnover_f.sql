-- Процедура расчета витрины оборотов
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Логирование начала (вставляем в logs.etl_log)
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES ('fill_account_turnover_f', NOW(), 'STARTED', 'Расчет оборотов за ' || i_OnDate);

    -- Удаляем записи за дату расчета
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    -- Расчет оборотов
    INSERT INTO dm.dm_account_turnover_f
    WITH credit_turns AS (
        SELECT
            credit_account_rk as account_rk,
            SUM(credit_amount) as credit_amount
        FROM ds.ft_posting_f
        WHERE oper_date = i_OnDate
        GROUP BY credit_account_rk
    ),
    debet_turns AS (
        SELECT
            debet_account_rk as account_rk,
            SUM(debet_amount) as debet_amount
        FROM ds.ft_posting_f
        WHERE oper_date = i_OnDate
        GROUP BY debet_account_rk
    ),
    all_accounts AS (
        SELECT DISTINCT account_rk
        FROM (
            SELECT credit_account_rk as account_rk FROM ds.ft_posting_f WHERE oper_date = i_OnDate
            UNION
            SELECT debet_account_rk as account_rk FROM ds.ft_posting_f WHERE oper_date = i_OnDate
        ) accounts
    )
    SELECT
        i_OnDate as on_date,
        a.account_rk,
        COALESCE(c.credit_amount, 0) as credit_amount,
        COALESCE(c.credit_amount * COALESCE(er.reduced_cource, 1), 0) as credit_amount_rub,
        COALESCE(d.debet_amount, 0) as debet_amount,
        COALESCE(d.debet_amount * COALESCE(er.reduced_cource, 1), 0) as debet_amount_rub
    FROM all_accounts a
    LEFT JOIN credit_turns c ON a.account_rk = c.account_rk
    LEFT JOIN debet_turns d ON a.account_rk = d.account_rk
    LEFT JOIN ds.md_account_d acc ON a.account_rk = acc.account_rk
    LEFT JOIN ds.md_exchange_rate_d er ON acc.currency_rk = er.currency_rk
        AND er.data_actual_date = i_OnDate;

    -- Логирование успеха
    INSERT INTO logs.etl_log (process_name, start_time, end_time, status, message)
    VALUES ('fill_account_turnover_f', NOW(), NOW(), 'SUCCESS', 'Рассчитано оборотов за ' || i_OnDate);

EXCEPTION WHEN OTHERS THEN
    -- Логирование ошибки
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES ('fill_account_turnover_f', NOW(), 'ERROR', 'Ошибка: ' || SQLERRM);
    RAISE;
END;
$$;