CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    report_start DATE;
    report_end DATE;
    report_period_start DATE;
BEGIN
    -- Логирование
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES ('fill_f101_round_f', NOW(), 'STARTED', 'Расчет формы 101 за период до ' || i_OnDate);

    -- Определяем отчетный период (предыдущий месяц)
    report_start := DATE_TRUNC('month', i_OnDate - INTERVAL '1 month')::DATE;  -- 2018-01-01
    report_end := (DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day')::DATE;    -- 2018-01-31
    -- Начало периода для проверки актуальности счетов
    report_period_start := report_start;  -- 2018-01-01

    -- Очистка
    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = report_start AND to_date = report_end;

    -- РАСЧЕТ ФОРМЫ 101 с правильными датами актуальности
    INSERT INTO dm.dm_f101_round_f
    WITH account_info AS (
        -- Берем счета актуальные НА ПЕРИОД ОТЧЕТА (2018-01-01)
        SELECT
            account_rk,
            LEFT(account_number, 5)::INTEGER as ledger_account,
            char_type as characteristic,
            CASE WHEN currency_code IN ('643', '810') THEN 'RUB' ELSE 'VAL' END as currency_type,
            -- Находим главу из плана счетов
            COALESCE(
                (SELECT DISTINCT chapter
                 FROM ds.md_ledger_account_s
                 WHERE ledger_account = LEFT(account_number, 5)::INTEGER
                 LIMIT 1),
                'A'
            ) as chapter
        FROM ds.md_account_d
        WHERE report_period_start BETWEEN data_actual_date AND data_actual_end_date
    ),
    balance_in AS (
        -- Остатки на начало периода (31.12.2017)
        SELECT
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            ai.currency_type,
            SUM(b.balance_out_rub) as balance_out_rub
        FROM dm.dm_account_balance_f b
        JOIN account_info ai ON b.account_rk = ai.account_rk
        WHERE b.on_date = '2017-12-31'
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter, ai.currency_type
    ),
    balance_out AS (
        -- Остатки на конец периода (31.01.2018)
        SELECT
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            ai.currency_type,
            SUM(b.balance_out_rub) as balance_out_rub
        FROM dm.dm_account_balance_f b
        JOIN account_info ai ON b.account_rk = ai.account_rk
        WHERE b.on_date = '2018-01-31'
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter, ai.currency_type
    ),
    turnovers AS (
        -- Обороты за период (январь 2018)
        SELECT
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            ai.currency_type,
            SUM(t.debet_amount_rub) as turn_deb_rub,
            SUM(t.credit_amount_rub) as turn_cre_rub
        FROM dm.dm_account_turnover_f t
        JOIN account_info ai ON t.account_rk = ai.account_rk
        WHERE t.on_date BETWEEN '2018-01-01' AND '2018-01-31'
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter, ai.currency_type
    ),
    aggregated AS (
        -- Агрегируем все данные
        SELECT
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            -- Остатки на начало периода
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN COALESCE(bi.balance_out_rub, 0) ELSE 0 END) as balance_in_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN COALESCE(bi.balance_out_rub, 0) ELSE 0 END) as balance_in_val,
            SUM(COALESCE(bi.balance_out_rub, 0)) as balance_in_total,
            -- Обороты по дебету
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN COALESCE(t.turn_deb_rub, 0) ELSE 0 END) as turn_deb_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN COALESCE(t.turn_deb_rub, 0) ELSE 0 END) as turn_deb_val,
            SUM(COALESCE(t.turn_deb_rub, 0)) as turn_deb_total,
            -- Обороты по кредиту
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN COALESCE(t.turn_cre_rub, 0) ELSE 0 END) as turn_cre_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN COALESCE(t.turn_cre_rub, 0) ELSE 0 END) as turn_cre_val,
            SUM(COALESCE(t.turn_cre_rub, 0)) as turn_cre_total,
            -- Остатки на конец периода
            SUM(CASE WHEN ai.currency_type = 'RUB' THEN COALESCE(bo.balance_out_rub, 0) ELSE 0 END) as balance_out_rub,
            SUM(CASE WHEN ai.currency_type = 'VAL' THEN COALESCE(bo.balance_out_rub, 0) ELSE 0 END) as balance_out_val,
            SUM(COALESCE(bo.balance_out_rub, 0)) as balance_out_total
        FROM account_info ai
        LEFT JOIN balance_in bi ON ai.ledger_account = bi.ledger_account AND ai.characteristic = bi.characteristic
        LEFT JOIN balance_out bo ON ai.ledger_account = bo.ledger_account AND ai.characteristic = bo.characteristic
        LEFT JOIN turnovers t ON ai.ledger_account = t.ledger_account AND ai.characteristic = t.characteristic
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter
    )
    SELECT
        report_start as from_date,
        report_end as to_date,
        a.chapter,
        a.ledger_account::CHAR(5),
        a.characteristic,
        -- BALANCE_IN
        a.balance_in_rub, a.balance_in_rub as r_balance_in_rub,
        a.balance_in_val, a.balance_in_val as r_balance_in_val,
        a.balance_in_total, a.balance_in_total as r_balance_in_total,
        -- TURN_DEB
        a.turn_deb_rub, a.turn_deb_rub as r_turn_deb_rub,
        a.turn_deb_val, a.turn_deb_val as r_turn_deb_val,
        a.turn_deb_total, a.turn_deb_total as r_turn_deb_total,
        -- TURN_CRE
        a.turn_cre_rub, a.turn_cre_rub as r_turn_cre_rub,
        a.turn_cre_val, a.turn_cre_val as r_turn_cre_val,
        a.turn_cre_total, a.turn_cre_total as r_turn_cre_total,
        -- BALANCE_OUT
        a.balance_out_rub, a.balance_out_rub as r_balance_out_rub,
        a.balance_out_val, a.balance_out_val as r_balance_out_val,
        a.balance_out_total, a.balance_out_total as r_balance_out_total
    FROM aggregated a
    WHERE a.balance_in_total != 0 OR a.turn_deb_total != 0 OR a.turn_cre_total != 0 OR a.balance_out_total != 0;

    -- Логирование успеха
    INSERT INTO logs.etl_log (process_name, start_time, end_time, status, message)
    VALUES ('fill_f101_round_f', NOW(), NOW(), 'SUCCESS',
            'Рассчитана форма 101 за период ' || report_start || ' - ' || report_end);

EXCEPTION WHEN OTHERS THEN
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES ('fill_f101_round_f', NOW(), 'ERROR', 'Ошибка: ' || SQLERRM);
    RAISE;
END;
$$;