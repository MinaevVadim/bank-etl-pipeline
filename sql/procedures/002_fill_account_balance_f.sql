-- Процедура расчета витрины остатков
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES ('fill_account_balance_f', NOW(), 'STARTED', 'Расчет остатков за ' || i_OnDate);

    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    INSERT INTO dm.dm_account_balance_f
    WITH active_accounts AS (
        SELECT account_rk, char_type
        FROM ds.md_account_d
        WHERE i_OnDate BETWEEN data_actual_date AND data_actual_end_date
    ),
    previous_balance AS (
        SELECT account_rk, balance_out, balance_out_rub
        FROM dm.dm_account_balance_f
        WHERE on_date = i_OnDate - INTERVAL '1 day'
    ),
    current_turns AS (
        SELECT account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
        FROM dm.dm_account_turnover_f
        WHERE on_date = i_OnDate
    )
    SELECT
        i_OnDate as on_date,
        a.account_rk,
        CASE
            WHEN a.char_type = 'А' THEN
                COALESCE(pb.balance_out, 0) + COALESCE(ct.debet_amount, 0) - COALESCE(ct.credit_amount, 0)
            WHEN a.char_type = 'П' THEN
                COALESCE(pb.balance_out, 0) - COALESCE(ct.debet_amount, 0) + COALESCE(ct.credit_amount, 0)
            ELSE 0
        END as balance_out,
        CASE
            WHEN a.char_type = 'А' THEN
                COALESCE(pb.balance_out_rub, 0) + COALESCE(ct.debet_amount_rub, 0) - COALESCE(ct.credit_amount_rub, 0)
            WHEN a.char_type = 'П' THEN
                COALESCE(pb.balance_out_rub, 0) - COALESCE(ct.debet_amount_rub, 0) + COALESCE(ct.credit_amount_rub, 0)
            ELSE 0
        END as balance_out_rub
    FROM active_accounts a
    LEFT JOIN previous_balance pb ON a.account_rk = pb.account_rk
    LEFT JOIN current_turns ct ON a.account_rk = ct.account_rk;

    INSERT INTO logs.etl_log (process_name, start_time, end_time, status, message)
    VALUES ('fill_account_balance_f', NOW(), NOW(), 'SUCCESS', 'Рассчитаны остатки за ' || i_OnDate);

EXCEPTION WHEN OTHERS THEN
    INSERT INTO logs.etl_log (process_name, start_time, status, message)
    VALUES ('fill_account_balance_f', NOW(), 'ERROR', 'Ошибка: ' || SQLERRM);
    RAISE;
END;
$$;