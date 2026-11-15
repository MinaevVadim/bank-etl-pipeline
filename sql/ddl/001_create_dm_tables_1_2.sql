-- Витрина оборотов (из структуры документа)
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f (
    on_date DATE,
    account_rk INTEGER,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);

-- Витрина остатков (восстановлена из текста задания)
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f (
    on_date DATE,
    account_rk INTEGER,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    PRIMARY KEY (on_date, account_rk)
);