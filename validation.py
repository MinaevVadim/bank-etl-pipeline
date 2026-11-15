import asyncpg


class DataValidator:

    def __init__(self, stop_on_error: bool = True) -> None:
        self.stop_on_error = stop_on_error

    async def validate_all(self, conn) -> int:

        result_referential_integrity = await self.validate_referential_integrity(conn)

        result_business_rules = await self.validate_business_rules(conn)

        result_data_quality = await self.validate_data_quality(conn)

        sum_result = (
            result_referential_integrity + result_business_rules + result_data_quality
        )

        return sum_result

    async def validate_referential_integrity(
        self, conn: asyncpg.Connection
    ) -> None | int:
        """Check referential integrity between tables."""
        print("Проверка ссылочной целостности...")

        invalid_credit_accounts = await conn.fetch(
            """
                SELECT DISTINCT credit_account_rk as invalid_account
                FROM ds.ft_posting_f 
                WHERE credit_account_rk NOT IN (SELECT account_rk FROM ds.md_account_d)
            """
        )

        invalid_debet_accounts = await conn.fetch(
            """
                SELECT DISTINCT debet_account_rk as invalid_account  
                FROM ds.ft_posting_f 
                WHERE debet_account_rk NOT IN (SELECT account_rk FROM ds.md_account_d)
            """
        )

        invalid_currencies = await conn.fetch(
            """
                SELECT DISTINCT currency_rk 
                FROM ds.md_account_d 
                WHERE currency_rk NOT IN (SELECT currency_rk FROM ds.md_currency_d)
            """
        )

        total_errors = (
            len(invalid_credit_accounts)
            + len(invalid_debet_accounts)
            + len(invalid_currencies)
        )

        error_msg = f"Найдено ошибок ссылочной целостности: {total_errors}"
        if invalid_credit_accounts:
            error_msg += f"\n- Невалидные credit_account_rk: {[r['invalid_account'] for r in invalid_credit_accounts[:10]]}"
        if invalid_debet_accounts:
            error_msg += f"\n- Невалидные debet_account_rk: {[r['invalid_account'] for r in invalid_debet_accounts[:10]]}"
        if invalid_currencies:
            error_msg += f"\n- Невалидные currency_rk: {[r['currency_rk'] for r in invalid_currencies]}"

        if total_errors > 0 and self.stop_on_error:
            raise ValueError(error_msg)
        elif total_errors > 0:
            print(
                f"Предупреждение: найдено {total_errors} ошибок ссылочной целостности"
            )
        else:
            print("Референсная целостность: OK")

        return total_errors

    async def validate_business_rules(self, conn: asyncpg.Connection) -> None | int:
        """Check business rules."""
        print("Проверка бизнес-правил...")

        unbalanced_postings = await conn.fetch(
            """
                SELECT oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount
                FROM ds.ft_posting_f 
                WHERE ROUND(credit_amount::numeric, 2) != ROUND(debet_amount::numeric, 2)
            """
        )

        invalid_char_types = await conn.fetch(
            """
                SELECT account_rk, char_type 
                FROM ds.md_account_d 
                WHERE char_type NOT IN ('А', 'П')
            """
        )

        total_errors = len(unbalanced_postings) + len(invalid_char_types)

        error_msg = f"Нарушения бизнес-правил: {total_errors}"
        if unbalanced_postings:
            error_msg += f"\nНесбалансированные проводки: {len(unbalanced_postings)}"
        if invalid_char_types:
            error_msg += f"\n- Невалидные char_type: {[r['char_type'] for r in invalid_char_types]}"

        if total_errors > 0 and self.stop_on_error:
            raise ValueError(error_msg)
        elif total_errors > 0:
            print(f"Предупреждение: {error_msg}")
        else:
            print("Бизнес-правила: OK")

        return total_errors

    async def validate_data_quality(self, conn: asyncpg.Connection) -> None | int:
        """Check data quality rules."""
        print("Проверка качества данных...")

        null_issues = await conn.fetch(
            """
                -- ft_posting_f - обязательные поля не должны быть NULL
                SELECT 'ft_posting_f' as table_name, 'NULL в обязательном поле' as issue_type,
                       COUNT(*) as issue_count
                FROM ds.ft_posting_f 
                WHERE oper_date IS NULL OR credit_account_rk IS NULL OR debet_account_rk IS NULL

                UNION ALL

                -- md_account_d - обязательные поля
                SELECT 'md_account_d' as table_name, 'NULL в обязательном поле' as issue_type,
                       COUNT(*) as issue_count  
                FROM ds.md_account_d
                WHERE data_actual_date IS NULL OR account_rk IS NULL OR account_number IS NULL

                UNION ALL

                -- ft_balance_f - обязательные поля
                SELECT 'ft_balance_f' as table_name, 'NULL в обязательном поле' as issue_type,
                       COUNT(*) as issue_count
                FROM ds.ft_balance_f
                WHERE on_date IS NULL OR account_rk IS NULL
            """
        )

        negative_amounts = await conn.fetch(
            """
                SELECT 'negative_amounts' as issue_type, COUNT(*) as issue_count
                FROM ds.ft_posting_f 
                WHERE credit_amount < 0 OR debet_amount < 0

                UNION ALL

                SELECT 'negative_balance' as issue_type, COUNT(*) as issue_count
                FROM ds.ft_balance_f
                WHERE balance_out < 0
            """
        )

        future_dates = await conn.fetch(
            """
                SELECT 'future_oper_date' as issue_type, COUNT(*) as issue_count
                FROM ds.ft_posting_f 
                WHERE oper_date > CURRENT_DATE

                UNION ALL

                SELECT 'future_on_date' as issue_type, COUNT(*) as issue_count
                FROM ds.ft_balance_f
                WHERE on_date > CURRENT_DATE
            """
        )

        duplicates = await conn.fetch(
            """
                -- В ft_balance_f не должно быть дублей по ключу
                SELECT 'duplicate_balance' as issue_type, COUNT(*) as issue_count
                FROM (
                    SELECT on_date, account_rk, COUNT(*) 
                    FROM ds.ft_balance_f 
                    GROUP BY on_date, account_rk 
                    HAVING COUNT(*) > 1
                ) dupes
            """
        )

        all_issues = null_issues + negative_amounts + future_dates + duplicates

        total_issues = sum(issue["issue_count"] for issue in all_issues)

        actual_issues = [issue for issue in all_issues if issue["issue_count"] > 0]

        if actual_issues:
            error_msg = "Проблемы качества данных:\n"
            for issue in actual_issues:
                error_msg += f"- {issue['table_name'] if 'table_name' in issue else 'Общее'}: {issue['issue_type']} - {issue['issue_count']} записей\n"
            if self.stop_on_error:
                raise ValueError(error_msg)
            else:
                print(f"Предупреждение: {error_msg}")
        else:
            print("Качество данных: OK")

        return total_issues
