import csv
import random
from datetime import datetime


def modify_csv_file(csv_filename: str):
    """Модификация CSV файла - изменение нескольких значений"""

    print("Модификация CSV файла...")

    with open(csv_filename, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        records = list(reader)
        fieldnames = reader.fieldnames

    if not records:
        print("Нет данных для модификации")
        return csv_filename

    modified_filename = csv_filename.replace(".csv", "_modified.csv")

    modified_count = 0
    for i, record in enumerate(records):
        if i < 3:
            if "balance_in_rub" in record and record["balance_in_rub"]:
                old_value = float(record["balance_in_rub"])
                new_value = round(old_value * 1.1, 8)
                record["balance_in_rub"] = str(new_value)
                record["r_balance_in_rub"] = str(new_value)
                modified_count += 1
                print(f"   Запись {i+1}: balance_in_rub {old_value} -> {new_value}")

    with open(modified_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Файл модифицирован: {modified_filename}")
    print(f"Изменено записей: {modified_count}")

    return modified_filename
