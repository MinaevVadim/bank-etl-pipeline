"""
Точка входа для выполнения задания 2.1 с использованием Spark
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    print("Запуск задания 2.1 с использованием PySpark...")

    try:
        from task_2_1_deduplicate import deduplicate_client_table

        success = deduplicate_client_table()

        if success:
            print("\nПрограмма завершена успешно!")
            sys.exit(0)
        else:
            print("\nПрограмма завершилась с ошибкой!")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠Программа прервана пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\nНеожиданная ошибка: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
