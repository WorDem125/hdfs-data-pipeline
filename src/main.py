# Точка входа pipeline.
# Запускает все три этапа последовательно: нормализация → Parquet → HDFS.

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from normalize import normalize_road_accidents, normalize_world_events
from convert_to_parquet import convert_all
from upload_to_hdfs import upload_all


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

ACCIDENTS_CSV = os.path.join(RAW_DIR, "Road_Accident_Data.csv")
EVENTS_CSV = os.path.join(RAW_DIR, "World_Important_Dates.csv")


def check_input_files():
    """Проверяет наличие исходных CSV-файлов. Завершает программу с ошибкой если файлы не найдены."""
    missing = [p for p in [ACCIDENTS_CSV, EVENTS_CSV] if not os.path.exists(p)]
    if missing:
        print("Ошибка: входные файлы не найдены:")
        for p in missing:
            print(f"  {p}")
        sys.exit(1)


def main():
    print("=" * 50)
    print("  HDFS Data Pipeline — veselov.dmitry")
    print("=" * 50)

    check_input_files()

    start = time.time()

    # --- Шаг 1: Нормализация ---
    print("\n[1/3] Нормализация данных...")
    try:
        df_accidents = normalize_road_accidents(ACCIDENTS_CSV)
        df_events = normalize_world_events(EVENTS_CSV)
        print(f"  road_accidents shape: {df_accidents.shape}")
        print(f"  world_events shape:   {df_events.shape}")
        print("  Нормализация завершена\n")
    except Exception as e:
        print(f"Ошибка при нормализации: {e}")
        sys.exit(1)

    # --- Шаг 2: Конвертация в Parquet ---
    print("[2/3] Конвертация в Parquet...")
    try:
        parquet_paths = convert_all(df_accidents, df_events)
        print("  Конвертация завершена\n")
    except Exception as e:
        print(f"Ошибка при конвертации: {e}")
        sys.exit(1)

    # --- Шаг 3: Загрузка в HDFS ---
    print("[3/3] Загрузка в HDFS...")
    try:
        upload_all(parquet_paths)
        print("  Загрузка завершена\n")
    except Exception as e:
        print(f"Ошибка при загрузке в HDFS: {e}")
        sys.exit(1)

    elapsed = time.time() - start
    print("=" * 50)
    print(f"  Pipeline выполнен за {elapsed:.1f} сек")
    print("=" * 50)


if __name__ == "__main__":
    main()
