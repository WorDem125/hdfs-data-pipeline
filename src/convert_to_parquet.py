import os
import pandas as pd


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def save_parquet(df, filename, compression="snappy"):
    if df is None or df.empty:
        raise ValueError(f"DataFrame пустой или None, невозможно сохранить {filename}")
    if not filename.endswith(".parquet"):
        raise ValueError(f"Имя файла должно заканчиваться на .parquet, получено: {filename}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.abspath(os.path.join(OUTPUT_DIR, filename))

    df.to_parquet(output_path, index=False, compression=compression, engine="pyarrow")

    size_bytes = os.path.getsize(output_path)
    size_str = (
        f"{size_bytes / (1024 * 1024):.1f} MB"
        if size_bytes >= 1024 * 1024
        else f"{size_bytes / 1024:.1f} KB"
    )

    print(f"  Сохранено: {output_path}")
    print(f"  Размер: {size_str}, строк: {len(df)}, колонок: {len(df.columns)}")
    return output_path


def convert_all(df_accidents, df_events):
    paths = {}
    paths["road_accident_data"] = save_parquet(df_accidents, "road_accident_data.parquet")
    paths["world_important_dates"] = save_parquet(df_events, "world_important_dates.parquet")
    return paths
