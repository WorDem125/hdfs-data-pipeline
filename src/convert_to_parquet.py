import os
import pandas as pd


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def save_parquet(df, filename, compression="snappy"):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.abspath(os.path.join(OUTPUT_DIR, filename))
    df.to_parquet(output_path, index=False, compression=compression, engine="pyarrow")
    size_kb = os.path.getsize(output_path) / 1024
    print(f"Сохранено: {output_path} ({size_kb:.1f} KB)")
    return output_path


def convert_all(df_accidents, df_events):
    paths = {}
    paths["road_accident_data"] = save_parquet(df_accidents, "road_accident_data.parquet")
    paths["world_important_dates"] = save_parquet(df_events, "world_important_dates.parquet")
    return paths
