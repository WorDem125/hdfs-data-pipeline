import re
import pandas as pd


def normalize_column_names(df):
    # приводим к snake_case: lower, спецсимволы → _, несколько _ → один
    new_cols = {}
    for col in df.columns:
        new_name = col.lower()
        new_name = re.sub(r"[^a-z0-9]+", "_", new_name)
        new_name = re.sub(r"_+", "_", new_name)
        new_name = new_name.strip("_")
        new_cols[col] = new_name
    return df.rename(columns=new_cols)


def clean_text_columns(df, lower=True):
    # str.strip() и str.lower() корректно работают с NaN — не превращают их в строку "nan"
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
        if lower:
            df[col] = df[col].str.lower()
        df[col] = df[col].replace("", "unknown")
    return df


def fill_missing_values(df):
    # текстовые пропуски заполняем "unknown", числовые не трогаем
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna("unknown")
    return df


def normalize_road_accidents(path):
    print(f"  Читаем: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    rows_before = len(df)
    missing_before = int(df.isnull().sum().sum())
    print(f"  Строк: {rows_before}, колонок: {len(df.columns)}, пропусков: {missing_before}")

    df = normalize_column_names(df)

    dupes = int(df.duplicated().sum())
    df = df.drop_duplicates()
    if dupes > 0:
        print(f"  Удалено дубликатов: {dupes}")

    # lower=True: все текстовые поля в нижний регистр для единообразия
    df = clean_text_columns(df, lower=True)
    df = fill_missing_values(df)

    # приводим дату к datetime
    if "accident_date" in df.columns:
        df["accident_date"] = pd.to_datetime(
            df["accident_date"], dayfirst=True, errors="coerce"
        )

    missing_after = int(df.isnull().sum().sum())
    print(f"  После нормализации: {df.shape}, пропусков осталось: {missing_after}")
    return df


def normalize_world_events(path):
    print(f"  Читаем: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")

    rows_before = len(df)
    missing_before = int(df.isnull().sum().sum())
    print(f"  Строк: {rows_before}, колонок: {len(df.columns)}, пропусков: {missing_before}")

    df = normalize_column_names(df)

    dupes = int(df.duplicated().sum())
    df = df.drop_duplicates()
    if dupes > 0:
        print(f"  Удалено дубликатов: {dupes}")

    # lower=False: оставляем читаемый регистр для исторических событий
    df = clean_text_columns(df, lower=False)
    df = fill_missing_values(df)

    # year, date и month не приводим к datetime —
    # там есть "Unknown", "2600 BC" и другие нестандартные значения

    missing_after = int(df.isnull().sum().sum())
    print(f"  После нормализации: {df.shape}, пропусков осталось: {missing_after}")
    return df
