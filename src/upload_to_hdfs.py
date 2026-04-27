# Модуль загрузки данных в HDFS.
# Подключается к кластеру через WebHDFS, создаёт директорию и загружает Parquet-файлы.

import os
import time
import requests
import hdfs


# параметры подключения берём из переменных окружения — так можно менять без правки кода
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_DIR = os.getenv("HDFS_DIR", "/veselov.dmitry")
HDFS_USER = os.getenv("HDFS_USER", "root")


def check_hdfs_available():
    """Проверяет доступность NameNode через WebHDFS API. Возвращает True если кластер готов к работе."""
    try:
        # LISTSTATUS на корне — стандартный способ проверить что WebHDFS отвечает
        resp = requests.get(f"{HDFS_URL}/webhdfs/v1/?op=LISTSTATUS", timeout=5)
        if resp.status_code != 200:
            return False
        # дополнительно проверяем что ответ содержит корректный JSON с ожидаемой структурой
        data = resp.json()
        return "FileStatuses" in data
    except (requests.exceptions.RequestException, ValueError):
        return False


def wait_for_hdfs(max_attempts=30, delay=5):
    """Ждёт готовности HDFS, делая попытки каждые delay секунд. Бросает исключение если кластер не поднялся."""
    print(f"  Ждём HDFS по адресу {HDFS_URL}...")
    for attempt in range(1, max_attempts + 1):
        if check_hdfs_available():
            print(f"  HDFS готов (попытка {attempt})")
            return
        print(f"  Попытка {attempt}/{max_attempts}, следующая через {delay} сек...")
        time.sleep(delay)
    raise TimeoutError(f"HDFS не ответил за {max_attempts} попыток")


def upload_file(client, local_path, hdfs_dir):
    """Загружает один файл в HDFS с перезаписью. Проверяет существование локального файла перед загрузкой."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Локальный файл не найден: {local_path}")

    filename = os.path.basename(local_path)
    hdfs_path = f"{hdfs_dir}/{filename}"
    size_mb = os.path.getsize(local_path) / (1024 * 1024)

    print(f"  Загружаем {filename} ({size_mb:.1f} MB) -> {hdfs_path}")
    client.upload(hdfs_path, local_path, overwrite=True)
    print(f"  Готово: {hdfs_path}")
    return hdfs_path


def upload_all(parquet_paths):
    """Загружает все Parquet-файлы в HDFS. Если один файл не загрузился — продолжает загрузку остальных."""
    wait_for_hdfs()

    client = hdfs.InsecureClient(HDFS_URL, user=HDFS_USER)

    # создаём целевую директорию если её ещё нет
    if client.status(HDFS_DIR, strict=False) is None:
        client.makedirs(HDFS_DIR)
        print(f"  Директория {HDFS_DIR} создана")
    else:
        print(f"  Директория {HDFS_DIR} уже существует")

    # загружаем каждый файл отдельно, собираем ошибки не останавливая весь процесс
    failed = []
    for name, local_path in parquet_paths.items():
        try:
            upload_file(client, local_path, HDFS_DIR)
        except Exception as e:
            print(f"  Ошибка при загрузке {name}: {e}")
            failed.append(name)

    if failed:
        raise RuntimeError(f"Не удалось загрузить файлы: {', '.join(failed)}")

    # выводим итоговый список файлов в HDFS с размерами
    print(f"\n  Файлы в HDFS {HDFS_DIR}:")
    try:
        files = client.list(HDFS_DIR, status=True)
        for fname, status in files:
            size_kb = status["length"] / 1024
            print(f"    {fname} — {size_kb:.1f} KB")
    except hdfs.util.HdfsError as e:
        print(f"  Не удалось получить список файлов: {e}")
