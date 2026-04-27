import os
import time
import requests
import hdfs


HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_DIR = os.getenv("HDFS_DIR", "/veselov.dmitry")
HDFS_USER = os.getenv("HDFS_USER", "root")

WAIT_TIMEOUT = 120
WAIT_INTERVAL = 5


def check_hdfs_available(url):
    try:
        resp = requests.get(f"{url}/webhdfs/v1/?op=GETFILESTATUS", timeout=5)
        # любой HTTP-ответ означает что namenode поднялся
        return resp.status_code in (200, 403, 404)
    except requests.exceptions.RequestException:
        return False


def wait_for_hdfs():
    print(f"Ждём HDFS по адресу {HDFS_URL}...")
    elapsed = 0
    while elapsed < WAIT_TIMEOUT:
        if check_hdfs_available(HDFS_URL):
            print(f"HDFS готов (ждали {elapsed} сек)")
            return
        time.sleep(WAIT_INTERVAL)
        elapsed += WAIT_INTERVAL
        print(f"Ещё ждём... ({elapsed} сек)")
    raise TimeoutError(f"HDFS не поднялся за {WAIT_TIMEOUT} секунд")


def upload_all(parquet_paths):
    wait_for_hdfs()

    client = hdfs.InsecureClient(HDFS_URL, user=HDFS_USER)

    # создаём директорию если не существует
    try:
        client.makedirs(HDFS_DIR)
        print(f"Директория {HDFS_DIR} создана")
    except hdfs.util.HdfsError as e:
        print(f"Директория уже существует или ошибка: {e}")

    # загружаем каждый файл
    for name, local_path in parquet_paths.items():
        filename = os.path.basename(local_path)
        hdfs_path = f"{HDFS_DIR}/{filename}"
        print(f"Загружаем {filename} -> {hdfs_path}")
        client.upload(hdfs_path, local_path, overwrite=True)
        print(f"Загружено: {hdfs_path}")

    # выводим список файлов в HDFS
    print(f"\nФайлы в HDFS {HDFS_DIR}:")
    try:
        files = client.list(HDFS_DIR, status=True)
        for fname, status in files:
            size_kb = status["length"] / 1024
            print(f"  {fname} — {size_kb:.1f} KB")
    except hdfs.util.HdfsError as e:
        print(f"Ошибка при листинге: {e}")
