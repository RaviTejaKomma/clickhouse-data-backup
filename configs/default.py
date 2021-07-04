import os

# GCS configs #
CREDENTIALS_JSON = ""
CLOUD_STORAGE_BUCKET = "clickhouse-data-backup"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_JSON

# ClickHouse configs #
GLOBAL_PATH = "/var/lib/clickhouse/"
SHADOW_PATH = os.path.join(GLOBAL_PATH, "shadow")


BACKUP_PATH = os.path.join(os.getcwd(), "backup")
RESTORE_PATH = os.path.join(os.getcwd(), "restore")
ERROR_LOG_PATH = "logs/error_log.log"