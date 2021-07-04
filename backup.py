from clickhouse import connect_db, freeze_partitions, move_from_shadow, delete_partitions
from gcs import connect_gcs, upload_to_cloud
from utils import clean_dir
from configs.default import *
import sys
import click


@click.command()
@click.option("--drop-partitions", is_flag=True, help="If specified, all the partitions between the given date range will be dropped from the table.")
@click.option('--prefix', default="", help="If specified, the backup file generated would be prefixed with the given value.")
@click.argument("db_name", nargs=1)
@click.argument("table_name", nargs=1)
@click.argument("start_date", nargs=1)
@click.argument("end_date", nargs=1)
def create_backup(drop_partitions, prefix, db_name, table_name, start_date, end_date):
    clickhouse_client, err = connect_db()
    if err is not None:
        sys.exit("Connection to the database failed.")
    print("Successfully connected to the database.")

    partitions, err = freeze_partitions(clickhouse_client, db_name, table_name, start_date, end_date)
    if err is not None:
        sys.exit("Exception occurred in freezing the partitions.")
    print("List of partitions that got freeze:", partitions)

    err = move_from_shadow(db_name, table_name)
    if err is not None:
        sys.exit("Exception occurred in moving the partitions from shadow folder to backup folder.")
    print("Successfully moved the partitions to backup folder.")

    if drop_partitions:
        err = delete_partitions(clickhouse_client, db_name, table_name, partitions)
        if err is not None:
            sys.exit("Exception occurred in dropping the partitions.")
        print("Successfully dropped the partitions.")

    global SHADOW_PATH
    clean_dir(SHADOW_PATH, exclude=["increment.txt"])
    print("Successfully cleared the contents of shadow folder.")

    gcs_client, err = connect_gcs()
    if err is not None:
        sys.exit("Connection to the Google Cloud Storage failed.")
    print("Successfully connected to the Google Cloud Storage.")

    blob_uploaded, err = upload_to_cloud(gcs_client, db_name, table_name, start_date, end_date, prefix)
    if err is not None:
        sys.exit("Exception occurred while uploading backup folders to GCS.")
    print("Successfully uploaded backup folders to GCS.")

    clean_dir(BACKUP_PATH, exclude=[".gitkeep"])
    print("Successfully cleared the contents od backup folder.")


if __name__ == "__main__":
    create_backup()