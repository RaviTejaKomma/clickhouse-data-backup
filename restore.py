from clickhouse import move_to_detached, attach_partitions, connect_db
from utils import retrieve_archive, clean_dir
from gcs import list_blobs_with_prefix, download_from_cloud, connect_gcs
import sys
import click
from configs.default import *


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("db_name", nargs=1)
@click.argument("table_name", nargs=1)
@click.pass_context
def list_blobs(ctx, db_name, table_name):
    prefix = "%s/%s" % (db_name, table_name)
    blobs, err = list_blobs_with_prefix(gcs_client, prefix)
    if err is not None:
        sys.exit("Exception occurred while retrieving the list of blobs.")
    print("Successfully retrieved the list of blobs.")
    for blob in blobs:
        print(blob.name)


@cli.command()
@click.argument("db_name", nargs=1)
@click.argument("table_name", nargs=1)
@click.argument("blob_name", nargs=1)
@click.pass_context
def restore_backup(ctx, db_name, table_name, blob_name):
    destination_file_name = os.path.join(RESTORE_PATH, "%s.%s" % (blob_name, "tar"))
    src_blob_name = "%s/%s/%s" % (db_name, table_name, blob_name)
    err = download_from_cloud(gcs_client, src_blob_name, destination_file_name)
    if err is not None:
        sys.exit("Exception occurred while downloading backup folders from GCS.")
    print(
        "Blob {} downloaded to {}.".format(
            src_blob_name, destination_file_name
        )
    )

    clickhouse_client, err = connect_db()
    if err is not None:
        sys.exit("Connection to the database failed.")
    print("Successfully connected to the database.")

    extract_dir = RESTORE_PATH
    err = retrieve_archive(destination_file_name, extract_dir, "tar")
    if err is not None:
        sys.exit("Exception occurred while retrieving the archive.")
    print("Successfully retrieved the backup to restore folder.")

    """Removing the archive file"""
    os.remove(destination_file_name)
    print("Successfully removed the archive file.")

    partitions, err = move_to_detached(db_name, table_name)
    if err is not None:
        sys.exit("Exception occurred in moving the partitions from restore folder to detached folder.")
    print("Successfully moved the partitions", partitions, "to detached folder.")

    clean_dir(RESTORE_PATH, exclude=[".gitkeep"])
    print("Successfully cleared the contents of restore folder.")

    err = attach_partitions(clickhouse_client, db_name, table_name, partitions)
    if err is not None:
        sys.exit("Exception occurred in attaching the partitions.")
    print("Successfully attached the partitions.")


if __name__ == "__main__":
    gcs_client, err = connect_gcs()
    if err is not None:
        sys.exit("Connection to the Google Cloud Storage failed.")
    print("Successfully connected to the Google Cloud Storage.")
    cli()
