from utils import make_archive
from google.cloud import storage
from configs.default import *


def connect_gcs():
    '''
    Creates a connection to the Google Cloud Storage.
    :return: Error if some exception occurred else tuple containing connection object and None.
    '''
    try:
        client = storage.Client()
    except Exception as e:
        print("Exception:", e)
        return None, e
    return client, None


def upload_to_cloud(gcs_client, db_name, table_name, start_date, end_date, prefix):
    '''
    Upload a blob to the bucket.
    :param prefix:
    :param gcs_client:
    :param start_date:
    :param end_date:
    :return: Error if some exception is raised else a tuple containing the blob name and None.
    '''
    try:
        if prefix != "":
            blob_name = "%s_%s_%s-%s" % (prefix, os.path.basename(BACKUP_PATH), start_date, end_date)
        else:
            blob_name = "%s_%s-%s" % (os.path.basename(BACKUP_PATH), start_date, end_date)
        extension = "tar"
        make_archive(BACKUP_PATH, blob_name, extension)

        bucket = gcs_client.bucket(CLOUD_STORAGE_BUCKET)
        blob = bucket.blob(os.path.join(db_name, table_name, blob_name))
        archive_to_upload = os.path.join(os.getcwd(), "%s.%s" % (blob_name, extension))
        blob.upload_from_filename(archive_to_upload)

        os.remove(archive_to_upload)
    except Exception as e:
        print("Exception:", e)
        return None, e
    return blob_name, None


def download_from_cloud(gcs_client, source_blob_name, destination_file_name):
    '''
    Downloads a blob from the bucket.
    :param gcs_client:
    :param source_blob_name:
    :param destination_file_name:
    :return: Error if some exception is raised else None.
    '''
    try:
        bucket = gcs_client.bucket(CLOUD_STORAGE_BUCKET)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
    except Exception as e:
        print("Exception:", e)
        return e


def list_blobs_with_prefix(gcs_client, prefix, delimiter=None):
    '''
    Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you just specify prefix = 'a', you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a' and delimiter='/', you'll get back:

        a/1.txt

    Additionally, the same request will return blobs.prefixes populated with:

        a/b/
    :param gcs_client:
    :param prefix:
    :param delimiter:
    :return: Error if some exception is raised else the tuple containing list of blobs and None.
    '''
    try:
        blobs = gcs_client.list_blobs(CLOUD_STORAGE_BUCKET, prefix=prefix, delimiter=delimiter)
    except Exception as e:
        print("Exception:", e)
        return None, e
    return blobs, None
