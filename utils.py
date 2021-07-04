import os
import shutil
from subprocess import call


def copy_dir(src_path, dest_path):
    try:
        print("Copying", src_path, "to", dest_path)
        call(['cp', '-rp', src_path, dest_path])
    except Exception as e:
        print("Exception:", e)
        return e


def clean_dir(dir_path, exclude=[]):
    print("Cleaning the contents of", dir_path)
    for folder in os.listdir(dir_path):
        if folder in exclude:
            continue
        folder_path = os.path.join(dir_path, folder)
        if os.path.isdir(folder_path):
            shutil.rmtree(folder_path)
        else:
            os.remove(folder_path)


def retrieve_archive(filename, extract_dir, archive_format):
    try:
        shutil.unpack_archive(filename, extract_dir, archive_format)
    except Exception as e:
        print("Exception:", e)
        return e


def make_archive(src, dest, extension):
    archive_from = os.path.dirname(src)
    archive_to = os.path.basename(src.strip(os.sep))
    shutil.make_archive(dest, extension, archive_from, archive_to)
