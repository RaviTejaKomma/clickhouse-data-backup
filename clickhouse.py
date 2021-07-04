from clickhouse_driver import Client
from utils import *
from configs.default import *


def connect_db():
    '''
    Creates a connection to the CLickhouse database.
    :return: Error if some exception occurred else tuple containing connection object and None.
    '''
    try:
        client = Client(host='localhost')
    except Exception as e:
        print("Exception:", e)
        return None, e
    return client, None


def get_data_path(gcs_client, db_name):
    '''

    :return:
    '''
    query = "SELECT metadata_path FROM system.tables WHERE database == '%s' limit 1" % (db_name)
    try:
        result = gcs_client.execute(query)
        global_path = '/'.join(result[0][0].split('/')[:4])
    except Exception as e:
        print("Exception:", e)
        return None, e
    return global_path, None




def freeze_partitions(clickhouse_client, db_name, table_name, start_date, end_date):
    '''
    Freeze the partitions that falls between the given dates.
    :param clickhouse_client:
    :param db_name:
    :param table_name:
    :param start_date:
    :param end_date:
    :return: Error if some exception occurred else tuple containing list of partitions that got freeze and None.
    '''
    query = """select distinct partition_id from system.parts where database='%s' and table='%s' and partition_id>='%s' and partition_id<='%s'""" % \
            (db_name, table_name, start_date, end_date)
    try:
        partitions = clickhouse_client.execute(query)
        partitions = list(map(lambda x: x[0], partitions))
        for partition in partitions:
            query = "ALTER TABLE `%s`.%s FREEZE PARTITION ID '%s';" % (db_name, table_name, partition)
            clickhouse_client.execute(query)
    except Exception as e:
        print("Exception:", e)
        return None, e
    return partitions, None


def delete_partitions(clickhouse_client, db_name, table_name, partitions):
    '''
    Drops the partitions from the given table.
    :param clickhouse_client:
    :param db_name:
    :param table_name:
    :param partitions:
    :return: Error if some exception is raised else None.
    '''
    try:
        for partition in partitions:
            query = "ALTER TABLE `%s`.%s DROP PARTITION ID '%s';" % (db_name, table_name, partition)
            clickhouse_client.execute(query)
    except Exception as e:
        print("Exception:", e)
        return e


def attach_partitions(clickhouse_client, db_name, table_name, partitions):
    '''
    Attach the partitions to the specified table.
    :param clickhouse_client:
    :param db_name:
    :param table_name:
    :param partitions:
    :return: Error if some exception is raised else None
    '''
    """List of partitions will be something like ['20190424_122_122_0_113', '20190425_110_110_0_113', '20190427_111_111_0_113'] so we need to split each one by '_' and get the first element."""
    try:
        for partition in partitions:
            query = "ALTER TABLE `%s`.%s ATTACH PARTITION ID '%s';" % (db_name, table_name, partition.split('_')[0])
            clickhouse_client.execute(query)
    except Exception as e:
        print("Exception:", e)
        return e


def move_from_shadow(db_name, table_name):
    '''
    Move the freezed partitions into backup folder
    :return: Error if some exception is raised else None
    '''
    try:
        data_path = "data/%s/%s/" % (db_name.replace("-", "%2D"), table_name)
        for folder in os.listdir(SHADOW_PATH):
            if folder == "increment.txt":
                continue
            partitions_path = os.path.join(SHADOW_PATH, folder, data_path)
            for partition in os.listdir(partitions_path):
                src_path = os.path.join(partitions_path, partition)
                copy_dir(src_path, BACKUP_PATH)
    except Exception as e:
        print("Exception :", e)
        return e


def move_to_detached(db_name, table_name):
    '''
    Move the backup partitions into the detached folder
    :return: Error if some exception is raised else a tuple containing list of partitions moved to detached and None.
    '''
    try:
        detached_path = os.path.join(GLOBAL_PATH, "data", db_name.replace("-", "%2D"), table_name, "detached")
        restore_backup_path = os.path.join(RESTORE_PATH, "backup")
        partitions = os.listdir(restore_backup_path)
        for partition in partitions:
            partition_path = os.path.join(restore_backup_path, partition)
            copy_dir(partition_path, detached_path)
    except Exception as e:
        print("Exception:", e)
        return None, e
    return partitions, None
