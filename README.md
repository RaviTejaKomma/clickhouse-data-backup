# Clickhouse Data Backup

clickhouse-data-backup is the collection of scripts which backup and restore ClickHouse data with GCS support.

## Features

- Easy creating and restoring backups of all or specific tables

## Usage

```
NAME:
    backup.py - Tool to backup ClickHouse data with GCS cloud support.
    restore.py - Tool to restore ClickHouse backup data from GCS.

USAGE:
    python backup.py <database> <table> <start_date> <end_date>
    python restore.py list-blobs <database> <table>
    python restore.py restore-backup <database> <table> <blob_name>

VERSION:
   1.0

DESCRIPTION:
   Run the scripts as 'clickhouse' user

COMMANDS:
    list-blobs          Print list of backups for a specific database and table. 
    --help              Shows a list of commands or help for one command.

GLOBAL OPTIONS:
   --help, -h              show help
```