# GD Workspace backup restore
Tool which restores one, or more, workspace analytical model (AM), logical data models (LDM) and user data filters (UDF) from source backup archives in an incremental manner.

The backups contain declarative definitions of AM, LDM and UDFs which are unarchived, loaded into memory and finally put into the target GD workspace.

The restores are workspace-agnostic, which means that if you need to, you can import a backed-up of one workspace into a different workspace.

## Usage
The tool requires the following arguments on input:
- `ws_csv` - a path to a csv file defining target workspace IDs to restore to, and a backup source paths
- `conf` - a path to a configuration file containing information required for accessing the backup source storage

Use the tool like so:

```sh
python scripts/restore.py ws_csv conf
```

Where ws_csv refers to input csv and conf to configuration file in yaml format.

For example, if you have csv file named "example_input.csv" in the folder from which you are executing the python command and configuration file named "example_conf.yaml" in subfolder relative to the folder you are executing the script from named "subfolder", the execution could look like this:

```sh
python scripts/restore.py example_input.csv subfolder/example_conf.yaml
```


To show the help for using arguments, call:
```sh
python scripts/restore.py -h
```

## Configuration file (conf)
The configuration files lets you define which type of storage the restore tool will source the backups from, and any additional storage-specific information that might be required. Currently only AWS S3 is supported.

The configuration file has the following format:
```yaml
storage_type: some_storage
storage:
  arg1: foo
  arg2: bar
```

### AWS S3

You can define the configuration file for S3 storage like so: 

```yaml
storage_type: s3
storage:
  bucket: some_bucket
  backup_path: some/path/to/backups/gd_org_id/
  profile: services 
```
Here, the meaning of different `storage` fields is as follows:
- bucket - S3 storage bucket containing the backups
- backup_path - absolute path within the S3 bucket which leads to the root directory of the backups (the input csv file defines sources from here)
- profile (optional) - AWS profile to be used


## Input CSV file (ws_csv)
The input CSV file defines the the targets and sources for backup restores (imports).

Following format of the csv is expected:

| workspace_id | path             |
|--------------|------------------|
| ws_id_1      | path/to/backup_1 |
| ws_id_2      | path/to/backup_2 |
| ws_id_3      | path/to/backup_1 |

Here, each `workspace_id` is the workspace ID of the workspace to perform the restore to. The `path` is the path (e.g. in S3) to a directory which contains the target backup archive (`gooddata_layouts.zip`).

The `path` is then prefixed with a additional information (e.g. S3 bucket and backup_path to backups root dir).

If something fails, please read over all ERROR log messages for information where the issue lies.