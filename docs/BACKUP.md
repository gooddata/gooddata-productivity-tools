# GD Export workspace definition

Tool which exports / creates a backup of one or more workspaces - their logical data model (LDM), analytical model (AM),  user data filters (UDF), filter views and automations. Backups are stored either locally or can be uploaded to S3 bucket.

## Usage
The tool requires the following arguments on input:
- `ws_csv` - a path to a csv file defining target workspace IDs to restore to, and a backup source paths
- `conf` - a path to a configuration file containing information required for accessing the backup source storage
- `--input-type`, `-t` - specification of how the input file from the first argument is handled. This argument is optional.

### Input type

The `input-type` argument is optional and has three options:

`list-of-workspaces` is the default option. The data in the input file is treated as an exhaustive list of workspaces to back up. This is also what will happen if the argument is omitted entirely

Use `list-of-parents` when the input file contains a list of parent workspaces and you wish to back up the entire hierarchy. For each workspace ID in the input, all of its direct and indirect children are included in the backup, as well as the parent workspaces themselves.

If the `entire-organization` option is selected, the script will back up all the workspaces within the organization. If this option is selected, you do not need to provide the `ws_csv` argument as it will be ignored. If a `ws_csv` value is provided, the script will log a warning message, but will proceed to back up the organization.

### Usage examples

If you want to back up a list of specific workspaces, run:

```sh
python scripts/backup.py ws_csv conf
```

Where ws_csv refers to input csv and conf to configuration file in yaml format. This would be equivalent to running:

```sh
python scripts/backup.py ws_csv conf -t list-of-workspaces
```

For example, if you have csv file named "example_input.csv" in the folder from which you are executing the python command and configuration file named "example_conf.yaml" in subfolder relative to the folder you are executing the script from named "subfolder", the execution could look like this:

```sh
python scripts/backup.py example_input.csv subfolder/example_conf.yaml
```

If you want to back up a specific hierarchy under a parent workspace, prepare the list of parents, store it in a csv file (named for example `parents.csv`) and run:

```sh
python scripts/backup.py parents.csv conf.yaml -t list-of-parents
```

If you want to back up all the workspaces in the organization, run:

```sh
python scripts/backup.py conf.yaml -t entire-organization
```

Note that in this case, you do not need to provide the `ws_csv` argument as no list is required.

To show the help for using arguments, call:
```sh
python scripts/backup.py -h
```

There are two more optional arguments for setting up GoodData profiles.
By default, a tool attempts to locate a GoodData profile file at ~/.gooddata/profiles.yaml, but you can also choose to provide a custom path like so:
- `-p` - path/to/profiles.yaml
- `--profile` - name of GoodData profile to be used

```sh
python scripts/backup.py input.csv conf.yaml -p path/to/profiles.yaml --profile customer
```

## Configuration file (conf)
The configuration files let you define which type of storage the export tool will save the backups to, and any additional storage-specific information that might be required. Currently AWS S3 and Local storage are supported.

If you run the script with `list-of-parents` or `entire-organization`, the script will fetch the IDs of workspaces to process (either hierarchies under the specified parents or all the workspaces within the organization) in batches. As a default, the batch size is set to `100`, but you can parametrize it by setting the `api_page_size` parametter in your configuration yaml.

The `batch_size` accepts integer value and determines how many workspaces will be processed before saving the backups to the selected storage. As a default, the batch size is set to 100.

The configuration file has the following format:
```yaml
storage_type: some_storage
storage:
  arg1: foo
  arg2: bar
api_page_size: 1000
batch_size: 20
```


### AWS S3

You can define the configuration file for S3 storage like so: 

```yaml
storage_type: s3
storage:
  bucket: some_bucket
  backup_path: some/path/to/backups/
  profile: services 
```
Here, the meaning of different `storage` fields is as follows:
- bucket - S3 storage bucket containing the backups
- backup_path - absolute path within the S3 bucket which leads to the root directory where the backups should be saved
- profile (optional) - AWS profile to be used
  
### Local Storage

```yaml
storage_type: local
storage:
batch_size: 100
```

In this case exports are saved to ./local_backups/ folder relative to where the script is executed from. The amount of backups already present in this folder might affect the performace of the script.

## Input CSV file (ws_csv)
The input CSV file defines the targets and sources for backup restores (imports).

Following format of the csv is expected:

| workspace_id |
|--------------| 
| ws_id_1      | 
| ws_id_2      | 
| ws_id_3      | 

Here, each `workspace_id` is the workspace ID of the workspace to perform the export on.
If the defined workspace does not exit in the target organization, this information will be present as ERROR log. If something fails, please read over all ERROR log messages for information where the issue lies.
