# GD Workspace Permission Management

Tool which helps manage user/userGroup bound workspace permissions within GoodData organization.

Goal of the tool is to help manage state of the user-workspace or userGroup-workspace permission pairs in a granular fashion (one input row per each permission - e.g. `user_1 - ws_id_1 - "ANALYZE"`).

## Usage

The tool requires the following argument on input:

- `perm_csv` - a path to a csv file defining workspace permissions bound to specific ws_id-user or ws_id-userGroup pairs and the permissions isActive state

Some other, _optional_, arguments are:

- `-d | --delimiter` - column delimiter for the csv files. Use this to define how the csv is parsed. Default value is "`,`"

Use the tool like so:

```sh
python scripts/permission_mgmt.py perm_csv
```

Where `perm_csv` refers to input csv.

If you would like to define custom delimiter, use the tool like so:

```sh
python scripts/permission_mgmt.py perm_csv -d ","
```

To show the help for using arguments, call:

```sh
python scripts/permission_mgmt.py -h
```

## Input CSV file (perm_csv)

The input CSV file defines the workspace permissions which you might want to manage.

[Example input csv.](examples/permission_mgmt/input.csv)

Following format of the csv is expected:

| user_id | ug_id | ws_id   | ws_permissions | is_active |
| ------- | ----- | ------- | -------------- | --------- |
| user_1  |       | ws_id_1 | ANALYZE        | True      |
| user_1  |       | ws_id_1 | VIEW           | False     |
| user_1  |       | ws_id_2 | MANAGE         | True      |
| user_2  |       | ws_id_1 | ANALYZE        | True      |
| user_2  |       | ws_id_2 | MANAGE         | True      |
|         | ug_1  | ws_id_1 | ANALYZE        | True      |
|         | ug_1  | ws_id_1 | VIEW           | True      |
|         | ug_1  | ws_id_1 | MANAGE         | False     |
|         | ug_2  | ws_id_1 | ANALYZE        | True      |
|         | ug_2  | ws_id_2 | MANAGE         | True      |

Here, each `user_id` is the ID of the user to manage, and `ug_id` is the ID of the user group to manage. Note that these fields are mutually exclusive and you should provide only one of the two values per each row.

The `ws_id` is the workspace ID that the permission is bound to.

Lastly, the `is_active` field holds boolean values containing information about whether the permission should or should not exist in the organization.
