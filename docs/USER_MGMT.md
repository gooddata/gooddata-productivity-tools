# GD User Management
Tool which helps manage user entities in an GoodData organization.

Users can be created, updated, and deleted. This includes creation of any new userGroups which would be provided in user details.

## Usage

The tool requires the following argument on input:
- `user_csv` - a path to a csv file defining user entities, their relevant attributes, userGroup memberships, and isActive state

Some other, _optional_, arguments are:
- `-d | --delimiter` - column delimiter for the csv files. Use this to define how the csv is parsed. Default value is "`,`"
- `-u | --ug_delimiter` - userGroups column value delimiter. Use this to separate the different userGroups defined in the userGroup column. Default value is "`|`". Note that `--delimiter` and `--ug_delimiter` have to differ.
- `-q | --quotechar` - quotation character used to escape special characters (such as the delimiter) within the column field value. Default value is '`"`' If you need to escape the quotechar itself, you have to embed it in quotechars and then double the quotation character (e.g.: `"some""string"` will yield `some"string`).

Use the tool like so:
```sh
python scripts/user_mgmt.py user_csv
```
Where `user_csv` refers to input csv.

If you would like to define custom delimiters, use the tool like so:
```sh
python scripts/user_mgmt.py user_csv -d "," -u "|"
```

To show the help for using arguments, call:
```sh
python scripts/user_mgmt.py -h
```

## Input CSV file (user_csv)
The input CSV file defines the user entities which you might want to manage. Note that GD organization users that are not defined in the input will not be modified in any way.

[Example input csv.](examples/user_mgmt/input.csv)

Following format of the csv is expected:

| user_id              | firstname | lastname | email                   | auth_id   | user_groups | is_active |
|----------------------|-----------|----------|-------------------------|-----------|-------------|-----------|
| jozef.mrkva          | jozef     | mrkva    | jozef.mrkva@test.com    | auth_id_1 |             | True      |
| bartolomej.brokolica |           |          |                         |           |             | False     |
| peter.pertzlen       | peter     | pertzlen | peter.pertzlen@test.com | auth_id_3 | ug_1, ug_2  | True      |
| zoltan.zeler         | zoltan    | zeler    | zoltan.zeler@test.com   | auth_id_4 | ug_1        | True      |
| kristian.kalerab     | kristian  | kalerab  |                         | auth_id_5 |             | True      |
| richard.cvikla       |           |          | richard.cvikla@test.com | auth_id_6 | ug_1, ug_2  | False     |
| adam.avokado         |           |          |                         | auth_id_7 |             | False     |

Here, each `user_id` is the ID of the user to manage.

The `firstname`, `lastname`, `email`, and `auth_id` fields are optional attributes of the user.

The `user_groups` field specifies user group memberships of the user.

Lastly, the `is_active` field contains information about whether the user should or should not exist in the organization. The `is_active` field is case-insensitive and considers `true` as the only value taken as positive. Any other value in this field is considered negative (e.g.: `blabla` would evaluate to `False`).
