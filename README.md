# gooddata-productivity-tools
This repository contains tools that help with GoodData Cloud/CN workspace management, user and user group management, and backup/restore of workspaces.

This section of the documentation contains information on how to set up the environment and relevant authentication files. At the end of the Tools section, there is more specific documentation for each tool. The steps mentioned here are shared between them.

## Requirements
Python 3.10+

Depending on your environment, the statements can start either as
```sh
pip  
pip3
```
```sh
python
python3
```
please use the one that works for you and refers to python 3.10+.

The version can be checked by running
```sh
python -V
```

## Install
In order to install tooling requirements to the target environment, run the following: 

```sh
pip install -r requirements.txt
```

## Authentication

Overall, the scripts developed within the repository follow the credential/authentication provisioning conventions of the GoodData and any used storage provider (e.g. AWS).

The following section describes what credentials need to be set up, where to find them, and what format they should follow. If you need help with how to edit files in your user home folder (~), you can also refer to [step by step authentication setup guide](docs/SETUPATUHENTICATION.md).


### GoodData
When authenticating against GoodData, you can either export the required credentials using environment variables, or provide a GoodData profiles file.

For example, you can export the environment variables like so:

```sh
export GDC_AUTH_TOKEN="some_auth_token"
export GDC_HOSTNAME="https://host.name.cloud.gooddata.com/"
```

or you can choose to provide a GoodData `profiles.yaml` file of the following format:

```yaml
default:
  host: https://host.name.cloud.gooddata.com/
  token: some_auth_token

customer:
  host: https://customer.hostname.cloud.gooddata.com/
  token: other_auth_token
```

By default, a tool attempts to locate a GoodData profile file at `~/.gooddata/profiles.yaml`, but you can also choose to provide a custom path like so:

```sh
python scripts/restore.py <input.csv> <conf.yaml> -p path/to/profiles.yaml
```

You can define multiple GoodData profiles in a single profiles file. By default, the `default` profile is used, but you can choose different one to use. For example, if you want to tell a tool to use the `customer` profile defined in the example `profiles.yaml` above, you can do so like this:

```sh
python scripts/restore.py <input.csv> <conf.yaml> -p path/to/profiles.yaml --profile customer
```

In case of providing both ways of authentication to a tool, the environment variables takes precedence and the profiles config is ignored.

### AWS

When authenticating against AWS, the [conventions made by the boto3 library](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) are followed.

From the tool user perspective that means following the points 3. to 8. from the [Configuring Credentials section](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).

One example of how you can supply AWS credentials for tools, is by defining one or more AWS profiles in `~/.aws/credentials` file.

```
[default]
aws_access_key_id = some_key_id
aws_secret_access_key = some_access_key

[services]
aws_access_key_id = other_key_id
aws_secret_access_key = other_access_key
```

If you want to specify the specific AWS credentials profile to be used, see the tool-specific documentation.

## Tools

- [Backup workspace](docs/BACKUP.md)
- [Restore workspace](docs/RESTORE.md)
- [Workspace permission management](docs/PERMISSION_MGMT.md)
- [User management](docs/USER_MGMT.md)
- [User group management](docs/USER_GROUP_MGMT.md)


## Known MacOS issue SSL: CERTIFICATE_VERIFY_FAILED

If you are getting the following message:

`Caused by SSLError(SSLCertVerificationError(1, ‘[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1129)'))`

it is likely caused by Python and it occurs if you have installed Python directly from python.org.

To mitigate, please install your SSL certificates in HD -> Applications -> Python -> Install Certificates.command.

---

## Development

This section is aimed towards developers wanting to adjust / test the code. If you are regular user you can ignore following parts.

### Setup
To set up local development environment do the following:

1. (optional) Set up a local python virtual environment:

```sh
    python -m venv venv
    source venv/bin/activate
```

2. Install tool, dev, and test requirements:

```sh
pip install -r requirements.txt -r requirements-test.txt -r requirements-dev.txt
```


### Style checking, linting, and typing
The codebase (both, scripts and tests) is style, lint, and type checked when the CI/CD pipeline runs.

Linting and style-checking is done with help of `black` and `ruff`.

Type checking is done using `mypy`.

To run either of the mentioned tools locally, just call the tool with a target directory.

```sh
<black|ruff|mypy> <target_path>
```

 For example, in order to check the typing in the scripts, call the following from the repository's root directory:

```sh
mypy scripts
```


### Testing
The tooling test suite makes use of some third party tools, such as `pytest`, `tox`, and `moto`.

To run the test suite locally, ensure you have test and script requirements installed (see Setup step above) change working directory to repository's root and then call:

```sh
pytest .
```


### Tox
To run the test suite, linters and type checks locally you can also use `tox`.

To check everything at once, ensure youre in the repository's root directory and simply call:

```sh
tox
```

## Contributing
If you want to contribute to the project, please read the [contributing guide](CONTRIBUTING.md).
