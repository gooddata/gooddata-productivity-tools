# Authentication

This section contains step by step guide how to set up authorization files either by using GUI or Terminal.

## MacOS

### GUI

In Finder, go to your current user home directory by pressing 
```sh
CMD+SHIFT+H
```

And subsequently reveal hidden folders by
```sh
CMD+SHIFT+.
```
Now you can create required folders and files manually. To hide folders afterwards, press the same combination.

### Terminal

1. Open Terminal
2. You should be in your current user home directory. You can check it by executing 

```sh
pwd
```

Expected result: /Users/{your_username}

If thats not the case, run

```sh
cd ~ 
```

Create directories `.aws` and `.gooddata` by executing following statements:

```sh
mkdir .aws
mkdir .gooddata
```

First create aws `credentials` file in the `.aws` directory:

```sh
nano .aws/credentials
```

Populate the credentials file with appropriate credentials.

Format:
```
[default]
aws_access_key_id = some_access_key_id
aws_secret_access_key = some_access_key

[customer1]
aws_access_key_id = other_access_key_id
aws_secret_access_key = other_access_key
```
Save by pressing ctrl+X, Y and Enter.


Now create create `profiles.yaml` file within the `.gooddata` folder:

```sh
nano .gooddata/profiles.yaml
```

Format:  
```yaml
default:
  host: https://host.name.cloud.gooddata.com/
  token: some_auth_token

customer:
  host: https://customer.hostname.cloud.gooddata.com/
  token: other_auth_token
```
Save by pressing ctrl+X, Y and Enter.


## Windows

### GUI

Navigate to your user folder. That’s `C:\Users\USERNAME\` (replace `USERNAME` with your actual username). Inside create a new folder named `.aws`, and inside the `.aws` folder create a file named `credentials`. The full path should look like this: `C:\Users\USERNAME\.aws\credentials`.

If you cannot see the `.aws` file after creating it, ensure you can [see hidden files](https://support.microsoft.com/en-us/windows/view-hidden-files-and-folders-in-windows-97fbc472-c603-9d90-91d0-1166d1d9f4b5#WindowsVersion=Windows_11) on your computer.

Inside the `credentials` file set up the necessary AWS credentials in a following format:

```
[default]
aws_access_key_id = some_access_key_id
aws_secret_access_key = some_access_key

[customer1]
aws_access_key_id = other_access_key_id
aws_secret_access_key = other_access_key
```

Now, on the same path (`C:\Users\USERNAME\`) create a new folder named `.gooddata`. Inside this folder create a file named `profiles.yaml`. The full path should look like this: `C:\Users\USERNAME\.gooddata\profiles.yaml`.

Inside the `profiles.yaml` file set up the necessary GoodData credentials in the following format:

```yaml
default:
  host: https://host.name.cloud.gooddata.com/
  token: some_auth_token

customer:
  host: https://customer.hostname.cloud.gooddata.com/
  token: other_auth_token
```
