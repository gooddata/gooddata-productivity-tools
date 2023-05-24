#!/usr/bin/env bash
# This creates fake ~/.aws/credentials file for boto3 mocking
mkdir ~/.aws && touch ~/.aws/credentials && echo "[default]\naws_access_key_id = test\naws_secret_access_key = test" > ~/.aws/credentials
