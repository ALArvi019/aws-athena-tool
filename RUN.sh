#!/bin/bash

cp -r ~/.aws .

docker build -t aws-athena-tool .

rm -rf .aws

docker run -it --rm aws-athena-tool
