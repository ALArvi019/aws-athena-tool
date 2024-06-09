#!/bin/bash

cp -r ~/.aws .

docker build -t aws-athena-tool .

rm -rf .aws

if [ "$1" == "wizard" ]; then
    docker run -it --rm aws-athena-tool python main.py wizard
elif [ "$1" == "arguments" ]; then
    shift
    docker run -it --rm aws-athena-tool python main.py $@
else
    echo "Usage: ./RUN.sh [wizard|argument] [arguments...]"
fi