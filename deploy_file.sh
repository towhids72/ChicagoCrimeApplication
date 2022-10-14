#!/bin/sh

echo 'Preparing to build and run dokcer containers...'

#other configs goes here

echo 'Creating docker network'
docker network create chicago_network

echo 'Building and running docker containers'
docker-compose up -d --build
