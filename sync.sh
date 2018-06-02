#!/bin/bash 
COUNTER=0
while [  $COUNTER -lt 2 ]; do
    aws s3 sync models s3://mikeokslonger-ticks/models
    sleep 5m
done
