#!/bin/bash

echo " flight trip spark job started "
spark-submit --master yarn --deploy-mode cluster --num-executors 10 --executor-cores 2 flights_trip.py

echo " flight trip spark job completed "
