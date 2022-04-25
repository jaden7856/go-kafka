#!/bin/bash

#ziphome="/home/oracle/prj/bin"

rm -f *.json *.log
rm -f ./producer
go build -o producer kafka-producer.go