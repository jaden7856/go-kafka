#!/bin/bash

#gsCL=( "cnt" "lop")

#  -9 go-client
topic="test2"

export KAFKA_PEERS=192.168.33.131:9092,192.168.33.132:9092,192.168.33.133:9092

# shellcheck disable=SC2181
if [ "$?" == "0" ]
then

  for nSize in 1024 # 512 1024 2048 4096 8192 16384 32768 65536
  do
    # shellcheck disable=SC2219
    let nCount=1000000

    for nIxTest in 1 # 2 3 4 5 # 반복 테스트 횟수 # [vvv] 5
    do
      sLogName=ztime-producer-$nSize-part3.json

      echo "./producer -topic=$topic -size=$nSize -count=$nCount -logtime=$sLogName"
      ./producer -topic=$topic -size=$nSize -count="$nCount" -logtime=$sLogName

      if [ "$?" == "0" ]
      then
        echo "OK"
      else
        touch zzz_producer.err
      fi
    done
  done
fi