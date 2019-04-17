#!/bin/sh -e
while true
do
 eventTime=$(date +"%Y-%m-%d-%T")
 userId=$(( ( RANDOM % 50 ) + 1 ))
 appId=$(( ( RANDOM % 100 ) + 1 ))
 appScore=$(( ( RANDOM % 100 ) + 1 ))
 appData=SomeTestData
 echo "$eventTime,$userId,$appId,$appScore,$appData"
 aws kinesis put-record --stream-name louis_stream --data "$eventTime,$userId,$appId,$appScore,$appData"$'\n' --partition-key $appId --region eu-west-1
done
