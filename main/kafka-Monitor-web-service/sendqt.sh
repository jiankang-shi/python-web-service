#!/bin/bash
today=`date +%F/%T`

send_qt_message1(){
  content1="$today\n$1"
  curl -X POST \
  http://qtalk.corp.qunar.com/innerpackage/corp/message/send_http_message.qunar \
  -H 'Content-Type: application/json' \
  -d '{
    "from": "dataplatform",
    "fromhost": "ejabhost1",
    "to": [{
        "user": "8c943604729347e1ae2aa6f1b8c064b2",
        "host": "conference.ejabhost1"
    }],
    "type": "groupchat",
    "extendinfo": "",
    "msgtype": "1",
    "content": "'$content1'",
    "system": "pf_bigdata_airflow",
    "auto_reply": "false",
    "backupinfo": ""
     }'
}

if [[ $# -eq 1 ]];then
  send_qt_message1 $1
else
  echo "usage: need four parameter or one"
fi