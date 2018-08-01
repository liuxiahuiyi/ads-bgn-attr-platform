#!/usr/bin/env bash

if [ $# -lt 2 ]; then
  echo "parameter not enough"
  exit 1
fi

if [ ! -n "$1" ] || [ $1 = "0000-00-00" ]; then
  DATE=$(date -d "-1 days" +%Y-%m-%d)
else
  DATE=$1
fi
echo $DATE

for i in $@
do
  if [ $i != $1 ]; then
    sh make_user_dict.sh $DATE $i
  fi
done
