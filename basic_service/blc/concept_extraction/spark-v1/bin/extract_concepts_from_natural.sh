#!/usr/bin/env bash

loginfo() { echo "[INFO] $@"; }
logerror() { echo "[ERROR] $@" 1>&2; }
usage() { echo "USAGE: $0 jar" 1>&2; }

loginfo "==========================================================="
loginfo "submit job"

if [ ! -n "$1" ] || [ $1 = "0000-00-00" ]; then
    DATE=$(date -d "-1 days" +%Y-%m-%d)
else
    DATE=$1
fi
echo $DATE
if [ ! -n "$2" ]; then
    CATEGORY="3c"
else
    CATEGORY=$2
fi  
echo $CATEGORY

spark-submit \
--num-executors 100 \
--class "com.jd.bgn.ExtractConceptsFromNatural" \
bgn-blc-assembly-0.1.0.jar \
--hive-path /user/jd_ad/ads_bgn/ad_bgn.db/ \
--partition-category $CATEGORY \
--date $DATE

if [ $? != 0 ]; then
  logerror "spark submit failed"
  exit 1
fi
loginfo "submit success"

