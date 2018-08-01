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

week=$(date +%w)
echo $week

if [ $week = 1 ]; then
  spark-submit \
    --num-executors 100 \
    --class "com.jd.bgn.BlcResultToSkuService" \
    bgn-application-assembly-0.1.0.jar \
    --date $DATE

  if [ $? != 0 ]; then
    logerror "spark submit failed"
    exit 1
  fi
  loginfo "submit success" 
else
  hadoop fs -mkdir /user/jd_ad/index_resource/ad_sku_offline_data/sku/sku_attr_field_data/daily/$DATE/bgn_info_field
  hadoop fs -touchz /user/jd_ad/index_resource/ad_sku_offline_data/sku/sku_attr_field_data/daily/$DATE/bgn_info_field/_SUCCESS
fi
