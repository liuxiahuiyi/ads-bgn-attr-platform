#!/bin/bash

if [ -z $1 ]; then
  echo "please input date!!"
  exit 1
fi
if [ -z $2 ]; then
  echo "please input item_first_cate_cd!!"
  exit 1
fi
if [ -z $3 ]; then
  echo "please input com_attr_cd!!"
  exit 1
fi

dir=$(dirname "$0")
hdfs_prefix='hdfs://ns1018/user/jd_ad/ads_aof'
current_date=`date +"%Y-%m-%d" -d "$1 -1 days"`
current_date_60=`date +"%Y-%m-%d" -d "$1 -60 days"`
formatter="${dir}/../pig/udf/formatter.py"
item_first_cate_cd=$2
com_attr_cd=$3
if [ -z $4 ]; then
  limits=1000000
else
  limits=$4
fi
train_data_hdfs="${hdfs_prefix}/personal/wangyike/attr-bert/item_first_cate_cd_${item_first_cate_cd}/com_attr_cd_${com_attr_cd}/train_data"

hadoop fs -rm -r "${train_data_hdfs}"


pig -f "${dir}/../pig/get_train_data.pig" \
  -useHCatalog \
  -p current_date=${current_date} \
  -p current_date_60=${current_date_60} \
  -p formatter=${formatter} \
  -p item_first_cate_cd=${item_first_cate_cd} \
  -p com_attr_cd=${com_attr_cd} \
  -p limits=${limits} \
  -p train_data_hdfs=${train_data_hdfs}
