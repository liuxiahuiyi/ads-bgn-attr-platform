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
current_date=`date +"%Y-%m-%d" -d "$1 -1 days"`
clear_date=`date +"%Y-%m-%d" -d "$1 -3 days"`
hdfs_prefix='hdfs://ns1018/user/jd_ad/ads_aof'
item_first_cate_cd=$2
com_attr_cd=$3
if [ -z $4 ]; then
  reducer_num=5
else
  reducer_num=$4
fi
if [ -z $5 ]; then
  db='ad_bgn'
else
  db=$5
fi
if [ -z $6 ]; then
  table='bgn_attr_bert_model'
else
  table=$6
fi
if [ -z $7 ]; then
  source=60
else
  source=$7
fi

category_dir_hdfs="${hdfs_prefix}/personal/wangyike/attr-bert/item_first_cate_cd_${item_first_cate_cd}/com_attr_cd_${com_attr_cd}"
vocab_file_hdfs="${hdfs_prefix}/personal/wangyike/bert_pretrain/vocab.txt"

hadoop fs -test -e "${category_dir_hdfs}/checkpoint"
if [ $? -ne 0 ]; then
  echo "remote checkpoint not exists!!"
  exit 1
fi
hadoop fs -test -e "${category_dir_hdfs}/attr_values.pkl"
if [ $? -ne 0 ]; then
  echo "remote attr_values.pkl not exists!!"
  exit 1
fi
hadoop fs -test -e "${vocab_file_hdfs}"
if [ $? -ne 0 ]; then
  echo "remote vocab.txt not exists!!"
  exit 1
fi

hadoop fs -rm -r "${category_dir_hdfs}/infer_data"
pig -f "${dir}/../pig/get_infer_data.pig" \
  -useHCatalog \
  -p current_date=${current_date} \
  -p item_first_cate_cd=${item_first_cate_cd} \
  -p com_attr_cd=${com_attr_cd} \
  -p infer_data_hdfs="${category_dir_hdfs}/infer_data"
if [ $? -ne 0 ]; then
  exit 1
fi

hive -f "${dir}/create_table.sql" \
  -hivevar clear_date=${clear_date} \
  -hivevar db=${db} \
  -hivevar table=${table} \
  -hivevar hdfs_prefix=${hdfs_prefix}
if [ $? -ne 0 ]; then
  exit 1
fi

hadoop_streaming_input="${category_dir_hdfs}/infer_data"
hadoop_streaming_output="${hdfs_prefix}/${db}.db/${table}/dt=${current_date}/item_first_cate_cd=${item_first_cate_cd}/com_attr_cd=${com_attr_cd}"
hadoop fs -rm -r "${hadoop_streaming_output}"
hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar \
  -D mapred.job.name="attr-bert-model_${item_first_cate_cd}_${com_attr_cd}" \
  -D mapred.reduce.tasks=${reducer_num} \
  -D mapreduce.map.memory.mb=8192 \
  -archives "${hdfs_prefix}/personal/wangyike/anaconda3/anaconda3.tar.gz#anaconda3" \
  -files "${category_dir_hdfs}/checkpoint#checkpoint,${category_dir_hdfs}/attr_values.pkl#attr_values_file,${vocab_file_hdfs}#vocab_file" \
  -file "${dir}/../python3" \
  -input "${hadoop_streaming_input}" \
  -output "${hadoop_streaming_output}" \
  -mapper "anaconda3/anaconda3/bin/python3.6 python3/mapper.py --checkpoint checkpoint --attr_values_file attr_values_file --vocab_file vocab_file --source ${source}" \
  -reducer "cat"
if [ $? -ne 0 ]; then
  exit 1
fi

hadoop fs -rm -r "${hadoop_streaming_input}"
hive -e "msck repair table ${db}.${table}"








