#!/bin/bash

if [ -z $1 ]; then
  echo "please input item_first_cate_cd!!"
  exit 1
fi
if [ -z $2 ]; then
  echo "please input com_attr_cd!!"
  exit 1
fi

dir=$(dirname "$0")
hdfs_prefix='hdfs://ns1018/user/jd_ad/ads_aof'
item_first_cate_cd=$1
com_attr_cd=$2

bert_pretrain_hdfs="${hdfs_prefix}/personal/wangyike/bert_pretrain"
bert_pretrain_local="${dir}/../data/bert_pretrain"
if [ ! -d "${bert_pretrain_local}" ]; then
  mkdir -p "${bert_pretrain_local}"
  hadoop fs -get "${bert_pretrain_hdfs}/*" "${bert_pretrain_local}"
fi

category_dir_hdfs="${hdfs_prefix}/personal/wangyike/attr-bert/item_first_cate_cd_${item_first_cate_cd}/com_attr_cd_${com_attr_cd}"
category_dir_local="${dir}/../data/attr-bert/item_first_cate_cd_${item_first_cate_cd}/com_attr_cd_${com_attr_cd}"
hadoop fs -test -e "${category_dir_hdfs}/train_data"
if [ $? -ne 0 ]; then
  echo "remote train_data not exists!!"
  exit 1
fi
/bin/rm -rf "${category_dir_local}/train_data"
mkdir -p "${category_dir_local}/train_data"
hadoop fs -cat "${category_dir_hdfs}/train_data/*" > "${category_dir_local}/train_data/raw.csv"

python "${dir}/../python3/train.py" \
  --category_dir "${category_dir_local}" \
  --bert_pretrain "${bert_pretrain_local}" \
  --vocab_file "${bert_pretrain_local}/vocab.txt"
if [ $? -ne 0 ]; then
  exit 1
fi

hadoop fs -rm -r "${category_dir_hdfs}/checkpoint"
hadoop fs -rm -r "${category_dir_hdfs}/attr_values.pkl"
hadoop fs -copyFromLocal "${category_dir_local}/checkpoint" "${category_dir_hdfs}"
hadoop fs -copyFromLocal "${category_dir_local}/attr_values.pkl" "${category_dir_hdfs}"

