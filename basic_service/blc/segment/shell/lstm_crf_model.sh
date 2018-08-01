#!/bin/sh

train_file=$1
test_file=$2
model_file=$3
pred_file=$4

config_file="./lstm_crf_tool/config.yml"

# 启动tf
#source /mnt/mfsb/bgn/concept_relation/taotong/valid_anaconda2
#source activate tensorflow

# 修改配置文件
python script/lstm_crf.py $config_file $train_file $test_file $pred_file

# 训练模型
cd lstm_crf_tool
/usr/bin/rm Model/*
/usr/bin/rm Res/voc/*

python preprocessing.py
python train.py
python test.py

# 停用tf
#source dctivate tensorflow
cd ..
