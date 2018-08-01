#!/bin/sh

date=$1
cmd_type=$2

mkdir -p ./data
mkdir -p ./model
mkdir -p ./vocab
mkdir -p ./temp
mkdir -p ./lstm_crf_tool/data
mkdir -p ./lstm_crf_tool/Model
mkdir -p ./lstm_crf_tool/Res
mkdir -p ./lstm_crf_tool/Res/voc
mkdir -p ./lstm_crf_tool/Res/embed

# export data
note_file="data/note_result.txt"
product_word_file="vocab/product_word.dat"
brand_word_file="vocab/brand_word.dat"
bash -x ./shell/export.sh $date $note_file $product_word_file $brand_word_file

# prepare input data
train_file="data/corpus.train"
test_file="data/corpus.test"
bash -x ./shell/corpus.sh $cmd_type $note_file $train_file $test_file $product_word_file $brand_word_file

# train model && predict
pred_file="data/corpus.pred"
model_file="model/model.crf.m"
#bash -x ./shell/crf_model.sh ./conf/template $train_file $test_file $model_file $pred_file

# train lstm+crf
pred_file="data/corpus.pred.nn"
model_file="model/lstm_crf.m"
bash -x ./shell/lstm_crf_model.sh $train_file $test_file $model_file $pred_file

# evaluate
python ./script/eval.py $pred_file

# debug
view_file="data/corpus.view"
python ./script/view.py $pred_file $view_file
