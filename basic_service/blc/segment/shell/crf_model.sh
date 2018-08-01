#!/bin/sh

templ_path=$1
train_corpus=$2
test_corpus=$3
output_model=$4
output_pred=$5

# crf learn
crf_learn $templ_path $train_corpus $output_model

# predict
crf_test -m $output_model $test_corpus > $output_pred

