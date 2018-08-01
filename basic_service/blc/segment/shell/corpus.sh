#!/bin/sh

cmd_type=$1
note_file=$2
train_file=$3
test_file=$4
product_word_file=$5
brand_word_file=$6

test_skuid_file="${test_file}.skuid"
train_file_raw="${train_file}.raw"
test_file_raw="${test_file}.raw"
note_file_clean="${note_file}.clean"

# normalise
python script/normalise_note.py $note_file vocab/product_word.dat vocab/brand_word.dat $note_file_clean

# split corpus
if [ $cmd_type = "once" ]; then
  cmd="python ./script/split_corpus.py seed $note_file_clean $test_skuid_file"
  echo "$cmd"
  $cmd
  
  cmd="python ./script/split_corpus.py include $note_file_clean $test_file_raw $test_skuid_file"
  echo "$cmd"
  $cmd

  cmd="python ./script/split_corpus.py exclude $note_file_clean $train_file_raw $test_skuid_file"
  echo "$cmd"
  $cmd
elif [ $cmd_type = "continue" ]; then
  cmd="python ./script/split_corpus.py exclude $note_file_clean $train_file_raw $test_skuid_file"
  echo "$cmd"
  $cmd
 
  cmd="python ./script/split_corpus.py include $note_file_clean $test_file_raw $test_skuid_file"
  echo "$cmd"
  $cmd
fi

# convert to crf++ format data
python ./script/convert2crf++.py $test_file_raw $test_file $product_word_file $brand_word_file
python ./script/convert2crf++.py $train_file_raw $train_file $product_word_file $brand_word_file

