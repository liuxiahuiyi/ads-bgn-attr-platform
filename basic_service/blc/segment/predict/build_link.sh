#!/bin/sh
ROOT_DIR=".."

function MakeLink(){
  mid=$1
  src=$2
  dest=$3
  #ln -s $ROOT_DIR/$mid/$src $dest
  cp -r $ROOT_DIR/$mid/$src $dest
}

function MakeSoftLink(){
  mid=$1
  src=$2
  dest=$3
  ln -s $ROOT_DIR/$mid/$src $dest
}

MakeLink lstm_crf_tool model_factory.py model_factory.py
MakeLink lstm_crf_tool load_data.py load_data.py
MakeLink lstm_crf_tool model.py model.py
MakeLink lstm_crf_tool utils.py utils.py
MakeLink lstm_crf_tool config.yml config.yml
MakeSoftLink lstm_crf_tool Res Res
MakeSoftLink lstm_crf_tool Model Model

MakeLink script util.py util.py
MakeLink script config.py config.py
MakeLink script feature_extractor.py feature_extractor.py

MakeSoftLink vocab brand_word.dat brand_word.dat
MakeSoftLink vocab product_word.dat product_word.dat
