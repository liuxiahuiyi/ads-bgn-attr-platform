#!/bin/sh
category=$1

sh word_count.sh

python get_word_prob.py

sh pack_model.sh $category
