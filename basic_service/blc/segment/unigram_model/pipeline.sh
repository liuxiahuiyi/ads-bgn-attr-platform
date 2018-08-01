#!/bin/sh
date=$1
category=$2

cd precut
sh run.sh $date $category

cd ../word_count
sh run.sh $category
