#!/bin/sh

if [ ! -d "./data" ]; then
  mkdir ./data
fi

if [ ! -d "./log" ]; then
  mkdir ./log
fi

bash script/run_3c.sh
bash scrip/run_clothe.sh

