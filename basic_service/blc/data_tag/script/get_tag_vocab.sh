#!/bin/sh

if [ ! -d "./data" ]; then
  mkdir ./data
fi

if [ ! -d "./vocab" ]; then
  mkdir ./vocab
fi

if [ ! -d "./vocab/3c" ]; then
  mkdir ./vocab/3c
fi

# export 3c vocab
outpath="/user/jd_ad/zhangyunfei13/bgn/resource/3c/export/vocab/"
#hive -e "insert overwrite directory '$outpath'
#         ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
#         select item_sku_id, brandname_cn, brandname_en, product_words_list 
#         from app.app_bgn_sku_attribute 
#         where dt = '2017-12-14'
#         and item_first_cate_cd in ('652', '670', '737', '9987')
#         "

# get brand && product vocab
#hadoop fs -cat /user/jd_ad/zhangyunfei13/bgn/resource/3c/export/vocab/* > data/3c_export_vocab.dat
#python script/get_pw_bw_vocab.py data/3c_export_vocab.dat vocab/3c/brand.dat vocab/3c/product.dat

# get note result
#python script/get_seq_note_vocab.py data/3c_note_result.dat data/3c_vocab_seq_note.dat

# get topk split vocab
ratio=98
#hadoop fs -cat /user/jd_ad/zhangyunfei13/bgn/vocab/3c/* | awk -F '\t' '{a[$1]+=$2}END{for(w in a){print w,a[w]}}' >  ./data/3c_vocab.dat
python script/vocab_topk.py data/3c_vocab.dat data/3c_vocab_$ratio.dat 0.${ratio}

# generate tag label split vocab
python script/get_tag_vocab.py data/3c_vocab_${ratio}.dat data/3c_vocab_seq_note.dat vocab/3c/brand.dat vocab/3c/product.dat data/3c_tag_vocab_${ratio}.dat

# select seed
#hadoop fs -cat /user/jd_ad/zhangyunfei13/bgn/cut/3c/* > data/3c_title.dat
python script/gen_tag_tuple.py data/3c_tag_tuple_${ratio}.dat data/3c_title.dat data/3c_tag_vocab_${ratio}.dat 5
