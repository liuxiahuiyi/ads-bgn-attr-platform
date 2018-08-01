#!/bin/sh
date=$1
category=$2
reducernum=$3

function check_partition(){
  # table, date, category
  temp=`hive -e "show partitions $1"`
  echo $temp|grep -wq "dt=$2/category=$3"
  if [ $? -eq 0 ];then
    return 0
  else
    return 1
  fi  
}

function get_data_from_hive(){
  # table, field_name, date, category, outputfile
  table=$1
  field_name=$2
  date=$3
  category=$4
  outputfile=$5

  data_dir=data_$date\_$category

  mkdir -p $data_dir
  
  hive -e "insert overwrite local directory './$data_dir'
           select distinct $field_name
           from $table 
           where dt='$date'
          "
  cat ./$data_dir/* > $outputfile
  rm -rf ./$data_dir
}

function get_data_from_hadoop(){
  # category, remotefile, outputfile
  category=$1
  remotefile=$2
  outputfile=$3
  rm ./$outputfile
  hadoop fs -get hdfs://ns3/user/jd_ad/ads_anti/taotong/bgn/word_prob/$category/$remotefile ./$outputfile
}

user_dict_file=user_dict_$date\_$category\.txt
product_words_file=product_words_$date\_$category\.txt

echo $user_dict_file
echo $product_words_file

# get user_dict and product_words
if check_partition ad_bgn.taxonomy_internal_user_dict_v2 $date $category ; then
  echo "get user dict from hive"
  get_data_from_hive ad_bgn.taxonomy_internal_user_dict_v2 word $date $category $user_dict_file
else
  echo "get user dict from hadoop"
  get_data_from_hadoop $category user_dict.txt $user_dict_file
fi

if check_partition ad_bgn.taxonomy_internal_product_words_v2 $date $category ; then
  echo "get product words from hive"
  get_data_from_hive ad_bgn.taxonomy_internal_product_words_v2 product_word $date $category $product_words_file
else
  echo "get_product_words from hadoop"
  get_data_from_hadoop $category product_words.txt $product_words_file
fi


sku_data_path="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/sku_attribute/dt=$date/category=$category"
output_path="hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/taxonomy_sku_basic_segmentation/dt=$date/category=$category"

hadoop fs -rm -r -f $output_path

hadoop jar "/software/servers/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar" \
  -D mapred.job.name="bgn_process_sku_title" \
  -D mapred.reduce.tasks=$reducernum \
  -files ../basic_segmentation/bgnseg.py,process_sku_name_hadoop.py,$user_dict_file,$product_words_file \
  -input $sku_data_path \
  -output $output_path \
  -mapper "anaconda2/anaconda2/bin/python2.7 process_sku_name_hadoop.py map $user_dict_file $product_words_file" \
  -reducer "cat" \
  -cacheArchive /user/jd_ad/zhangyunfei13/tool/anaconda2.tar.gz#anaconda2 \
  -cacheArchive /user/jd_ad/ads_anti/taotong/bgn/word_prob/$category/word_prob.tar.gz#word_prob

hive -e "msck repair table ad_bgn.taxonomy_sku_basic_segmentation"

rm $user_dict_file
rm $product_words_file
