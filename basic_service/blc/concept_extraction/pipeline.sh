if [ ! -n "$1" ]; then
    DATE=$(date -d "-1 days" +%Y-%m-%d)
else
    DATE=$1
fi
echo $DATE

if [ ! -d "./intermediate" ]; then
  mkdir ./intermediate
fi

if [ ! -d "./result" ]; then
  mkdir ./result
fi

./delete_intermediate_and_result.sh

echo 'extract_natural_phrase.py'
python extract_natural_phrase.py
echo 'get_color.py'
python get_color.py
echo 'get_brand.py'
python get_brand.py
echo 'filter_non_concepts.py'
/home/ads_anti/bin/python/bin/python2.7 filter_non_concepts.py
echo 'deduplicate_concepts.py'
/home/ads_anti/bin/python/bin/python2.7 deduplicate_concepts.py intermediate/cellphone_name_from_natural_concepts.txt intermediate/deduplicated_concepts.txt
echo 'make_userdict.py'
/home/ads_anti/bin/python/bin/python2.7 make_userdict.py
echo 'process_sku_name.py'
/home/ads_anti/bin/python/bin/python2.7 process_sku_name.py
echo 'get_final_concepts.py'
/home/ads_anti/bin/python/bin/python2.7 get_final_concepts.py
echo 'deduplicate filinal concepts'
/home/ads_anti/bin/python/bin/python2.7 deduplicate_concepts.py intermediate/final_concepts.txt result/concepts.txt 

paste -d "\t" rawdata/cellphone_sku_id_name.txt intermediate/cellphone_sku_name_processed.txt > sku_info.txt
hadoop fs -mkdir yangxi/bgn/blc/concept_extraction/$DATE
hadoop fs -put rawdata/product_words.txt yangxi/bgn/blc/concept_extraction/$DATE
hadoop fs -put result/concepts.txt yangxi/bgn/blc/concept_extraction/$DATE
hadoop fs -put sku_info.txt yangxi/bgn/blc/concept_extraction/$DATE

spark-submit \
--num-executors 100 \
--class "com.jd.bgn.GenerateSkuConcept" \
target/scala-2.11/bgn-blc-assembly-0.1.0.jar \
--date $DATE \
--project-path yangxi/bgn/blc/concept_extraction/
