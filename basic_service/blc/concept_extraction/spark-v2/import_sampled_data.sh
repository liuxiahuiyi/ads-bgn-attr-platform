# sh import_sampled_data.sh 2018-04-06
date=$1
categories=('3c' 'babies' 'beauty' 'books' 'cars' 'clothes' 'foods' 'furniture' 'sports')

for c in ${categories[@]}
do
{
  hadoop fs -rm guokun/dgraph/$c
  
  hadoop fs -put /home/ads_anti/lvlei/entity-linking/entity_optimization/mysql/$date.$c guokun/dgraph/$c
  
  spark-submit \
  --num-executors 200 \
  --class "com.jd.bgn.DgraphSampledDataGetter" \
  target/scala-2.11/bgn-blc-assembly-0.1.0.jar \
  --date $date \
  --partition-category $c
} &
done
wait
