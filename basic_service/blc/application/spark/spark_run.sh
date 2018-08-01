if [ ! -n "$1" ] || [ $1 = "0000-00-00" ]; then
    DATE=$(date -d "-1 days" +%Y-%m-%d)
else
    DATE=$1
fi
echo $DATE
if [ ! -n "$2" ]; then
    CATEGORY="3c"
else
    CATEGORY=$2
fi
echo $CATEGORY
#BlcResultToSkuService
spark-submit \
--num-executors 100 \
--class "com.jd.bgn.BlcResultToSkuService" \
target/scala-2.11/bgn-application-assembly-0.1.0.jar \
--partition-category $CATEGORY \
--date $DATE \
--hive-path hdfs://ns3/user/jd_ad/ads_bgn/ad_bgn.db/ 
