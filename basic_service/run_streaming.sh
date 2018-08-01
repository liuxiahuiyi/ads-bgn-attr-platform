set -x


D_DAY=`date -d "3 day ago" +%Y-%m-%d`
hive -e "ALTER TABLE bgn.clothes_attr_online DROP IF EXISTS PARTITION(dt='${D_DAY}');"
hadoop fs -rm -r /user/mart_sz/bgn.db/clothes_attr_online/dt=${D_DAY}
hadoop fs -rm -r /user/bi_tao/.Trash/Current/user/mart_sz/bgn.db/clothes_attr_online


ITEM_SECOND_CATE_CD=$1
DT=`date -d last-day +%Y-%m-%d`
NAME=$2
INDEX_COM_FILE="${ITEM_SECOND_CATE_CD}""_""${NAME}""_get_data.sql"

sh generate_sql.sh $NAME $ITEM_SECOND_CATE_CD $DT
hive -f $INDEX_COM_FILE
INPUT="hdfs:/user/mart_sz/bgn.db/ext_data/item_second_cate_cd=${ITEM_SECOND_CATE_CD}/com_attr_cd=${NAME}"
OUTPUT="hdfs:/user/bi_tao/wangsanpeng/bgn_model_output/${ITEM_SECOND_CATE_CD}_${NAME}/"
REDUCER_NUM=10000
MAPPER_NUM=10000


MAPPER="./program/mapper.py"

REDUCER="./program/reducer.py"

PROGRAM_PATH="./program"

CHECKPOINTS_FILE="${ITEM_SECOND_CATE_CD}_${NAME}_best_model"
CHECKPOINTS_HDFS_PATH="hdfs:/user/bi_tao/wangsanpeng/checkpoints/${CHECKPOINTS_FILE}"
CHECKPOINTS_PATH="./checkpoints/${CHECKPOINTS_FILE}"
if [ ! -d "./checkpoints" ];then
mkdir ./checkpoints
fi

if [ ! -f $CHECKPOINTS_PATH ];then
hadoop fs -get $CHECKPOINTS_HDFS_PATH $CHECKPOINTS_PATH
fi


hadoop fs -test -e $OUTPUT
if [ $? -eq 0 ]
then
    hadoop fs -rm -r $OUTPUT
fi



hadoop fs -rm -r /user/bi_tao/.Trash/Current/user/


hadoop fs -rm -r /user/mart_sz/bi_tao/.Trash/Current/

hadoop jar /software/servers/hadoop-2.7.1/share/hadoop/tools/lib/hadoop-streaming-2.7.1.jar  \
           -D mapred.job.priority=HIGH  \
           -D mapred.job.name="$NAME"  \
           -D mapred.map.tasks=$MAPPER_NUM \
           -D mapreduce.map.memory.mb=4096 \
           -D mapreduce.map.java.opts=-Xmx2024M \
           -D mapred.reduce.tasks=$REDUCER_NUM \
           -cacheArchive "/user/bi_tao/wangsanpeng/age/python27.tar.gz#python27"  \
           -input "${INPUT}" \
           -output "${OUTPUT}"  \
           -mapper "python27/python27/bin/python2.7 ${MAPPER}  --item_second_cate_cd ${ITEM_SECOND_CATE_CD}  --model ./${CHECKPOINTS_FILE}/best_model --dt '${DT}' --com_attr_cd '${NAME}'" -file "${PROGRAM_PATH}" -file "${CHECKPOINTS_PATH}"  \
           -reducer "python27/python27/bin/python2.7 ${REDUCER}" -file "${PROGRAM_PATH}"
hive -e "load data inpath '/user/bi_tao/wangsanpeng/bgn_model_output/${ITEM_SECOND_CATE_CD}_${NAME}/' overwrite into table bgn.clothes_attr_online PARTITION (dt='${DT}',item_second_cate_cd='${ITEM_SECOND_CATE_CD}',com_attr_cd='${NAME}');"

hadoop fs -test -e $OUTPUT

if [ $? -eq 0 ]
then
    hadoop fs -rm -r $OUTPUT
fi

hadoop fs -rm -r /user/bi_tao/.Trash/Current/user/
hadoop fs -rm -r /user/mart_sz/bi_tao/.Trash/Current/
