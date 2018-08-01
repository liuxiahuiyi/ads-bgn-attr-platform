set -x

NAME=$1
ITEM_SECOND_CATE_CD=$2
DT1=$3
NUMBER=$4
DT2=$(date -d "$DT1 -3 month" +%Y-%m-%d)
DT3=$(date -d "$DT2 -3 month" +%Y-%m-%d)
DT4=$(date -d "$DT3 -3 month" +%Y-%m-%d)

if [ "${NUMBER}" == "" ]; then
NUMBER="1000000"
fi

WRITE_FILE="orig_tr.txt"

ALL_THIRD=`hive -e "select distinct item_third_cate_cd from gdm.gdm_m03_item_sku_da a where item_second_cate_cd = '${ITEM_SECOND_CATE_CD}' and dt = '${DT1}' and sku_valid_flag = 1 and sku_status_cd != '3000' and sku_status_cd != '3010' and item_sku_id is not null "`

INDEX_SQL=""
FIRST_FLAG="1"
for i in `echo "$ALL_THIRD" | sed 's/ /\n/g'`
do
if [ "${FIRST_FLAG}" == "1" ]; then
INDEX_SQL="${INDEX_SQL}""cate_id = '""${i}""' "
FIRST_FLAG="2"
else
INDEX_SQL="${INDEX_SQL}""or cate_id = '""${i}""' "
fi
done

GET_TR_SQL=`sed  "s/ITEM_THIRD_CATE_CD_ORIG/${INDEX_SQL}/g" get_train_data_orig.sql | \
sed  's/ITEM_SECOND_CATE_CD_ORIG/'${ITEM_SECOND_CATE_CD}'/g' | \
sed  's/date1/'${DT1}'/g' | \
sed  's/date2/'${DT2}'/g' | \
sed  's/date3/'${DT3}'/g' | \
sed  's/date4/'${DT4}'/g' | \
sed  's/com_cd/'${NAME}'/g' | \
sed  's/all_number/'${NUMBER}'/g'`

# echo $GET_TR_SQL
hive -e "$GET_TR_SQL" > $WRITE_FILE
