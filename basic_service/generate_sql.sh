set -x

NAME=$1
ITEM_SECOND_CATE_CD=$2
DT=$3

INDEX_FILE="${ITEM_SECOND_CATE_CD}""_get_data.sql"
INDEX_COM_FILE="${ITEM_SECOND_CATE_CD}""_""${NAME}""_get_data.sql"


if [ ! -f $INDEX_FILE ];then
ALL_THIRD=`hive -e "select distinct item_third_cate_cd from gdm.gdm_m03_item_sku_da a where item_second_cate_cd = '${ITEM_SECOND_CATE_CD}' and dt = '${DT}' and sku_valid_flag = 1 and sku_status_cd != '3000' and sku_status_cd != '3010' and item_sku_id is not null "`
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
sed  "s/ITEM_THIRD_CATE_CD_ORIG/${INDEX_SQL}/g" get_data_all_orig.sql | sed  's/ITEM_SECOND_CATE_CD_ORIG/'${ITEM_SECOND_CATE_CD}'/g' > $INDEX_FILE
else
echo "get data already exist"
fi

if [ "${DT}" == "0" ]; then
echo "not replace date and com_cd"
else
sed  's/2018-05-01/'$DT'/g' $INDEX_FILE | sed  's/com_cd/'${NAME}'/g'  > $INDEX_COM_FILE
fi
