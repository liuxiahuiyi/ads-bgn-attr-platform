set -x

ITEM_SECOND_CATE_CD=$1
DT1=$2
NUMBER=$3
DT2=$(date -d "$DT1 -3 month" +%Y-%m-%d)
DT3=$(date -d "$DT2 -3 month" +%Y-%m-%d)
DT4=$(date -d "$DT3 -3 month" +%Y-%m-%d)

if [ "${NUMBER}" == "" ]; then
NUMBER="3000000"
fi

WRITE_FILE="orig_w2v.txt"


INDEX_SQL="select a.* from (select sku_name from gdm.gdm_m03_item_sku_da a where item_second_cate_cd = 'ITEM_SECOND_CATE_CD_ORIG' and dt = 'date1' and sku_valid_flag = 1 and sku_status_cd != '3000' and sku_status_cd != '3010' and item_sku_id is not null order by rand() limit all_number) a\nunion all\nselect a.* from (select sku_name from gdm.gdm_m03_item_sku_da a where item_second_cate_cd = 'ITEM_SECOND_CATE_CD_ORIG' and dt = 'date2' and sku_valid_flag = 1 and sku_status_cd != '3000' and sku_status_cd != '3010' and item_sku_id is not null order by rand() limit all_number) a\nunion all\nselect a.* from (select sku_name from gdm.gdm_m03_item_sku_da a where item_second_cate_cd = 'ITEM_SECOND_CATE_CD_ORIG' and dt = 'date3' and sku_valid_flag = 1 and sku_status_cd != '3000' and sku_status_cd != '3010' and item_sku_id is not null order by rand() limit all_number) a\nunion all\nselect a.* from (select sku_name from gdm.gdm_m03_item_sku_da a where item_second_cate_cd = 'ITEM_SECOND_CATE_CD_ORIG' and dt = 'date4' and sku_valid_flag = 1 and sku_status_cd != '3000' and sku_status_cd != '3010' and item_sku_id is not null order by rand() limit all_number) a"


GET_TR_SQL=`echo -e "$INDEX_SQL" | sed  's/ITEM_SECOND_CATE_CD_ORIG/'${ITEM_SECOND_CATE_CD}'/g' | \
sed  's/date1/'${DT1}'/g' | \
sed  's/date2/'${DT2}'/g' | \
sed  's/date3/'${DT3}'/g' | \
sed  's/date4/'${DT4}'/g' | \
sed  's/all_number/'${NUMBER}'/g'`

# echo $GET_TR_SQL
hive -e "$GET_TR_SQL" > $WRITE_FILE
