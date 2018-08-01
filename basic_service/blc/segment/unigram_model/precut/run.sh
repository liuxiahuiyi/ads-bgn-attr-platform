date=$1
category=$2

sh get_data.sh $category 

python get_user_dict.py

sh process_sku_name_hadoop.sh $date $category
