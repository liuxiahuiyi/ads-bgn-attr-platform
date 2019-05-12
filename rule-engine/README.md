# package

```
mvn clean package
```

# launch

```
./launch/launch.sh date target_db_and_table path_of_rules_file(local or hdfs path)
```

# rules config

* item_first_cate_cds

define item_first_cate_cds that should be revised (necessary), e.t.

```
item_first_cate_cds = 652,670,9987
```

* com_attr_cds

define com_attr_cds in one item_first_cate_cd that should be revised (necessary), e.t.

```
// separator "+" represents similar attr, separator "," represents independent attr
652.com_attr_cds = 3184+3680+878,3626
```

* columns

定义某一(item_first_cate_cd,com_attr_cd)中，sku的meta data的列名，多个列名","分割，支持"sku_name","colour","size","jd_prc","barndname_full"

```
// default "sku_name"
652.3184+3680+878.columns = sku_name
```

* category.split

定义某一(item_first_cate_cd,com_attr_cd)中，按照哪个类目层级进行修复，支持"item_first_cate_cd","item_second_cate_cd","item_third_cate_cd"
```
// default "item_second_cate_cd"
652.3184+3680+878.category.split = item_first_cate_cd
```

* revision.bulk

定义某一(item_first_cate_cd,com_attr_cd)中，修复无效的属性还是全量修复，支持"invalid","all"

```
// default "invalid"
652.3184+3680+878.revision.bulk = invalid
```

* attr_value.length_mode

定义某一(item_first_cate_cd,com_attr_cd)中，计算属性值长度采取哪种方式，支持"word","char",主要用于属性值的排序

```
// default "char"
652.3184+3680+878.attr_value.length_mode = word
```

* attr_value.omit

定义某一(item_first_cate_cd,com_attr_cd)中，需要滤除的属性值脏数据，","分割

```
// default ""
652.3184+3680+878.attr_value.omit = 222
```

* rule

定义某一(item_first_cate_cd,com_attr_cd)中，适用规则的名字，目前支持"matching","range"，可扩展

```
// default "matching"
652.3184+3680+878.rule = matching
```


