import codecs
import random
import itertools

concepts = list()
sku_names = list()

with codecs.open('result/concepts_with_probability_pos_decay.txt', 'r', 'utf-8') as f:
  for line in f.readlines():
    concepts.append(line.split(':')[0].strip())

with codecs.open('rawdata/cellphone_sku_name.txt', 'r', 'utf-8') as f1, \
     codecs.open('rawdata/cellphone_item_sku_id.txt', 'r', 'utf-8') as f2:
  for sku, id_ in itertools.izip(f1.readlines(), f2.readlines()):
    sku_names.append((sku.strip(), id_.strip()))

# randomly find sku name which includes concepts, considering priority
# using a recursive mode
def get_random_ele_from_multi_lists(eles, lists, num):
  if len(lists[0]) >= num:
    r = random.sample(range(len(lists[0])), num)
    for n in range(num):
      eles.append(lists[0][r[n]])
    return eles
  else:
    for x in lists[0]:
      eles.append(x)
    if len(lists) > 1:
      next_num = num - len(lists[0])
      next_lists = lists[1:]
      return get_random_ele_from_multi_lists(eles, next_lists, next_num)
    else:
      return eles


def get_related_sku_name(concept):
  sku_list_1 = list()
  sku_list_2 = list()
  sku_list_3 = list()
# sku_list_1 is the best match, while sku_list_3 is acceptable match
# priority will be considered when randomly selecting sku name
  for sku_name_raw in sku_names:
    sku_name = u' ' + sku_name_raw[0] + u' '
    if (u' ' + concept + u' ') in sku_name:
      sku_list_1.append(sku_name_raw)
    elif (u' '+concept) in sku_name or (concept+u' ') in sku_name:
      sku_list_2.append(sku_name_raw)
    elif concept in sku_name:
      sku_list_3.append(sku_name_raw)
    else:
      pass
  return_list_1 = list()
  return_list_2 = list()
  result1 = get_random_ele_from_multi_lists(return_list_1, [sku_list_1, sku_list_2, sku_list_3], 2)
  result2 = get_random_ele_from_multi_lists(return_list_2, [sku_list_3, sku_list_2, sku_list_1], 2)
  return result1, result2

with codecs.open('result/label_data_1.csv', 'w', 'utf-8') as f1,\
     codecs.open('result/label_data_2.csv', 'w', 'utf-8') as f2,\
     codecs.open('result/label_data_3.csv', 'w', 'utf-8') as f3,\
     codecs.open('result/label_data_4.csv', 'w', 'utf-8') as f4:
  f1.write('concept,sku_name,sku_id\n')
  f2.write('concept,sku_name,sku_id\n')
  f3.write('concept,sku_name,sku_id\n')
  f4.write('concept,sku_name,sku_id\n')
  for concept in concepts:
    skus, skus_negative = get_related_sku_name(concept)
    f1.write(concept+','+skus[0][0] + ',' + skus[0][1]+'\n')
    next_num = 1 if len(skus)>1 else 0
    f2.write(concept+','+skus[next_num][0] + ',' + skus[next_num][1]+'\n')
    skus = skus_negative
    f3.write(concept+','+skus[0][0] + ',' + skus[0][1]+'\n')
    next_num = 1 if len(skus)>1 else 0
    f4.write(concept+','+skus[next_num][0] + ',' + skus[next_num][1]+'\n')

