# coding: utf-8
import codecs
import collections
from filter_non_concepts import check_word_type

colors = u'金银蓝黑黄橙粉红绿灰白紫青棕色棕'

def check_color(word):
  for color in colors:
    if word.endswith(color):
      return True
  return False

raw_concept_list = list()
concept_set = set()

freq_limit = 100
#freq_limit = 1

i = 0
with codecs.open('intermediate/cellphone_sku_name_processed.txt', 'r', 'utf-8') as f:
  for line in f.readlines():
    if i % 100000 == 0:
      print i
    i += 1
    r = line.strip().split()
    for c in r:
      raw_concept_list.append(c)

print len(raw_concept_list)

freq = collections.Counter(raw_concept_list)

i = 0
for c, f in freq.items():
  if i % 1000 == 0:
    print i
  i += 1
  if f <= freq_limit:
    continue
#  if check_word_type(c, check_color=False) == 7 and not check_color(c):
  if check_word_type(c) == 7:
    concept_set.add(c)
     
with codecs.open('intermediate/final_concepts.txt', 'w', 'utf-8') as f:
  for c in concept_set:
    f.write('%s\n' % c)
