import codecs

brand_file = 'brandname_raw.txt'  
product_words_file = 'product_words_raw.txt'
user_dict_file = 'user_dict.txt'
product_file = 'product_words.txt'

def check_all_chinese(word):
  for ch in word:
    if ch < u'\u4e00' or ch > u'\u9fff':
      return False
  return True

def check_contains_chinese(word):
  for ch in word:
    if ch >= u'\u4e00' and ch <= u'\u9fff':
      return True
  return False

pn_set = set()
brand_set = set()

with codecs.open(brand_file, 'r', 'utf-8') as f:
  for line in f:
    if len(line.strip()) > 1 and check_all_chinese(line.strip()):
      brand_set.add(line.strip())

with codecs.open(product_words_file, 'r', 'utf-8') as f:
  for line in f:
    for s in line.strip().split(','):
      if len(s.strip()) > 1 and check_contains_chinese(s):
        pn_set.add(s.strip())

all_set = pn_set | brand_set

with codecs.open(user_dict_file, 'w', 'utf-8') as f:
  for word in all_set:
    f.write(word + '\n')

with codecs.open(product_file, 'w', 'utf-8') as f:
  for word in pn_set:
    f.write(word + '\n')
