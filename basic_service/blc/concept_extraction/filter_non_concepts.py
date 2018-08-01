# coding: utf-8
import codecs
import jieba

colors = list() 
brands = list()
stopwords = list()
product_words = list()


colors = u'金银蓝黑黄橙粉红绿灰白紫青棕色棕'

def check_color_word(word):
  for color in colors:
    if word.endswith(color):
      return True
  return False

#def check_color_word(word_):
#  global colors
#  if len(colors) == 0:
#    print 'check_color_word'
#    with codecs.open('intermediate/colors.txt', 'r', 'utf-8') as f:
#      for line in f.readlines():
#        word = line.strip()
#        if len(word) > 0:
#          colors.append(word)
#  for color in colors:
#    if (color in word_) or (word_ in color):
#      return True
#  return False

def check_eng_num_word(word):
  for ch in word:
    if ch < u'\u4e00' or ch > u'\u9fff':
      return True
  return False

def check_brand(word_):
  global brands
  if len(brands) == 0:
    print 'check brand'
    with codecs.open('intermediate/brands.txt', 'r', 'utf-8') as f:
      for line in f.readlines():
        word = line.strip()
        if len(word) > 0:
          brands.append(word)
  for brand in brands:
    if brand in word_ or word_ in brand:
      return True
  return False

def check_stopword(word_):
  global stopwords
  if len(stopwords) == 0:
    print 'check stop word'
    with codecs.open('stopwords.txt', 'r', 'utf-8') as f:
      for line in f.readlines():
        word = line.strip()
        if len(word) > 0:
          stopwords.append(word)
  for stopword in stopwords:
    if stopword in word_:
      return True
  return False

def check_product_word(word_):
  global product_words
  if len(product_words) == 0:
    print 'check product word'
    with codecs.open('rawdata/product_words.txt', 'r', 'utf-8') as f:
      for line in f.readlines():
        word = line.strip()
        if len(word) > 0:
          product_words.append(word)
  for product_word in product_words:
    if product_word in word_:
      return True
  return False

def check_exact_product_word(word_):
  global product_words
  if len(product_words) == 0:
    print 'check exact product word'
    with codecs.open('rawdata/product_words.txt', 'r', 'utf-8') as f:
      for line in f.readlines():
        word = line.strip()
        if len(word) > 0:
          product_words.append(word)
  if word_ in product_words:
    return True
  else:
    return False

# check the type of word
# 5 for product word
# 7 for concept
def check_word_type(word, check_color=True):
  if len(word) <= 1:
    return -1
  elif check_eng_num_word(word):
    return 2
  elif check_stopword(word):
    return 4
  elif check_color and check_color_word(word):
    return 1
  elif check_product_word(word):
    if check_exact_product_word(word):
      return 5
    else:
      return 6
  elif check_brand(word):
    return 3
  else:
    return 7


def filter_non_concepts(input_file_name, output_file_name):
  words = list()  
  with codecs.open(input_file_name, 'r', 'utf-8') as f:
    i = 0
    for line in f.readlines():
      if i % 10000 == 0:
        print i
      i += 1
      word = line.split('\t')[0].strip()
      count = int(line.split('\t')[1].strip())
      if count > 100:
        if check_word_type(word) == 7:
            words.append(word)
  with codecs.open(output_file_name, 'w', 'utf-8') as f:
    for word in words:
      f.write(word + '\n')

if __name__ == '__main__':
  filter_non_concepts('intermediate/cellphone_name_from_natural.txt', 'intermediate/cellphone_name_from_natural_concepts.txt')
