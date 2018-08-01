import codecs
import collections


def get_set(input_file_name):
  words = set()
  with codecs.open(input_file_name, 'r', 'utf-8') as f:
    for word in f.readlines():
      if len(word.strip()) > 0:
        words.add(word.strip())
  return words

  
if __name__ == '__main__':
  a = get_set('rawdata/cellphone_brandname_cn.txt')
  b = get_set('rawdata/cellphone_brandname_en.txt')
  c = get_set('rawdata/cellphone_brandname_full.txt')
  d = a | b | c
  with codecs.open('intermediate/brands.txt', 'w', 'utf-8') as f:
    for word in d:
      f.write(word + '\n')
