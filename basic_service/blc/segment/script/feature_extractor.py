#coding=utf8
#author=zhangyunfei
punct_set = u''':!),:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…*/'''
punct_set = set(punct_set)

char_map_dict = {u' ' : '__SPAC__',
                 u'\t' : '__TAB__'}
char_map_dict_reverse = {v:k for k, v in char_map_dict.items()}

# 将一个词中的空白符做转换
def convert_space(seq):
  word = ''
  for s in seq:
    word += char_map_dict.get(s, s)
  return word

def pure_digit(word):
  for i in range(len(word)):
    if word[i] < '0' or word[i] > '9':
      return False
  return True
def pure_en(word):
  for i in range(len(word)):
    if not((word[i] >= 'a' and word[i] <= 'z') or \
           (word[i] >= 'A' and word[i] <= 'z')):
      return False
  return True

def pure_en_digit(word):
  for i in range(len(word)):
    if not((word[i] >= 'a' and word[i] <= 'z') or \
           (word[i] >= 'A' and word[i] <= 'z') or \
           (word[i] >= '0' and word[i] <= '9')):
      return False
  return True

def pure_punct(word):
  for i in range(len(word)):
    if word[i] not in punct_set:
      return False
  return True

def pure_space(word):
  for i in range(len(word)):
    if word[i] != ' ':
      return False
  return True

# 特征：1. 是否只有数字；
#       2. 是否只有英文和数字
#       3. 是否只有英文
#       4. 是否是产品词
#       5. 是否是品牌词
#       6. 其他统计特征
#       7. 是否是标点符号
#       8. 是否是空白符
def get_word_features(word, product_words=None, brand_words=None, other_vocab_list=None):
  word = word.strip()
  if len(word) == 0:
    return None
  features = [word]
  vocab_list = list()
  if product_words:
    vocab_list.append(product_words)
  if brand_words:
    vocab_list.append(brand_words)
  if other_vocab_list:
    vocab_list.extend(other_vocab_list)
  for vocabs in vocab_list:
    features.append(str(int(word in vocabs)))
  features.append(str(int(pure_digit(word))))
  features.append(str(int(pure_en(word))))
  features.append(str(int(pure_en_digit(word))))
  features.append(str(int(pure_punct(word))))
  features.append(str(int(pure_space(word))))
  features.append(word[0])
  features.append(word[-1])
  return features

# 得到中心词的特征
def get_char_features(name, x):
  char = name[x]
  features = list()
  features.append(min(x, 100))
  features.append(int(pure_digit(char)))
  features.append(int(pure_en(char)))
  features.append(int(pure_en_digit(char)))
  features.append(int(pure_punct(char)))
#  features.append(int(pure_space(char)))
  return features

# 得到当前词匹配词表的特征
def get_vocab_features(name, start, matcher, namespace, name_dim_map, feature_matrix):
  '''
  name : string
  '''
  end_list = matcher.match_all(name, start)
  for end, key in end_list:
    key = '%s_%s' % (namespace, key)
    dim = name_dim_map[key]
    for x in range(start, end):
      #print 'name=%s x=%s row=%s col=%s start=%s end=%s dim=%s' % (len(name), x, len(feature_matrix), len(feature_matrix[0]), start, end, dim)
      feature_matrix[x][dim] += 1
