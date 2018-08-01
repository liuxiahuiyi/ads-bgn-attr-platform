# coding: utf-8
import codecs
import jieba
#jieba.load_userdict('intermediate/user_dict.txt')

raw_concepts = list()
new_concepts = list()
raw_concept_probability = dict()
new_concept_probability = dict()
new_concept_set = set()

def init_data():
  with codecs.open('result/concepts_with_probability_pos_decay_filtered_2.txt', 'r', 'utf-8') as f:
    for line in f.readlines():
      s = line.strip().split(': ')
      raw_concept_probability[s[0]] = s[1]
      raw_concepts.append(s[0])

def remove_product_word(word):
  newword = list()
  word_seg = jieba.lcut(word)
  for w in word_seg:
    if w.endswith(u'手机'):
      w = w.strip(u'手机')
    if len(w) > 0:
      newword.append(w)
  return ''.join(newword)

def segment_if_contains(word):
  global new_concept_set
  word_seg = jieba.lcut_for_search(word, False)
#  print word
#  print 'left'
  while True:
#    print 'left loop'
    left_words = list()
#    print len(new_concept_set)
    for s in new_concept_set:
      if not word.startswith(s):
        continue
#      if (s != word) and (word.startswith(s)):
      contains_s = False
      for i in range(1, len(word_seg)+1):
        if s == ''.join(word_seg[:i]):
          contains_s = True
          break
#      if (s != word) and (s==word_seg[0] or s==''.join(word_seg[:2]) or s==''.join(word_seg[:3]) or s==''.join(word_seg[:4])):
      if (s != word) and contains_s:
        left_words.append(s)
    if len(left_words) > 0:
      max_left_len = 0
      max_left = ''
      for s in left_words:
        if len(s) > max_left_len:
          max_left = s
          max_left_len = len(s)
      if len(word.lstrip(max_left)) > 1:
        word = word.lstrip(max_left)
        new_concept_set.add(word)
      else:
        break
    else:
      break
#  print 'right'
  while True:
#    print 'right loop'
    left_words = list()
#    print len(new_concept_set)
    for s in new_concept_set:
      if not word.endswith(s):
        continue
#      if (s != word) and (word.startswith(s)):
      contains_s = False
      for i in range(1, len(word_seg)+1):
        if s == ''.join(word_seg[-i:]):
          contains_s = True
          break
#      if (s != word) and (s==word_seg[0] or s==''.join(word_seg[:2]) or s==''.join(word_seg[:3]) or s==''.join(word_seg[:4])):
      if (s != word) and contains_s:
        left_words.append(s)
    if len(left_words) > 0:
      max_left_len = 0
      max_left = ''
      for s in left_words:
        if len(s) > max_left_len:
          max_left = s
          max_left_len = len(s)
      if len(word.rstrip(max_left)) > 1:
        word = word.rstrip(max_left)
        new_concept_set.add(word)
      else:
        break
    else:
      break
  return word

#def remove_combination(concept_list):
#  origin_set = set(concept_list)
#  result_list = list()
#  for concept in concept_list:
#    s = set(jieba.lcut(concept))
#    if len(s) == 1:
#      result_list.append(concept)
#    elif not s < origin_set:
#      result_list.append(concept)
#    else:
#      
#      pass
#  return result_list


def main():
  global new_concept_set
  init_data()
  for c in raw_concepts:
    s = remove_product_word(c)
    if len(s) > 1:
      new_concepts.append(s)
      new_concept_probability[s] = raw_concept_probability[c]
#  result_concept = remove_combination(new_concepts)
  new_concept_set = set(new_concepts)
  final_concepts = list()
  final_concept_probability = dict()
#  while True:
  for i in range(1):
    old_concept_num = len(new_concept_set)
    for c in new_concepts:
      new_c = segment_if_contains(c)
      if len(new_c) > 1 and (not new_c in final_concepts):
        final_concepts.append(new_c)
        final_concept_probability[new_c] = new_concept_probability[c]
#        print c, new_c
    print 'one iteration'
    if len(new_concept_set) == old_concept_num:
      break
  result = list()
  for c in final_concepts:
    result.append((c, final_concept_probability[c]))
  sorted_result = sorted(result, key = lambda x: x[1], reverse=True)
  with codecs.open('result/concepts.txt', 'w', 'utf-8') as f:
    for x in sorted_result:
      f.write('%s: %s\n' % (x[0], x[1]))
  
if __name__ == '__main__':
  main()
