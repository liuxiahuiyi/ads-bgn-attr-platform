# coding: utf-8
from __future__ import absolute_import, unicode_literals
import jieba
import re
from jieba._compat import *
from six.moves import cPickle
from math import log, sqrt
import codecs
import os

cur_path = os.path.split(os.path.realpath(__file__))[0]

USER_DICT_FILE = os.path.join(cur_path, 'user_dict.txt')
PRODUCT_WORDS_FILE = os.path.join(cur_path, 'product_words.txt')
WORD_PROB_FILE = os.path.join(cur_path, 'word_prob.pkl')

class BgnSeg(object):
  # 初始化自定义词典、产品词列表、词的unigram概率值
  def __init__(self, user_dict_file = USER_DICT_FILE, 
               product_words_file = PRODUCT_WORDS_FILE, 
               word_prob_file = WORD_PROB_FILE):
    self.re_han_cut_all = re.compile("([\u4E00-\u9FD5]+)", re.U)
    
    self.dt = jieba.Tokenizer()
    self.dt.load_userdict(user_dict_file)
    
    self.pn_set = set()
#    with codecs.open(product_words_file, 'r', 'utf-8') as f:
    with codecs.open(user_dict_file, 'r', 'utf-8') as f:
      for line in f:
        self.pn_set.add(line.strip())
    
    with open(word_prob_file, 'r') as f:
      self.word_prob = cPickle.load(f)

  # 获取当前语句的所有切词方案（DAG的全部路径）
  def __get_path(self, sentence, dag, cur_path, cur_idx, all_paths):
    if cur_idx >= len(sentence):
      all_paths.append(cur_path)
      return
    for x in dag[cur_idx]:
      word = sentence[cur_idx : x+1]
      cur_path_copy = cur_path[:]
      cur_path_copy.append(word)
      self.__get_path(sentence, dag, cur_path_copy, x+1, all_paths)

  # 按最近邻原则合并相邻词，避免产品词被切分
  def __combine_pn(self, words):
    newlist = list()
    start_idx = 0
    while start_idx < len(words):
      if start_idx + 1 < len(words):
        new_gram = words[start_idx] + words[start_idx+1]
      else:
        new_gram = words[start_idx]
      if new_gram.lower() in self.pn_set:
        i = 2
        while start_idx+i < len(words):
          if (new_gram + words[start_idx+i]).lower() in self.pn_set:
            new_gram += words[start_idx+i]
            i += 1
          else:
            break
        newlist.append(new_gram)
        start_idx += i
      else:
        newlist.append(words[start_idx])
        start_idx += 1
    return newlist


  # 判断两个切词方案中的两个词是否有交叉，词用在句子中的位置表示
  # 0 means a in b
  # 1 means b in a
  # 2 means a cross b
  # 3 means a and b has no common part
  # 4 means a and b are equal
  def __check_cross(self, a, b):
    aa = set(range(a[0], a[1]+1))
    bb = set(range(b[0], b[1]+1))
    if len(aa&bb) > 0:
      if aa < bb:
        return 0
      elif aa > bb:
        return 1
      elif aa == bb:
        return 4
      else:
        return 2
    else:
      return 3

  # 针对只有组合歧义、没有交叉歧义的part，获取最长词
  def __get_combine_words(self, dag):
    all_set = set()
    all_set_after_delete = set()
    deleted_set = set()
    for k in dag:
      L = dag[k]
      for j in L:
        if j > k:
          all_set.add((k, j))
          all_set_after_delete.add((k, j))
    for a in all_set:
      for b in all_set_after_delete:
        result = self.__check_cross(a, b)
        if result == 0:
          deleted_set.add(a)
        elif result == 1:
          deleted_set.add(b)
        elif result == 2:
          deleted_set.add(a)
          deleted_set.add(b)
        else:
          pass
      all_set_after_delete -= deleted_set
      deleted_set.clear()
    return all_set_after_delete

  # 基于组合歧义的情形，优化DAG
  def __refine_dag(self, dag):
    combine_word_set = self.__get_combine_words(dag)
    for k in dag:
      cur_longest = (k, dag[k][-1])
      if cur_longest in combine_word_set:
        dag[k] = [cur_longest[1]]


  # 基础功能，获取词的unigram概率
  def __get_word_prob(self, word):
    if word in self.word_prob:
      return self.word_prob[word]
    else:
      return self.word_prob[u'_BGN_UNK']

  # 计算给定切词方案的概率
  def __get_segmentation_prob(self, words):
    total_sum = 0
    total_count = 0
    for word in words:
      total_sum += log(self.__get_word_prob(word))
      total_count += 1
  #  return total_sum / float(total_count)
    return total_sum / sqrt(total_count)

  # 对中文语句，获取最优切词方案，返回切词list
#  def __cut_chinese(self, sentence):
#    dag = self.dt.get_DAG(sentence.lower())
#    self.__refine_dag(dag)
#    all_paths = list()
#    self.__get_path(sentence, dag, [], 0, all_paths)
#    
#    newlist = list()
#    for x in all_paths:
#      newlist.append((x, self.__get_segmentation_prob(x)))
#    newlist.sort(key = lambda x: x[1], reverse=True)
#  
#    return self.__combine_pn(newlist[0][0])


  def calc(self, sentence, DAG, route):
    N = len(sentence)
    # route: (score, idx, n)
    route[N] = (0, 0, 0)
    for idx in xrange(N - 1, -1, -1):
      route[idx] = max(((log(self.__get_word_prob(sentence[idx:x + 1])) +
                        route[x+1][0]*sqrt(route[x+1][2]))/sqrt(route[x+1][2]+1), x, route[x+1][2]+1) for x in DAG[idx])

  def __cut_chinese(self, sentence):
    dag = self.dt.get_DAG(sentence.lower())
    self.__refine_dag(dag)
    route = {}
    self.calc(sentence, dag, route)

    word_list = list()

    x = 0
    N = len(sentence)
    while x < N:
      y = route[x][1] + 1
      l_word = sentence[x:y]
      word_list.append(l_word)
      x = y
  
    return self.__combine_pn(word_list)

  def __combine_single_char(self, path):
    result_path = list()
    mid_path = list()
    for x in path:
      if len(x) > 1:
        if len(mid_path) > 1:
          result_path.extend(self.dt.lcut(''.join(mid_path)))
          mid_path[:] = []
        elif len(mid_path) > 0:
          result_path.extend(mid_path)
          mid_path[:] = []
        else:
          pass
        result_path.append(x)
      else:
        mid_path.append(x)
    if len(mid_path) > 1:
      result_path.extend(self.dt.lcut(''.join(mid_path)))
      mid_path[:] = []
    elif len(mid_path) > 0:
      result_path.extend(mid_path)
      mid_path[:] = []
    else:
      pass
    return result_path

  # 对任意语句，获取最优切词方案（概率最大），将句子中的中英文分割开分别处理，返回切词list
  # 这是该类唯一对外的接口
  def cut(self, sentence):
    sentence = strdecode(sentence)
  
    re_han = self.re_han_cut_all
    
    cut_block = self.__cut_chinese
    blocks = re_han.split(sentence)
    words = list()
    for blk in blocks:
      if not blk:
        continue
      if re_han.match(blk):
        words.extend(cut_block(blk))
      else:
        words.extend(self.dt.lcut(blk))
    return self.__combine_single_char(self.__combine_pn(words))
  

