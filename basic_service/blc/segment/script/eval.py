#coding=utf8
import sys
import config
from collections import defaultdict
from util import load_instances, load_label_blocks, print_blocks

class Evaluater(object):
  
  def __init__(self):
    self.total = 0
    self.right = 0

  def add_right(self, n=1):
    self.total += n
    self.right += n

  def add_wrong(self, n=1):
    self.total += n

class EvaluaterF1(object):
  
  def __init__(self):
    self.pred_total = 0
    self.ground_total = 0
    self.right = 0
  
  def add_pred(self, pred_cnt):
    self.pred_total += pred_cnt

  def add_ground(self, ground_cnt):
    self.ground_total += ground_cnt

  def add_right(self, n=1):
    self.right += n

def eval_char_level(instances):
  char_eval = Evaluater()
  for inst in instances:
    for trunck in inst:
      items = trunck.strip().split('\t')
      ground, pred = items[-2], items[-1]
      if ground == '' or pred == '':
        continue
      if ground == pred:
        char_eval.add_right(1)
      else:
        char_eval.add_wrong(1)
  return char_eval 

def word_info_equal(word_info1, word_info2):
  return ((word_info1.pos == word_info2.pos) and \
          (word_info1.length == word_info2.length) and \
          (word_info1.label == word_info2.label))

def eval_word_level(instances):
  word_eval_dict = defaultdict(EvaluaterF1)
  unknow_label = 'UNK'
  for inst in instances:
    # 过滤掉UNK的
    ground_label_index = load_label_blocks(inst, 'ground')
    ground_label_index = [word_info for word_info in ground_label_index if (word_info.label in config.g_eval_labels)]
    ground_label_index = {word_info.pos:word_info for word_info in ground_label_index}

    #print_blocks(ground_label_index)

    pred_label_index = load_label_blocks(inst, 'pred')
    pred_label_index = [word_info for word_info in pred_label_index if (word_info.label in config.g_eval_labels)]
    pred_label_index = {word_info.pos:word_info for word_info in pred_label_index}

    #print_blocks(pred_label_index)
    
    for pos in ground_label_index:
      word_info = ground_label_index[pos]
      word_eval_dict[word_info.label].add_ground(1)
    for pos in pred_label_index:
      word_info = pred_label_index[pos]
      word_eval_dict[word_info.label].add_pred(1)
    for pos in ground_label_index:
      if pos in pred_label_index and word_info_equal(ground_label_index[pos], pred_label_index[pos]):
        word_eval_dict[ground_label_index[pos].label].add_right(1)
  return word_eval_dict

def eval_total(bio_file):
  instances = load_instances(bio_file)
  #char_eval = eval_char_level(instances)
  word_eval = eval_word_level(instances)
  #return char_eval, word_eval
  return word_eval

if __name__ == '__main__':
  pred_file = sys.argv[1]

  # label level evaluate
  word_eval_dict = eval_total(pred_file)
  for label in word_eval_dict:
    word_eval = word_eval_dict[label]
    acc = word_eval.right / float(word_eval.pred_total + 1)
    recall = word_eval.right / float(word_eval.ground_total + 1)
    f1 = 2 * acc * recall / (acc + recall)
    print 'label=%s\tacc=%s\trecall=%s\tf1=%s\tground=%s\tpred=%s\tright=%s' % (label, acc, recall, f1, word_eval.ground_total, word_eval.pred_total, word_eval.right)

  # total evaluate
  total_eval = EvaluaterF1()
  for label in word_eval_dict:
    total_eval.add_ground(word_eval_dict[label].ground_total)
    total_eval.add_pred(word_eval_dict[label].pred_total)
    total_eval.add_right(word_eval_dict[label].right)
  acc = total_eval.right / float(total_eval.pred_total + 1)
  recall = total_eval.right / float(total_eval.ground_total + 1)
  f1 = 2 * acc * recall / (acc + recall)
  print 'label=total\tacc=%s\trecall=%s\tf1=%s\tground=%s\tpred=%s\tright=%s' % (acc, recall, f1, total_eval.ground_total, total_eval.pred_total, total_eval.right)
