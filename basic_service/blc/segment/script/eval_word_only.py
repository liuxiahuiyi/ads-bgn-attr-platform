#coding=utf8
import sys
sys.path.append('./')
from data import load_instances

class Evaluater(object):
  
  def __init__(self):
    self.total = 0
    self.right = 0

  def add_right(self, n=1):
    self.total += n
    self.right += n

  def add_wrong(self, n=1):
    self.total += n

def eval(bio_file):
  char_eval = Evaluater()
  vocab_eval = Evaluater()

  instances = load_instances(bio_file)
  for inst in instances:
    prev_label = ''
    wrong_cn = 0
    for trunck in inst:
      items = trunck.strip().split('\t')
      ground, pred = items[-2], items[-1]
      if ground == '' or pred == '' or ground[0] == 'O':
        continue
      if ground[0] == pred[0]:
        char_eval.add_right(1)
      else:
        wrong_cn += 1
        char_eval.add_wrong(1)

      if ground[0] == 'B' or ground[0] == 'O':
        if prev_label != '':
          if wrong_cn == 0:
            vocab_eval.add_right(1)
          else:
            vocab_eval.add_wrong(1)
        wrong_cn = 0
      prev_label = ground
    # 处理最后一个
    if prev_label != '':
      if wrong_cn == 0:
        vocab_eval.add_right(1)
      else:
        vocab_eval.add_wrong(1)

  return char_eval, vocab_eval

if __name__ == '__main__':
  pred_file = sys.argv[1]
  char_eval, vocab_eval = eval(pred_file)
  char_acc = char_eval.right / float(char_eval.total + 1)
  vocab_acc = vocab_eval.right / float(vocab_eval.total + 1)
  print 'char_acc=%s right=%s total=%s' % (char_acc, char_eval.right, char_eval.total)
  print 'vocab_acc=%s right=%s total=%s' % (vocab_acc, vocab_eval.right, vocab_eval.total)
