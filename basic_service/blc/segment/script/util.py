#coding=utf8
import codecs

punct_set = u''':!),:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…*/'''
punct_set = set(punct_set)

def load_vocab(vocab_file, do_filter=True):
  f = codecs.open(vocab_file, 'r', 'utf8')
  vocab_set = set()
  for line in f:
    items = line.strip().split('\t')
    if len(items) == 0:
      continue
    words = set(items[0])
    if len(words) == len(punct_set & words):
      continue
    vocab_set.add(items[0])
  return vocab_set

def load_instances(bio_file):
  instances = list()
  truncks = list()
  f = codecs.open(bio_file, 'r', 'utf8')
  for line in f:
    trunck = line.strip()
    if trunck == '':
      if len(truncks) != 0:
        instances.append(truncks)
      truncks = list()
    else: 
      truncks.append(trunck)
  if len(truncks) != 0:
    instances.append(truncks)
  return instances

class BlockInfo(object):
  
  def __init__(self, pos, label, length, word, valid):
    self.pos = pos
    self.label = label
    self.length = length
    self.word = word
    self.valid = valid

def print_blocks(blocks):
  for block in blocks:
    print 'word=%s label=%s pos=%s length=%s valid=%s' % (block.word, block.label, block.pos, block.length, block.valid)
  print 

def do_load_label_blocks(trunck_insts, convert_O):
  FLAG_CONTINUE = 1
  FLAG_DONE = 2
  
  ret_blocks = list()
  block_trunck = ''
  block_label = None
  block_pos = 0
  flag = FLAG_CONTINUE

  pos = 0
  while pos < len(trunck_insts):
    word, label = trunck_insts[pos]
    if flag == FLAG_CONTINUE:
      # 初始化阶段
      if block_label is None:
        block_pos = pos
        block_trunck = word
        block_label = label
        pos += 1
        continue

      # 普通阶段
      # 继续走
      if (label[0] == 'I' and label[2:] == block_label[2:]) or \
         (label[0] == 'O' and label[2:] == block_label[2:]) :
         pos += 1
         block_trunck += word
      # 其他非法状态则结束游走
      else :
        flag = FLAG_DONE
    elif flag == FLAG_DONE:
      valid = block_label[0] in set(['B', 'O'])
      if convert_O:
        ret_blocks.append(BlockInfo(block_pos, block_label[2:] or 'UNK', pos - block_pos, block_trunck, valid))
      else:
        ret_blocks.append(BlockInfo(block_pos, block_label[2:], pos - block_pos, block_trunck, valid))
      block_label = None
      flag = FLAG_CONTINUE
    else:
      raise Exception("unknonw flag")

  if block_label is not None:
    valid = block_label[0] in set(['B', 'O'])
    if convert_O:
      ret_blocks.append(BlockInfo(block_pos, block_label[2:] or 'UNK', pos - block_pos, block_trunck, valid))
    else:
      ret_blocks.append(BlockInfo(block_pos, block_label[2:], pos - block_pos, block_trunck, valid))
  return ret_blocks

# 返回BlockInfo表征整个句子的切分
def load_label_blocks(instance, label_type, convert_O=True):
  # 解析instance
  trunck_insts = list()
  for line in instance:
    try:
        items = line.strip().split('\t')
        word, ground, pred = items[0], items[-2], items[-1]
        label = ground if label_type == 'ground' else pred
        trunck_insts.append((word, label))
    except :
        print 'invalid line=%s' % line
  
  ret_blocks = do_load_label_blocks(trunck_insts, convert_O)

  return ret_blocks

if __name__ == '__main__':
  import sys
  instances = load_instances(sys.argv[1])
  for idx, instance in enumerate(instances):
    label_blocks = load_label_blocks(instance, 'pred')
    print_blocks(label_blocks)
    #if idx >= 10:
    #  break

