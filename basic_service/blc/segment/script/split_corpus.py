#coding=utf8
import sys
import codecs
import json
import random

# 随机选择测试集，
# 这样做的目的是保证多次试验在同一个测试集上进行
# 保证测试的稳定性
def select_seed_skuid(note_file, test_skuid_file):
  ratio = 0.2
  f = open(note_file, 'r')
  instances = f.readlines()
  f.close()

  instances = [inst.strip().split('\t') for inst in instances]
  total_index = range(len(instances))
  test_index = random.sample(total_index, int(len(total_index) * ratio))
  ftest_skuid = open(test_skuid_file, 'w')
  for i, inst in enumerate(instances):
    if len(inst) < 4 or i not in test_index:
      continue
    ftest_skuid.write("%s\n" % inst[1])

def select_corpus(note_file, outfile, skuid_file, include):
  f = open(skuid_file, 'r')
  test_skuids = set([skuid.strip() for skuid in f.readlines()])
  f.close()

  fnote = open(note_file, 'r')
  fout = open(outfile, 'w')
  for line in fnote:
    items = line.strip().split('\t')
    if len(items) < 4:
      continue
    if include and (items[1] in test_skuids):
        fout.write(line)
    if not include and items[1] not in test_skuids:
        fout.write(line)

# 切分数据集
if __name__ == '__main__':
  cmd = sys.argv[1]
  if cmd == 'seed':
    note_file = sys.argv[2]
    skuid_file = sys.argv[3]
    select_seed_skuid(note_file, skuid_file)
  elif cmd == 'include':
    note_file = sys.argv[2]
    extract_file = sys.argv[3]
    skuid_file = sys.argv[4]
    select_corpus(note_file, extract_file, skuid_file, True)
  elif cmd == 'exclude':
    note_file = sys.argv[2]
    extract_file = sys.argv[3]
    skuid_file = sys.argv[4]
    select_corpus(note_file, extract_file, skuid_file, False)
  else:
    raise Exception("split_corpus unkonw cmd=%s" % cmd)
