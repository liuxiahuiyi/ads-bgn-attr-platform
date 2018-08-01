#coding=utf8
import sys
import codecs
sys.path.append('./')
from util import load_instances, load_label_blocks

def get_sentence(instance):
  sent = ''
  for line in instance:
    sent += line.strip().split('\t')[0]
  return sent

def get_trunck_with_label(instance, label_type):
  trunck_insts = load_label_blocks(instance, label_type)
  string = ''
  for block in trunck_insts:
    s = block.word + '/' + block.label + ' '
    string += s
  return string.strip()

def view(bio_file, outfile):
  fout = codecs.open(outfile, 'w', 'utf8')
  instances = load_instances(bio_file)
  for inst in instances:
    sent = get_sentence(inst)
    y_sent = get_trunck_with_label(inst, 'ground')
    p_sent = get_trunck_with_label(inst, 'pred')
    s = '%s\n%s\n%s\n\n' % (sent, y_sent, p_sent)
    fout.write(s)

if __name__ == '__main__':
  bio_file = sys.argv[1]
  outfile = sys.argv[2]
  view(bio_file, outfile)

