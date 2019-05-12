from model_config import InferConfig
from infer import Inferer
from optparse import OptionParser
import sys

batch_size = InferConfig().batch_size
parser = OptionParser()
parser.add_option('', '--checkpoint')
parser.add_option('', '--attr_values_file')
parser.add_option('', '--vocab_file')
parser.add_option('', '--source')
(options, args) = parser.parse_args()

def mapper():
  batch = []
  inferer = Inferer(options.checkpoint, options.attr_values_file, options.vocab_file)
  for line in sys.stdin:
    try:
      if line.strip() == '' or len(line.strip().split('\t')) != 9:
        continue
      segment = line.strip().split('\t')
      if segment[4].strip() == '':
        continue
      batch.append(segment)
      if len(batch) < batch_size:
        continue
      sequences = [e[4] for e in batch]
      inferences = inferer.infer(sequences)
      for i, e in enumerate(batch):
        print('\t'.join(e[1:5] + e[6:9] + list(inferences[i]) + [options.source]))
      batch.clear()
    except:
      pass

  sequences = [e[4] for e in batch]
  inferences = inferer.infer(sequences)
  for i, e in enumerate(batch):
    print('\t'.join(e[1:5] + e[6:9] + list(inferences[i]) + [options.source]))
  batch.clear()

if __name__ == '__main__':
  mapper()


