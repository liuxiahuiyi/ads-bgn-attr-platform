from bert.tokenization import FullTokenizer
import tensorflow as tf
import os
import pickle

class TrainDataReader():
  def __init__(self, config, category_dir, vocab_file):
    self.config = config
    self.category_dir = category_dir
    self.tokenizer = FullTokenizer(vocab_file)
    if not os.path.exists(os.path.join(self.category_dir, 'train_data', 'raw.csv')):
      raise Exception("local raw train data not exists!!")
    if not os.path.exists(vocab_file):
      raise Exception("local vocab_file not exists")
  def transform(self):
    with open(os.path.join(self.category_dir, 'train_data', 'raw.csv')) as fr, \
         open(os.path.join(self.category_dir, 'attr_values.pkl'), 'wb') as fwa:
      attr_values_c = {}
      for row in fr:
        if row.strip() == '' or len(row.strip().split('\t')) != 10:
          continue
        segment = row.strip().split('\t')
        attr_values_c[(segment[8], segment[9])] = 1
      attr_values = {k:i for i, k in enumerate(attr_values_c.keys())}
      attr_values_r = {i:k for k, i in attr_values.items()}
      print('start to write local attr_values.pkl!!')
      pickle.dump((attr_values, attr_values_r), fwa)
    with open(os.path.join(self.category_dir, 'train_data', 'raw.csv')) as fr, \
         open(os.path.join(self.category_dir, 'train_data', 'transform.csv'), 'w') as fwt:
      print('start to write local train_data transform.csv!!')
      for row in fr:
        if row.strip() == '' or len(row.strip().split('\t')) != 10:
          continue
        segment = row.strip().split('\t')
        label = attr_values[(segment[8], segment[9])]
        tokens = self.tokenizer.tokenize(segment[7])
        if len(tokens) > self.config.max_seq_length - 2:
          tokens = tokens[0:self.config.max_seq_length - 2]
        tokens = ['[CLS]'] + tokens + ['[SEP]']
        token_ids = self.tokenizer.convert_tokens_to_ids(tokens)
        token_ids_patch = token_ids[0:self.config.max_seq_length] + [0] * (self.config.max_seq_length - len(token_ids))
        token_ids_patch = list(map(lambda x: str(x), token_ids_patch))
        fwt.write(str(label) + ',' + str(min(len(token_ids), len(token_ids_patch))) + ',' + ','.join(token_ids_patch) + '\n')
    return len(attr_values)

  def read(self):
    transform = os.path.join(self.category_dir, 'train_data', 'transform.csv')
    queue = tf.train.string_input_producer([transform])
    reader = tf.TextLineReader()
    _, value = reader.read(queue)
    row = tf.decode_csv(value, [[0]] * (self.config.max_seq_length + 2))
    label = tf.stack(row[0])
    length = tf.stack(row[1])
    mask = tf.cast(tf.sequence_mask(length, self.config.max_seq_length), tf.int32)
    sequence = tf.stack(row[2:self.config.max_seq_length + 2])
    return tf.train.shuffle_batch([label, sequence, mask], self.config.batch_size, 50000, 10000)


        
