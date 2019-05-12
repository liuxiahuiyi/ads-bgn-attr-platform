import tensorflow as tf
from model import Model
from model_config import InferConfig
from bert.tokenization import FullTokenizer
import pickle
import os

class Inferer:
  def __init__(self, checkpoint, attr_values_file, vocab_file):
    self.checkpoint = checkpoint
    self.attr_values_file = attr_values_file
    self.vocab_file = vocab_file
    if not os.path.exists(self.checkpoint):
      raise Exception("local checkpoint %s not exists" % self.checkpoint)
    if not os.path.exists(self.attr_values_file):
      raise Exception("local attr_values_file %s not exists" % self.attr_values_file)
    if not os.path.exists(self.vocab_file):
      raise Exception("local vocab_file %s not exists" % self.vocab_file)
    self.config = InferConfig()
    self.tokenizer = FullTokenizer(self.vocab_file)
    with open(self.attr_values_file, 'rb') as fr:
      attr_values, attr_values_r = pickle.load(fr)
    self.attr_values_r = attr_values_r
    self.config.output_dim = len(attr_values_r)

    self.graph = tf.Graph()
    with self.graph.as_default():
      self.input_ids_p = tf.placeholder(tf.int32, [None, self.config.max_seq_length])
      self.token_type_ids_p = tf.placeholder(tf.int32, [None, self.config.max_seq_length])
      self.input_mask_p = tf.placeholder(tf.int32, [None, self.config.max_seq_length])
      model = Model(self.config)
      self.inference = model.infer(self.input_ids_p, self.token_type_ids_p, self.input_mask_p)
      ckpt_state = tf.train.get_checkpoint_state(self.checkpoint)
      if not (ckpt_state and ckpt_state.model_checkpoint_path):
        raise Exception('No model to eval yet at: ' + self.checkpoint)
      self.sess = tf.Session(config = tf.ConfigProto(allow_soft_placement = True))
      saver = tf.train.Saver()
      saver.restore(self.sess, ckpt_state.model_checkpoint_path)

  def infer(self, sequences):
    transforms = [self._transform(s) for s in sequences if s != '']
    input_ids, token_type_ids, input_mask = list(map(lambda x: list(x), zip(*transforms)))
    with self.graph.as_default():
      result = self.sess.run(self.inference, feed_dict = {
                                                           self.input_ids_p: input_ids,
                                                           self.token_type_ids_p: token_type_ids,
                                                           self.input_mask_p: input_mask
                                                         })
    return [self.attr_values_r[e] for e in result]

  def _transform(self, sequence):
    tokens = self.tokenizer.tokenize(sequence)
    if len(tokens) > self.config.max_seq_length - 2:
      tokens = tokens[0:self.config.max_seq_length - 2]
    tokens = ['[CLS]'] + tokens + ['[SEP]']
    token_ids = self.tokenizer.convert_tokens_to_ids(tokens)

    input_ids_1 = token_ids[0:self.config.max_seq_length] + [0] * (self.config.max_seq_length - len(token_ids))
    token_type_ids_1 = [0] * self.config.max_seq_length
    input_mask_1 = [1] * len(token_ids) + [0] * (self.config.max_seq_length - len(token_ids))
    return input_ids_1, token_type_ids_1, input_mask_1
