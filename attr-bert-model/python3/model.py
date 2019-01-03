from bert.modeling import BertModel
from bert.optimizer import create_optimizer
import tensorflow as tf

class Model():
  def __init__(self, config):
    self.config = config

  def _buildModel(self, input_ids, token_type_ids, input_mask):
    bert_model = BertModel(self.config,
                           self.config.training,
                           input_ids,
                           input_mask,
                           token_type_ids,
                           self.config.use_one_hot_embeddings)
    bert_output = bert_model.get_pooled_output()
    output = tf.layers.dense(bert_output,
                             self.config.output_dim,
                             kernel_initializer = tf.truncated_normal_initializer(stddev = self.config.initializer_range),
                             kernel_regularizer = tf.contrib.layers.l2_regularizer(1.0),
                             bias_regularizer = tf.contrib.layers.l2_regularizer(1.0),
                             name = 'output')
    return output

  def _getTotalLoss(self, input_ids, token_type_ids, input_mask, labels):
    logits = self._buildModel(input_ids, token_type_ids, input_mask)
    loss = tf.reduce_mean(tf.nn.sparse_softmax_cross_entropy_with_logits(labels = labels, logits = logits))
    accuracy = tf.reduce_mean(tf.cast(tf.nn.in_top_k(logits, labels, 1), tf.float32))
    tf.losses.add_loss(loss)
    return tf.losses.get_total_loss(), accuracy

  def getTrainOp(self, input_ids, token_type_ids, input_mask, labels):
    total_loss, accuracy = self._getTotalLoss(input_ids, token_type_ids, input_mask, labels)
    train_op, learning_rate = create_optimizer(total_loss, self.config.learning_rate,
                                               self.config.train_steps, int(0.1 * self.config.train_steps),
                                               False)
    return train_op, learning_rate, total_loss, accuracy

  def infer(self, input_ids, token_type_ids, input_mask):
    return tf.argmax(self._buildModel(input_ids, token_type_ids, input_mask), 1)

  def getGlobalStep(self):
    return tf.train.get_or_create_global_step()
    



