import tensorflow as tf
from model import Model
from train_data_reader import TrainDataReader
from model_config import TrainConfig
import os
import shutil
from tensorflow.python.client import device_lib

FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_string('category_dir', None, '')
tf.app.flags.DEFINE_string('bert_pretrain', None, '')
tf.app.flags.DEFINE_string('vocab_file', None, '')
def train(category_dir, bert_pretrain, vocab_file):
  local_device_protos = device_lib.list_local_devices()
  gpu_list = [d.name for d in local_device_protos if d.device_type == 'GPU']
  if len(gpu_list) > 0:
    print('use gpu:', gpu_list[0])
    os.environ['CUDA_VISIBLE_DEVICES'] = gpu_list[0].split(':')[-1]
  config = TrainConfig()
  with tf.Graph().as_default():
    train_data_reader = TrainDataReader(config, category_dir, vocab_file)
    output_dim = train_data_reader.transform()
    config.output_dim = output_dim
    labels, sequences, masks = train_data_reader.read()
    token_types = tf.get_variable('token_type',
                                   shape = [config.batch_size, config.max_seq_length],
                                   dtype = tf.int32,
                                   initializer = tf.zeros_initializer(),
                                   trainable = False)
    print('shape of sequences is:', sequences)

    model = Model(config)
    train_op, learning_rate, total_loss, accuracy = model.getTrainOp(sequences, token_types, masks, labels)
    summary_dir = os.path.join(category_dir, 'summary')
    checkpoint_dir = os.path.join(category_dir, 'checkpoint')
    if os.path.exists(summary_dir):
      shutil.rmtree(summary_dir)
    if os.path.exists(checkpoint_dir):
      shutil.rmtree(checkpoint_dir)

    _initFromCheckpoint(bert_pretrain)
 
    summary_hook = tf.train.SummarySaverHook(
      save_steps = config.summary_save_steps,
      output_dir = summary_dir,
      summary_op = tf.summary.merge_all())
    logging_hook = tf.train.LoggingTensorHook(
      tensors = {
        'step': model.getGlobalStep(),
        'learning_rate': learning_rate,
        'loss': total_loss,
        'accuracy': accuracy
      },
      every_n_iter = config.logging_steps)
    checkpoint_hook = tf.train.CheckpointSaverHook(
      checkpoint_dir,
      save_steps = config.checkpoint_save_steps,
      checkpoint_basename = 'attr-bert.ckpt',
      saver = tf.train.Saver(max_to_keep = 1))
    stop_hook = tf.train.StopAtStepHook(config.train_steps)
    print('start to training model!')

    with tf.train.MonitoredTrainingSession(
      hooks = [summary_hook, logging_hook, checkpoint_hook, stop_hook],
      save_checkpoint_steps = None,
      save_checkpoint_secs = None,
      save_summaries_steps = None,
      save_summaries_secs = None,
      config = tf.ConfigProto(log_device_placement = True, allow_soft_placement = True)) as sess:
      while not sess.should_stop():
        sess.run(train_op)


def _initFromCheckpoint(bert_pretrain):
  if not os.path.exists(bert_pretrain):
    raise Exception("local bert_pretrain not exists!!")
  bert_checkpoint = os.path.join(bert_pretrain, 'bert_model.ckpt')
  tvars = tf.trainable_variables()
  cvars = list(map(lambda x: x[0], tf.train.list_variables(bert_checkpoint)))
  assignment_map = {}
  for var in tvars:
    name = var.name.split(':')[0]
    if name in cvars:
      assignment_map[name] = name
  tf.train.init_from_checkpoint(bert_checkpoint, assignment_map)

def main(_):
  tf.logging.set_verbosity(tf.logging.INFO)
  train(FLAGS.category_dir, FLAGS.bert_pretrain, FLAGS.vocab_file)

if __name__ == '__main__':
  tf.app.run(main = main)




