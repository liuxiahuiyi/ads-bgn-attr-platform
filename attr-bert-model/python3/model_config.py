import tensorflow as tf
class TrainConfig:
  def __init__(self):
    self.vocab_size = 21128
    self.hidden_size = 768
    self.num_hidden_layers = 12
    self.num_attention_heads = 12
    self.hidden_act = 'gelu'
    self.intermediate_size = 3072
    self.hidden_dropout_prob = 0.1
    self.attention_probs_dropout_prob = 0.1
    self.max_position_embeddings = 512
    self.type_vocab_size = 2
    self.initializer_range = 0.02
    self.use_one_hot_embeddings = True
    self.output_dim = 1

    self.learning_rate = 5e-5
    self.batch_size = 128
    self.max_seq_length = 50
    self.training = True
    self.train_steps = 15000

    self.summary_save_steps = 100
    self.logging_steps = 50
    self.checkpoint_save_steps = 500
    

class InferConfig:
  def __init__(self):
    self.vocab_size = 21128
    self.hidden_size = 768
    self.num_hidden_layers = 12
    self.num_attention_heads = 12
    self.hidden_act = 'gelu'
    self.intermediate_size = 3072
    self.hidden_dropout_prob = 0.1
    self.attention_probs_dropout_prob = 0.1
    self.max_position_embeddings = 512
    self.type_vocab_size = 2
    self.initializer_range = 0.02
    self.use_one_hot_embeddings = True
    self.output_dim = 1

    self.learning_rate = 5e-5
    self.batch_size = 256
    self.max_seq_length = 50
    self.training = False
    self.train_steps = 15000
