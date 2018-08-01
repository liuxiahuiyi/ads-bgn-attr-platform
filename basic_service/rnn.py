# -*- coding:utf-8 -*-
import tensorflow as tf
class Model:
    def __init__(self,hidden_size,maxSeqLen,numTags,dropout_prob):
        self.hidden_size = hidden_size
        self.num_tags = numTags
        self.seq_len = maxSeqLen
        self.dropout_keep_prob = dropout_prob
        self.w = tf.get_variable(
            shape=[self.hidden_size[-1], self.num_tags],
            initializer=tf.contrib.layers.xavier_initializer(),
            name="weights",
            regularizer=tf.contrib.layers.l2_regularizer(0.001))
        self.b = tf.Variable(tf.zeros([self.num_tags], name="bias"))
        # self.input = tf.placeholder(tf.int32, [None, max_length], name='input')
        # self.target = tf.placeholder(tf.float32, [None, num_tags], name='target')
    # main function,train and predict use this function.
    def scores1(self, embedded_words,sequence_length, train_model=True, reuse=False, random_state=None):
        if train_model:
            index_dropout = self.dropout_keep_prob
        else:
            index_dropout = 1.0
        with tf.variable_scope("rnn", reuse=reuse):
            outputs = embedded_words
            for h in self.hidden_size:
                outputs = self.__rnn_layer(h, outputs, sequence_length, index_dropout,reuse=reuse)
            # lstm_cell = self.__cell(self.hidden_size[0], index_dropout, reuse, random_state)
            # outputs, _ = tf.nn.dynamic_rnn(lstm_cell, outputs, dtype=tf.float32, sequence_length=sequence_length)
        outputs = tf.reduce_mean(outputs, reduction_indices=[1])

        scores = tf.nn.xw_plus_b(outputs, self.w, self.b, name='scores')
            # tf.summary.histogram('final_layer/wx_plus_b', scores)
        return scores
    def __rnn_layer(self, hidden_size, x, seq_len, dropout_keep_prob, variable_scope=None, random_state=None, reuse=False):
        """
        Builds a LSTM layer
        :param hidden_size: Number of units in the LSTM cell
        :param x: Input with shape [batch_size, max_length]
        :param seq_len: Sequence length tensor with shape [batch_size]
        :param dropout_keep_prob: Tensor holding the dropout keep probability
        :param variable_scope: Optional. Name of variable scope. Default is 'rnn_layer'
        :param random_state: Optional. Random state for the dropout wrapper
        :return: outputs with shape [batch_size, max_seq_len, hidden_size]
        """
        with tf.variable_scope(variable_scope, default_name='rnn_layer'):
            # Build LSTM cell
        	lstm_cell = self.__cell(hidden_size, dropout_keep_prob, reuse, random_state)

            # Dynamically unroll LSTM cells according to seq_len. From TensorFlow documentation:
            # "The parameter `sequence_length` is used to copy-through state and zero-out outputs when past a batch
            # element's sequence length."
            # print ('xxxx',x.get_shape())
        	outputs, _ = tf.nn.dynamic_rnn(lstm_cell, x, dtype=tf.float32, sequence_length=seq_len)
        return outputs
    def __cell(self, hidden_size, dropout_keep_prob, reuse, seed=None):
        """
        Builds a LSTM cell with a dropout wrapper
        :param hidden_size: Number of units in the LSTM cell
        :param dropout_keep_prob: Tensor holding the dropout keep probability
        :param seed: Optional. Random state for the dropout wrapper
        :return: LSTM cell with a dropout wrapper
        """
        # lstm_cell = tf.nn.rnn_cell.LSTMCell(hidden_size, state_is_tuple=True, reuse=reuse)
        lstm_cell = tf.contrib.rnn.LSTMCell(hidden_size,reuse=reuse)
        dropout_cell = tf.nn.rnn_cell.DropoutWrapper(lstm_cell, input_keep_prob=dropout_keep_prob,
                                                     output_keep_prob=dropout_keep_prob, seed=seed)
        return dropout_cell
    # main function,train and predict use this function.
    def scores(self, X, length, train_model=True, reuse=False):
    	print ("train_model, reuse",train_model, reuse)
        length_64 = tf.cast(length, tf.int64)
        with tf.variable_scope("bilstm", reuse=reuse):
            forward_output, _ = tf.nn.dynamic_rnn(
                tf.contrib.rnn.LSTMCell(self.hidden_size[0],
                                        reuse=reuse),
                X,
                dtype=tf.float32,
                sequence_length=length,
                scope="RNN_forward")
        outputs = tf.reduce_mean(forward_output, reduction_indices=[1])
        if reuse is None or not reuse:
            outputs = tf.nn.dropout(outputs, 0.5)
        matricized_unary_scores = tf.matmul(outputs, self.w) + self.b
        return matricized_unary_scores
    def scores2(self, X, length, train_model=True,reuse=False):
        length_64 = tf.cast(length, tf.int64)
        with tf.variable_scope("bilstm", reuse=reuse):
            forward_output, _ = tf.nn.dynamic_rnn(
                tf.contrib.rnn.LSTMCell(self.hidden_size[0],
                                        reuse=reuse),
                X,
                dtype=tf.float32,
                sequence_length=length,
                scope="RNN_forward")
            backward_output_, _ = tf.nn.dynamic_rnn(
                tf.contrib.rnn.LSTMCell(self.num_hidden,
                                        reuse=reuse),
                inputs=tf.reverse_sequence(X,
                                           length_64,
                                           seq_dim=1),
                dtype=tf.float32,
                sequence_length=length,
                scope="RNN_backword")

        backward_output = tf.reverse_sequence(backward_output_,
                                              length_64,
                                              seq_dim=1)

        output = tf.concat([forward_output, backward_output], 2)
        output = tf.reshape(output, [-1, self.num_hidden * 2])
        if reuse is None or not reuse:
            output = tf.nn.dropout(output, 0.5)

        matricized_unary_scores = tf.matmul(output, self.W) + self.b
        unary_scores = tf.reshape(
            matricized_unary_scores,
            [-1, self.max_seq_len, self.num_tags],
            name="Reshape_7" if reuse else None)
        return unary_scores
