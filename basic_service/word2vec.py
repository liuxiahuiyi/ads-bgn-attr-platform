# -*- encoding:utf-8 -*-
import os
import sys
# import pandas as pd
# import numpy as np
class Word2Vec:
    def __init__(self,work_path,item_second_cate_cd,online_flag=False):
        self.work_path = work_path
        self.item_second_cate_cd = item_second_cate_cd
        if online_flag:
            blc_path = './program/blc/segment/basic_segmentation'
        else:
            blc_path = './blc/segment/basic_segmentation'
        sys.path.insert(0, blc_path)
        reload(sys)
        sys.setdefaultencoding("utf-8")
        from bgnseg import BgnSeg
        self.bgnseg = BgnSeg()
    def run_w2v(self,orig_file,vec_file,word_min_count):
        if os.path.exists(vec_file):
            print "orig_file already exists!!!!! end get w2v step!!!!"
            return
        # 1.process original data
        self.process_orig_file(orig_file,self.work_path + '/' + self.item_second_cate_cd \
            + '_w2v_segment.txt')
        # 2.word prepare
        os.system('./word2vec/word2vec -train ' + self.work_path + '/' + self.item_second_cate_cd \
            + '_w2v_segment.txt -save-vocab ' + self.work_path  + '/' + self.item_second_cate_cd \
            + '_pre_vocab.txt -min-count ' + str(word_min_count))
        # 3.replace title word with unkown
        self.replace_unk(self.work_path + '/' + self.item_second_cate_cd + '_pre_vocab.txt',self.work_path + '/' + self.item_second_cate_cd \
            + '_w2v_segment.txt', self.work_path + '/' + self.item_second_cate_cd + '_chars_for_w2v.txt')
        # 4. train model
        os.system('./word2vec/word2vec -train ' + self.work_path + '/'  + self.item_second_cate_cd \
            + '_chars_for_w2v.txt -output ' + vec_file + ' -size 100 -sample 1e-4 \
            -negative 5 -hs 1 -binary 0 -iter 10')
    def process_orig_file(self,orig_file,write_file):
        f = open(orig_file)
        flag = 0
        map_tags = {}
        fsv = open(write_file,'w')
        content_w2v = ''
        for line in f.readlines():
            flag += 1
            index_line = line.decode('utf8').strip('\n')
            index_title = index_line
            seg_list = self.bgnseg.cut(index_title)
            num_flag = 0
            content_index_w2v = ''
            flag_first = 0
            for i in seg_list:
                # i = i.decode("utf8", "ignore")
                content_index_w2v += i + ' '
            content_index_w2v += '\n'
            content_index_w2v = content_index_w2v.replace(' \n','\n')
            content_w2v += content_index_w2v
            if not(flag % 10000):
                print flag
                fsv.write(content_w2v)
                content_w2v = ''


        f.close()
        fsv.write(content_w2v)
        fsv.close()
    def replace_unk(self,pre_vocab,orig_segment,ouput):
        vp = open(pre_vocab, "r")
        inp = open(orig_segment, "r")
        oup = open(ouput, "w")
        vobsMap = {}
        for line in vp:
            line = line.strip()
            ss = line.split(" ")
            vobsMap[ss[0]] = 1
        while True:
            line = inp.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            ss = line.split(" ")
            tokens = []
            for s in ss:
                if s in vobsMap:
                    tokens.append(s)
                else:
                    tokens.append("<UNK>")
            oup.write("%s\n" % (" ".join(tokens)))
        oup.close()
        inp.close()
        vp.close()


                