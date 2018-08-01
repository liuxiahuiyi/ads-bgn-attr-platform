# -*- encoding:utf-8 -*-
import os
import sys
reload(sys)
sys.setdefaultencoding( "utf-8")
# import pandas as pd
# import numpy as np
class DataManager:
    def __init__(self,online_flag=False):
        if online_flag:
            blc_path = './program/blc/segment/basic_segmentation'
        else:
            blc_path = './blc/segment/basic_segmentation'
        sys.path.insert(0, blc_path)
        reload(sys)
        sys.setdefaultencoding("utf-8")
        from bgnseg import BgnSeg
        self.bgnseg = BgnSeg()        

    # title_contain_value_mode,if title contain tags value,1.delete it;2.save it; \
    #   3.remove the value from title 
    def process_train_data(self,orig_data_file,write_file_tr,write_file_val,dic_save_path,com_cd,index_second_cate, \
        title_contain_value_mode=1, save_others=False,only_get_1_tag=True,remove_multiple_tags=True, \
        index_remove_brand=False,train_model=True,tags_limit=0.01,val_radio=0.1):
        if os.path.exists(write_file_tr):
            print "train already exists!!!!! end get process step!!!!"
            return
        f = open(orig_data_file)
        flag = 0
        map_tags = {}
        fs = open(write_file_tr,'w')
        ft = open(write_file_val,'w')
        content_test = 'Sentiment,SentimentText\n'
        content = 'Sentiment,SentimentText\n'
        map_tags_appear_num = {}
        last_id = ''
        not_null_data = []
        read_lines = f.readlines()
        data_length = len(read_lines)
        for line in read_lines:
            flag += 1
            datablock = line.decode('utf8').strip('\n').split('\t')
            index_tags = datablock[6]
            if (last_id == datablock[0]) and only_get_1_tag:
                last_id = datablock[0]
                continue
            last_id = datablock[0]
            if (index_tags.find(u'其它') != -1 or index_tags.find(u'其他') != -1) and (not save_others):
                continue
            if (flag < data_length) and remove_multiple_tags:
                next_id = read_lines[flag].strip('\n').split('\t')[0]
                if next_id == datablock[0]:
                    continue
            not_null_data.append(line)
            if map_tags_appear_num.has_key(index_tags):
                map_tags_appear_num[index_tags] = map_tags_appear_num[index_tags] + 1
            else:
                map_tags_appear_num[index_tags] = 1
        # tags_limit,appear radio less than tags_limit removed
        # average,less than 
        not_null_data_length = len(not_null_data)
        index_tags_dict = {}
        index_tags_block = []
        for ii in map_tags_appear_num:
            index_apear_radio = map_tags_appear_num[ii] / float(not_null_data_length)
            if index_apear_radio < tags_limit:
                continue
            index_tags_block.append(ii)
            index_tags_dict[ii] = True
        print index_tags_block,index_tags_dict
        last_id = ''
        flag = 0
        map_id_value = {}
        for line in not_null_data:
            flag += 1
            stod_word_null = [',','"','季','【','】','）','（','*','#','2016','2017','2018']
            sto_word_blank = ['/','+','-',]
            index_line = line.decode('utf8').strip('\n')
            for ii in stod_word_null:
                index_line = index_line.replace(ii,'')
            for ii in sto_word_blank:
                index_line = index_line.replace(ii,' ')
            datablock = index_line.split('\t')
            index_third_cate = datablock[3]
            index_title = datablock[1]
            index_tags = datablock[6]
            index_tags_id = datablock[5]
            continue_flag = 0
            for ii in index_tags_block:
                if index_title.find(ii) != -1:
                    continue_flag = 1
                    break
            if continue_flag:
                if title_contain_value_mode == 1:
                    continue
                elif title_contain_value_mode == 3:
                    index_title = index_title.replace(ii,'')
            if (not index_tags_dict.has_key(index_tags)):
                continue
            if map_tags.has_key(index_tags):
                index_tags_res = map_tags[index_tags].split('_')[0]
            else:
                map_tags[index_tags] = str(len(map_tags)) + '_' + index_tags_id
                index_tags_res = str(len(map_tags) - 1)
            seg_list = self.bgnseg.cut(index_title)
            # seg_list_third = bgnseg.cut(index_third_cate)
            # index_third_cate = ''
            # for i in seg_list_third:
            #     index_third_cate += i + ' '

            # contentindex = index_tags_res + ',' + index_third_cate
            contentindex = index_tags_res + ','
            num_flag = 0
            content_index_w2v = ''
            flag_first = 0
            for i in seg_list:
                # i = i.decode("utf8", "ignore")

                if not flag_first and index_remove_brand:
                    flag_first = 1
                    continue
                if i == ' ':
                    continue
                contentindex += i + ' '
                num_flag += 1
                if num_flag == 25:
                    break
            contentindex += '\n'
            contentindex = contentindex.replace(' \n','\n')
            if not (flag % int(1 / val_radio)):
                content_test += contentindex
                # print "test",flag
            else:
                content += contentindex
            if not(flag % 10000):
                print flag
                fs.write(content)
                content = ''
                # print content_test
                ft.write(content_test)
                content_test = ''


        f.close()

        fs.write(content)
        fs.close()
        ft.write(content_test)
        ft.close()

        print map_tags
        for ii in map_tags:
            print map_tags[ii],",",ii
        self.save_dic(map_tags,dic_save_path,com_cd,index_second_cate)
        return map_tags
    # remove sku when title contain index_tags_block
    def process_test_data_all(self,index_second_cate,write_file,com_cd,index_tags_map):
        flag = 0
        map_tags = {}
        id_result = []
        index_tags_block = []
        for ii in index_tags_map:
            index_tags_block.append(index_tags_map[ii].split('_')[1])
        # print index_tags_block
        fs = open(write_file,'w')
        content = 'Sentiment,SentimentText\n'
        for line in sys.stdin:
            flag += 1
            stod_word_null = ['"','季','【','】','）','（','*','#','2016','2017','2018']
            sto_word_blank = ['/','+','-',]
            index_line = line.decode('utf8').strip('\n')
            if not line:
                continue
            datablock_orig = index_line.split(',')
            index_id_title = datablock_orig[0] + ',' + datablock_orig[6] + ',' +  datablock_orig[1].replace(' ','')
            for ii in stod_word_null:
                index_line = index_line.replace(ii.decode('utf8'),'')
            for ii in sto_word_blank:
                index_line = index_line.replace(ii.decode('utf8'),' ')
            datablock = index_line.split(',')
            index_third_cate = datablock[3]
            index_title = datablock[1]
            index_com_value = datablock[6]
            current_continue = self.data_filter_predict(com_cd,index_title, \
                index_tags_block,index_com_value,index_second_cate)
            if current_continue:
                continue
            seg_list = self.bgnseg.cut(index_title)
            contentindex = '1' + ','
            num_flag = 0
            flag_first = 0
            for i in seg_list:
                # i = i.decode("utf8", "ignore")
                #if not flag_first:
                #    flag_first = 1
                #    continue
                contentindex += i + ' '
                num_flag += 1
                if num_flag == 24:
                    break
            contentindex += '\n'
            contentindex = contentindex.replace(' \n','\n')
            content += contentindex
            id_result.append(index_id_title)
            if not(flag % 10000):
                #print flag
                fs.write(content)
                content = ''
        fs.write(content)
        fs.close()
        return id_result
    def data_filter_predict(self,com_cd,index_title,index_tags_block,index_com_value, \
        index_second_cate):
        continue_flag = 0
        for ii in index_tags_block:
            if index_title.find(ii.decode('utf8')) != -1:
                continue_flag = 1
                break
        if (index_second_cate == '1343') and (not continue_flag) and (com_cd == '2879'):
            if index_title.find(u'裤') != -1 and index_title.find(u'套装') == -1 and index_title.find(u'件套') == -1:
                continue_flag = 1

        if (index_second_cate == '1343') and (not continue_flag) and (com_cd == '2882'):
            if index_title.find(u'裤') == -1 and index_title.find(u'套装') == -1 and index_title.find(u'件套') == -1:
                continue_flag = 1
        if (index_second_cate == '1343') and (not continue_flag) and (com_cd == '2879') \
            and (index_com_value == u'其它'):
            continue_flag = 1
        return continue_flag

    def print_predict(self,predict_result,orig_data,map_tags,com_cd,index_second_cate,dt,map_com_attr,second_cate_name):
        flag = 0
        predict_data = predict_result.split('\n')
        for ii in predict_data:
            if flag == len(predict_data) - 1:
                break
            index_lable = int(ii.split(';')[1].split(',')[0])
            index_id_block = orig_data[flag].split(',')
            index_value = map_tags[str(index_lable)].decode('utf8').split('_')[1]
            index_value_id = map_tags[str(index_lable)].decode('utf8').split('_')[0]
            content = index_id_block[0] + ',' + index_value_id + ',' \
            + index_value  + ',' + index_id_block[1] + ',' + index_id_block[2] + ',' \
            + map_com_attr[com_cd] + ',' + second_cate_name + ',32,' + dt + ',' + index_second_cate \
            + ',' + com_cd + '\n'
            print content
            flag += 1
    def save_dic(self,map_tags,save_path,com_cd,index_second_cate):
        content = ""
        for ii in map_tags:
            # print map_tags[ii],",",ii
            content += index_second_cate + '_' + com_cd + '_' + map_tags[ii] + '_' + ii.decode('utf8') + '\n'
        w = file(save_path,'a')
        w.write(content)
        w.close()
    def get_dic(self,save_path,com_cd,index_second_cate):
        f = open(save_path)
        read_lines = f.readlines()
        map_result = {}
        for line in read_lines:
            datablock = line.decode('utf8').strip('\n').split('_')
            if (datablock[0] == index_second_cate) and (datablock[1] == com_cd):
                map_result[datablock[2]] = datablock[3] + '_' + datablock[4]
        f.close()

        return map_result


                