#coding=utf8

import sys
import codecs

key_name_map = {u'产品':u'产品词',
                u'产品名':u'产品词',
                u'功能特点':u'功能使用',
                u'包装形式':u'包装方式',
                u'品牌':u'品牌词',
                u'品牌名':u'品牌词',
                u'图片':u'图案',
                u'地点':u'地理信息',
                u'型号':u'型号词',
                u'定位/定位':u'价格/定位',
                u'整体款式样式':u'整体样式款式',
                u'材料':u'材质',
                u'版本':u'系列词',
                u'系列':u'系列词',
                u'纹理/质感':u'纹理质感',
                u'纹理.质感':u'纹理质感',
                u'款式':u'整体样式款式',
                u'使用场景':u'场景',
                u'露标':u'',
                u'分别错误':u'',
                u'分次错误':u'',
                u'其它':u'',
                }

fin = codecs.open(sys.argv[1], 'r', 'utf8')
fout = codecs.open(sys.argv[2], 'w', 'utf8')
for line in fin:
  val, key = line.strip().split(',')
  key = key_name_map.get(key, key.strip())
  if not key:
    continue
  s = '%s,%s\n' % (val,key)
  fout.write(s)
