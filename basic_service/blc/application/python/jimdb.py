#!/data0/jd_ad/admin/python/bin/python2.7
import redis
import subprocess
import json
import sys
sys.path.append("./")

if __name__ == "__main__":
    pool = redis.ConnectionPool(host='ap2.jd.local', port=5360,
            password='jim://2580239461021160815/4817')
    r = redis.StrictRedis(connection_pool=pool)

    pipe = subprocess.Popen("hadoop fs -cat /user/jd_ad/ads_anti/yangxi/sku_service/2018-04-14/3c/*",
            shell=True, bufsize=1, stdout=subprocess.PIPE)
    #send_skus = {}
    for line in pipe.stdout:
        parts = line.replace('\n', '').split('\t')
        skuid = long(parts[0])
        blcids = [long(blcid) for blcid in json.loads(parts[2])["blc_ids"]]
        #send_skus[skuid] = blcids
        r.set(skuid, blcids)
    #print(len(send_skus))
    #r.mset(send_skus)
