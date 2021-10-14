# -*- coding:utf-8 -*-
import json

import null as null
from kazoo.client import KazooClient

zk =KazooClient(hosts="l-logcollectkafka1.ops.cna")
zk.start()
node = zk.get_children('/config/topics')





for i in node:
    if  i.startswith("prod_") or i.startswith("pub"):
        zk_node = '/config/topics/'+i
        # print(zk_node)
        aa=zk.get(zk_node)
        # print(aa)
        bb = eval(aa[0].decode('utf-8'))
        # print(bb["config"])
        if  bb["config"]:
            print(aa)




