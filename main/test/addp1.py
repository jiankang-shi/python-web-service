# -*- coding:utf-8 -*-
import time
import logging

from flask import Flask


from kafka import KafkaAdminClient
# from kafka import Cl
# from kafka.admin import NewPartitions
from kafka import KafkaClient
from kafka.admin import NewPartitions
from kazoo.client import KazooClient

app = Flask(__name__)

LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt = DATE_FORMAT ,
                    # filename=r"/opt/work/testlog.txt"
                    # filename=r"d:\test\test.log"
                    )


@app.route('/create/<topic_name>')
def index(topic_name):
    get_partition(topic=topic_name)
    return "<h1 style='color:red'> %s </h1> " %topic_name

# def get_partitions(topic):
#     client = KafkaClient(bootstrap_servers=['x'], request_timeout_ms=3000)
#     # print(client.cluster.available_partitions_for_topic(topic=topic))
#     # partitions = client.cluster.available_partitions_for_topic(topic)
#     # print("00000",partitions)

def add_partition(topic,Pum):
    client = KafkaAdminClient(bootstrap_servers="x")
    print (client.create_partitions({topic: NewPartitions(Pum)}))
    now = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
    addinfolog = '%s info topic:%s add_partitions:%s success' %(now,topic,Pum)
    print(addinfolog)
    logging.info()

def get_partition(topic):
    zk = KazooClient(hosts='x',logger=None)
    zk.start()
    print (zk.exists('/brokers/topics/%s/partitions' %topic))
    node = zk.get_children('/brokers/topics/%s/partitions' %topic)
    data = map(eval, node)
    try:
        currentP =(max(data)+1)
        needaddp = currentP//2
        if needaddp == 0:
            add_partition(topic,currentP+1)
        else:
            add_partition(topic,currentP+needaddp)
            # pg_insert(topic,currentP,needaddp)
    except Exception as err:
        print (err.message)
    zk.stop()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
