# -*- coding:utf-8 -*-

import time
import logging
import psycopg2
from flask import Flask
from kazoo.client import KazooClient
from kafka.admin import KafkaAdminClient, NewPartitions

app = Flask(__name__)


cluster_name = "pf_kafka_docker_log"
conn = psycopg2.connect(dbname="logsysmeta", user="platform_bigdata_prd",
                            password="2d7e3ec9-6256-400a-bba8-09a923b35203", host="l-bigdatadbvip4.pf.cn6",
                            port="5432")

LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '
logging.basicConfig(level=logging.DEBUG,
                    format=LOG_FORMAT,
                    datefmt = DATE_FORMAT ,
                    filename=r"/opt/work/testlog.txt"
                    )








@app.route('/create/<topic>')
def index(topic):
    try:
        pg_select(topic)
        code = 200
        return "<h1 style='color:red'> %s,%s </h1> " %topic %code
    except:
        return  500

def day(infor):
    startTime=(time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
    cmd ='sh  /opt/work/qt_alter.sh %s %s'  %(startTime,infor)

def add_partition(topic,Pum):
    client = KafkaAdminClient(bootstrap_servers="l-logcollectkafka1.ops.cna:9092")
    print (client.create_partitions({topic: NewPartitions(Pum)}))
    now = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
    addinfolog = '%s info topic:%s add_partitions:%s success' %(now,topic,Pum)
    logging.info(addinfolog)


def pg_select(topic):
    cursor = conn.cursor()
    cursor.execute("select sum(add_partitions) from kafka_partitions where topic_name = '%s' and update_time::date = CURRENT_DATE;" %(topic))
    rows = cursor.fetchall()
    temp = tuple(rows)
    print (temp[0][0])
    if str((temp[0][0])) == 'None' or int((temp[0][0])) < 50:
        get_partition(topic)
    else:
        now = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
        addwarnlog = '%s info topic:%s Today_is_add_partition_exceed_50' %(now,topic)
        logging.warning(addwarnlog)


def pg_insert(topic_name,now_partitions,add_partitions):
    print ("start insert db ")
    print (cluster_name,topic_name,now_partitions,add_partitions)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO kafka_partitions (cluster_name,topic_name,now_partitions,update_time,add_partitions) \
                   VALUES (%s,%s,%s,now(),%s)",(cluster_name,topic_name,now_partitions,add_partitions))
    conn.commit()
    cursor.close()
    conn.close()
    now = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
    adddblog = '%s info:insert into db success' %now
    logging.info(adddblog)

def get_partition(topic):
    zk = KazooClient(hosts='l-logcollectkafka1.ops.cna,l-logcollectkafka2.ops.cna,l-logcollectkafka3.ops.cna,l-logcollectkafka4.ops.cna,l-logcollectkafka5.ops.cna',logger=None)
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
            pg_insert(topic,currentP,needaddp)
    except Exception as err:
        print (err.message)
    zk.stop()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)