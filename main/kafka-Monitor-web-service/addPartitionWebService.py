# -*- coding:utf-8 -*-

import time
import logging
import psycopg2
import subprocess
from flask import Flask
from kazoo.client import KazooClient
from kafka.admin import KafkaAdminClient, NewPartitions

app = Flask(__name__)

cluster_name = "pf_kafka_docker_log"

LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S %a '
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    filename=r"/opt/work/testlog.txt")


@app.route('/addpartitions/<topic>')
def index(topic):
    try:
        logging.info("1********start--select:%s********" % (topic))
        pg_select(topic)
        code = 200
        # return "<h1 style='color:red'> %s %s </h1> " %(topic,code)
        return "topic_name:%s,Right_code:%s" % (topic, code)
    except:
        code = 500
        return "topic_name:%s,Error_code:%s" % (topic, code)


def sendqt(infor):
    cmd = 'sh  /opt/work/addPQT.sh %s ' % infor
    (status, output) = subprocess.getstatusoutput(cmd)


def add_partition(topic, Pum):
    # client = KafkaAdminClient(bootstrap_servers="x",logger=info)
    client = KafkaAdminClient(bootstrap_servers="x")
    client.create_partitions({topic: NewPartitions(Pum)})
    addinfolog = 'topic_name:%s add_partitions:%s success' % (topic, Pum)
    sendilog = 'info:[topic_name:%s##add_partitions:%s##]' % (topic, Pum)
    logging.info(addinfolog)
    sendqt(sendilog)


def pg_select(topic):
    conn = psycopg2.connect(dbname="x", user="x",
                            password="x", host="x",
                            port="x")
    cursor = conn.cursor()
    cursor.execute(
        "select sum(add_partitions) from kafka_partitions where topic_name = '%s' and update_time::date = CURRENT_DATE;" % (
            topic))
    rows = cursor.fetchall()
    conn.commit()
    temp = tuple(rows)
    # print (temp[0][0])
    if str((temp[0][0])) == 'None' or int((temp[0][0])) < 50:
        logging.info("2********select--db:%s:is None/<50 ********" % (topic))
        get_partition(topic)
    else:
        now = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
        addwarnlog = '********END**********topic:%s Today_is_add_partition_exceed_50********' % topic
        sendelog = 'Exception:[topic_name:%s##Today_is_add_partition_exceed_50##]' % topic
        logging.info(addwarnlog)
        sendqt(sendelog)


def pg_insert(topic_name, now_partitions, add_partitions):
    # print (cluster_name,topic_name,now_partitions,add_partitions)
    conn = psycopg2.connect(dbname="x", user="x",
                            password="x", host="x",
                            port="x")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO kafka_partitions (cluster_name,topic_name,now_partitions,update_time,add_partitions) \
                   VALUES (%s,%s,%s,now(),%s)", (cluster_name, topic_name, now_partitions, add_partitions))
    insertdblog = "********END   INSERT DB  cluster_name:%s topic_name:%s now_partitions:%s add_partitions:%s success-END" % (
    cluster_name, topic_name, now_partitions, add_partitions)
    logging.info(insertdblog)
    conn.commit()
    cursor.close()
    conn.close()


def get_partition(topic):
    zk = KazooClient(hosts='l-logcollectkafka1.ops.cna')
    zk.start()
    zkdir = '/brokers/topics/%s/partitions' % topic
    if zk.exists(zkdir):
        try:
            # print ("存在")
            node = zk.get_children('/brokers/topics/%s/partitions' % topic)
            # print (node)
            data = map(eval, node)
            currentP = (max(data) + 1)
            needaddp = currentP // 2
            # print (currentP,needaddp)
            if needaddp == 0:
                saddplogz = "zero********START topic_name:%s  add partition:%s" % (topic, needaddp)
                logging.info(saddplogz)
                add_partition(topic, currentP + 1)
                eaddplogz = "zero********END  topic_name:%s  add partition:%s" % (topic, needaddp)
                logging.info(eaddplogz)

                sdblog = "zero********start  INSERT DB information"
                logging.info(sdblog)
                pg_insert(topic, currentP, needaddp)
            else:
                saddplog = "3********START topic_name:%s  add partition:%s" % (topic, needaddp)
                logging.info(saddplog)
                add_partition(topic, currentP + needaddp)
                eaddplog = "4********END   topic_name:%s  add partition:%s" % (topic, needaddp)
                logging.info(eaddplog)

                sdblog = "5********start  INSERT DB information"
                logging.info(sdblog)
                pg_insert(topic, currentP, needaddp)

        except Exception as err:
            print(err.message)
            logging.info(err.message)

    else:
        inter = "topic:%s is not exits" % topic
        logging.warning(inter)
        node = zk.get_children('/brokers/topics/%s/partitions' % topic)
    zk.stop()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)