# -*- coding:utf-8 -*-

import time
import logging
import psycopg2
import subprocess
from flask import request
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
        flow = request.args.get("l")
        logging.info("1********start--select:%s & flow:%s********" % (topic, flow))
        pg_select(topic, flow)
        code = 200
        # return "<h1 style='color:red'> %s %s </h1> " %(topic,code)
        return "topic_name:%s,Right_code:%s" % (topic, code)
    except:
        code = 500
        return "topic_name:%s,Error_code:%s" % (topic, code)


def sendqt(infor):
    startTime = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
    cmd = 'sh  /opt/work/addPQT.sh %s ' % infor
    (status, output) = subprocess.getstatusoutput(cmd)


def add_partition(topic_name, now_partitions, add_partitions):
    # client = KafkaAdminClient(bootstrap_servers="l-logcollectkafka1.ops.cna:9092",logger=info)
    client = KafkaAdminClient(bootstrap_servers="l-logcollectkafka1.ops.cna:9092")
    client.create_partitions({topic_name: NewPartitions(add_partitions)})
    addinfolog = '6********topic_name:%s add_partitions:%s success' % (topic_name, add_partitions)
    sendilog = 'info:[topic_name:%s##add_partitions_after_num:%s##]' % (topic_name, add_partitions)
    logging.info(addinfolog)
    pg_insert(topic_name, now_partitions, add_partitions)
    sendqt(sendilog)


def pg_select(topic, flow):
    conn = psycopg2.connect(dbname="logsysmeta", user="platform_bigdata_prd",
                            password="2d7e3ec9-6256-400a-bba8-09a923b35203", host="l-bigdatadbvip4.pf.cn6",
                            port="5432")
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
        get_partition(topic, flow)
    else:
        now = (time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime()))
        addwarnlog = '********END**********topic:%s Today_is_add_partition_exceed_50********' % topic
        sendelog = 'Exception:[topic_name:%s##Today_is_add_partition_exceed_50##]' % topic
        logging.info(addwarnlog)
        sendqt(sendelog)


def pg_insert(topic_name, now_partitions, add_partitions):
    # print (cluster_name,topic_name,now_partitions,add_partitions)
    conn = psycopg2.connect(dbname="logsysmeta", user="platform_bigdata_prd",
                            password="2d7e3ec9-6256-400a-bba8-09a923b35203", host="l-bigdatadbvip4.pf.cn6",
                            port="5432")
    cursor = conn.cursor()
    cursor.execute("INSERT INTO kafka_partitions (cluster_name,topic_name,now_partitions,update_time,add_partitions) \
                   VALUES (%s,%s,%s,now(),%s)", (cluster_name, topic_name, now_partitions, add_partitions))
    insertdblog = "7********END   INSERT DB  cluster_name:%s topic_name:%s now_partitions:%s add_partitions:%s success-END" % (
        cluster_name, topic_name, now_partitions, add_partitions)
    logging.info(insertdblog)
    conn.commit()
    cursor.close()
    conn.close()


def currency(topic, singlePflow, flowss, currentP):
    if singlePflow > 10485760:
        runlog = "3********current total-flow:%s and current partition:%s and Average flow:%s" % (
            flowss, currentP, singlePflow)
        logging.info(runlog)
        estimateP = int(flowss / 10485760)
        addP = estimateP - currentP
        addmessage = "4********add message addP=estimateP - currentP:%s= %s - %s" % (addP, estimateP, currentP)
        logging.info(addmessage)
        if addP == 0:
            saddplog = "5********START topic_name:%s  add partition:%s" % (topic, 1)
            logging.info(saddplog)
            add_partition(topic, currentP, currentP + 1)
            eaddplog = "8********END   topic_name:%s  add partition:%s done" % (topic, 1)
            logging.info(eaddplog)
        else:
            saddplog = "5********START topic_name:%s  add partition:%s" % (topic, addP)
            logging.info(saddplog)
            add_partition(topic, currentP, currentP + addP)
            eaddplog = "8********END   topic_name:%s  add partition:%s done" % (topic, addP)
            logging.info(eaddplog)

    else:
        exitlog = "topic:%s Average partitions flow:%s 不大于10M" % (topic, singlePflow)
        logging.info(exitlog)


def get_partition(topic, flow):
    zk = KazooClient(hosts='l-logcollectkafka1.ops.cna')
    zk.start()
    zkdir = '/brokers/topics/%s/partitions' % topic
    if zk.exists(zkdir):
        try:
            node = zk.get_children('/brokers/topics/%s/partitions' % topic)
            data = map(eval, node)
            currentP = (max(data) + 1)
            inforpartition = "********topic:%s current parition:%s " % (topic, currentP)
            logging.info(inforpartition)
            if "M" in flow:
                flows = flow.replace("M", "")
                flowss = float(flows) * 1024 * 1024  # 换算总流量
                singlePflow = int(flowss / currentP)  # 平均分区流量
                currency(topic, singlePflow, flowss, currentP)


            elif "G" in flow:
                flows = flow.replace("G", "")
                flowss = float(flows) * 1024 * 1024 * 1024  # 换算总流量
                singlePflow = int(flowss / currentP)  # 平均分区流量
                currency(topic, singlePflow, flowss, currentP)


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
