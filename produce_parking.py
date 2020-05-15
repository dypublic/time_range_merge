from kafka import KafkaConsumer, KafkaClient, TopicPartition, KafkaProducer
import json
import datetime
import dataclasses
import random
import string
import threading
import time
import sched


def utc_time():
    return datetime.datetime.utcnow().timestamp()

def end_time(seconds):
    scheduler.enter(seconds, 0, print, argument=(seconds, " pass, should be the end event"))

Scheduler = sched.scheduler(utc_time, time.sleep)


topic = "parking_test"
qa_servers = ["bddevk01.dbhotelcloud.com:9092", "bddevk02.dbhotelcloud.com:9092", "bddevk03.dbhotelcloud.com:9092"]
sandbox = ["35.160.180.78:9092", "52.41.13.231:9092", "52.41.89.52:9092", "34.221.25.91:9092", "35.163.122.16:9092"]
producer = KafkaProducer(bootstrap_servers=qa_servers, api_version=(0, 10, 1),
                         client_id="parking_test_producer", compression_type="gzip")


def send_to_kafka(message):
    producer.send(topic, value=bytes(message, 'utf-8'))
    print(f"sending:{topic}:{message}")


def abs_send(time, message):
    Scheduler.enterabs(time, action=send_to_kafka, priority=0, argument=(message,))

#
# def write_topic(topic, cluster, messages):
#     producer = build_producer(cluster)
#     print(topic)
#
#     for message in messages:
#         # message value and key are raw bytes -- decode if necessary!
#         # e.g., for unicode: `message.value.decode('utf-8')`
#         producer.send(topic, value=bytes(message, 'utf-8'))
#         print(f"{topic}:{message}")
#     producer.flush()





"""
{"level":"info","time":"2019-12-18 11:01:42.019","msg":"","token":"af3d8651-a0fc-41fa-a7d3-af5effda2a2a","in.time":"2019-12-18 08:16:39","out.time":"2019-12-18 08:16:49","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
{"level":"info","time":"2019-12-18 11:01:42.023","msg":"","token":"2fc7f088-aa64-44bf-8bdb-02da92657137","in.time":"2019-12-18 08:16:38.97","out.time":"2019-12-18 08:16:48.97","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
{"level":"info","time":"2019-12-18 11:01:42.040","msg":"","token":"b5b45d82-337d-40c0-ade3-2b6019115f39","in.time":"2019-12-18 08:16:39.04","out.time":"2019-12-18 08:16:49.04","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
{"level":"info","time":"2019-12-18 11:01:42.050","msg":"","token":"2f5b2d98-7c7f-42ad-a868-041ebc3a7ac4","in.time":"2019-12-18 08:16:38.97","out.time":"2019-12-18 08:16:48.97","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
{"level":"info","time":"2019-12-18 11:01:42.057","msg":"","token":"827c4099-c383-47f2-9e08-7ea63b23541d","in.time":"2019-12-18 08:16:39.05","out.time":"2019-12-18 08:16:49.05","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
{"level":"info","time":"2019-12-18 11:01:42.062","msg":"","token":"c1e5e7d1-005b-447a-8636-16809acb1a2c","in.time":"2019-12-18 08:16:39.05","out.time":"2019-12-18 08:16:49.05","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
{"level":"info","time":"2019-12-18 11:01:42.062","msg":"","token":"c1575dab-0e9e-430d-be05-542ad2be1a65","in.time":"2019-12-18 08:16:39.04","out.time":"2019-12-18 08:16:49.04","hotel":"hotel_1","supplier":"supplier_1","channel":"channel_1","advance.day":0,"check.in":"2019-12-18","parking.time":10000}
"""

"""
<process=ParkingInTime> <supplier=HILTON> <hotel=HDNTK><in.time=2019-11-11 12:34:56.789> <token=D3A983DEF8*******B> <check.in=2019-11-11>
"""
"""
<process=ParkingOutTime> <supplier=HILTON> <channel=EXPEDIA> <hotel=HDNTK> <out.time=2019-11-11 12:33:33.789> <token=D3A983DEF8*******B> 
"""


@dataclasses.dataclass
class InTime:
    process: str
    supplier: str
    hotel: str
    in_time: str
    token: str
    check_in: str


@dataclasses.dataclass
class OutTime:
    process: str
    supplier: str
    channel: str
    hotel: str
    out_time: str
    token: str
    check_in: str



#
# write_topic(topic, cluster, ["test1"])
def produce(n):
    for i in range(n):
        suppliers = ["HILTON", "ACCOR", "BESTWESTERN", "CARLSON", "HOTELBEDS"]
        supplier = random.choice(suppliers)
        t1 = utc_time()

        hotel = "".join(random.choices(string.ascii_letters + string.digits, k=5))
        token = "".join(random.choices(string.ascii_letters + string.digits, k=20))
        check_in = datetime.datetime.now() + datetime.timedelta(days=random.randint(0, 365))
        check_in_string = check_in.date().isoformat()
        channels = ["AGODA", "BOOKINGCOM", "CTRIP"]
        channel = random.choice(channels)
        in_time = InTime("ParkingInTime", supplier, hotel, t1, token, check_in_string)
        in_time_json = json.dumps(dataclasses.asdict(in_time))
        print(in_time_json)

        out_msg_num = random.randint(1, 200)
        for i in range(out_msg_num):
            t2 = t1 + random.randrange(-2, 30)
            out_time = OutTime("ParkingOutTime", supplier, channel, hotel, t2, token, check_in_string)
            out_time_json = json.dumps(dataclasses.asdict(out_time))
            abs_send(t2, out_time_json)
            # print(out_time_json)


if __name__ == '__main__':
    # scheduler = sched.scheduler(utc_time, time.sleep)
    sched_thread = threading.Thread(target=Scheduler.run)
    # end_time(60*10)
    # sched_thread.start()
    produce(20)
    sched_thread.start()
    time.sleep(1000)