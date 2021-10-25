import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Kafka topic
TOPIC = "search-query-str-topic"

words = "兔子,老虎,犀牛,蟒蛇,海豹,孔雀,鳄鱼,猴子,棕熊,豺狼 石榴,香蕉,苹果,梨子,草莓,芒果,葡萄,蜜桃,荔枝,西瓜 海报,墙壁,围墙,汽车,卡车,单车,单词,鼠标,书包,书本 象棋,香气,围棋,跳棋,军旗。军区,蘑菇,木耳,母鸡,木屐 钱包,铅笔,前辈,相片,橡皮,格尺,彩旗,古筝".split(",")


for i in range(10):
    msg = {"user_id": "85a08043-1163-4148-8eda-a469b44767db", "query": random.choice(words), "ts": random.randint(0, 1000000000)}
    producer.send(TOPIC, json.dumps(msg, ensure_ascii=False).encode('utf8')).get(timeout=10)
