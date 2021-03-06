import random
import time

from kafka import KafkaProducer
import json

from tqdm import tqdm

'''
    生产者demo
    向test_lyl2主题中循环写入10条json数据
    注意事项：要写入json数据需加上value_serializer参数，如下代码
'''
# producer = KafkaProducer(
#     value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
#     bootstrap_servers=['localhost:9092']
# )

# for i in range(2):
#     data = {
#         "name": "李四",
#         "age": 23,
#         "gender": "男",
#         "id": i
#     }
#     # data = "hello {}".format(i)
#     producer.send('test_topic', data)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']
)
# for i in range(2):
#     data = "hello {}".format(i).encode()
#     producer.send('test', data)
# producer.close()
# exit()


filepath = "./UserDataOrder.csv"


total_records_num = 100150807
# total_records_num = 1000
# top_num = 1000000
top_num = 100

all_behavior = {}
with open(filepath, 'r') as f:
    # all_behavior = {idx: l.strip() for idx, l in tqdm(enumerate(f)) if idx <= top_num else break}
    for idx, l in tqdm(enumerate(f)):
        if idx <= top_num:
            all_behavior.update({idx: l.strip()})
        else:
            break

line_no = -1


def generate_user_behavior_in_order():
    global line_no
    line_no += 1
    if line_no >= len(all_behavior):
        return ""
    return all_behavior[line_no]


def generate_user_behavior():
    line_no = random.randrange(0, len(all_behavior) - 1)
    return all_behavior[line_no]


# generate_user_behavior()

while True:
    # time.sleep(random.randint(0, 2))
    time.sleep(random.random())
    # data = generate_user_behavior().encode()
    data = generate_user_behavior_in_order().encode()
    if not data:
        break
    producer.send('user_behavior', data)
    print(data)
    # data = "hello".encode()
    producer.send("test", data)

producer.close()
