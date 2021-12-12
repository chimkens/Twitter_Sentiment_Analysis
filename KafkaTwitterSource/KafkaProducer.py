from kafka import KafkaProducer
from KafkaTwitterSource import MyTwitterStream
import requests
import json

# For more information on kafka-python producer, visit this link
# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html

# set-up producer bootstrap server and serialize json messages
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def kafka_twitter_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=MyTwitterStream.bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))
            # Block until a single message is sent (or timeout)
            # future = producer.send('my_favorite_topic', key='Ukraine', value=twitter.get_stream(set))
            future = producer.send('my_favorite_topic', value=json_response)
            result = future.get(timeout=60)


# set rules
rules = MyTwitterStream.get_rules()
delete = MyTwitterStream.delete_all_rules(rules)
settings = MyTwitterStream.set_rules(delete)
kafka_twitter_stream(settings)
