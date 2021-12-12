from kafka import KafkaConsumer

# For more information on kafka-python consumer, visit this link
# https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html

consumer = KafkaConsumer('my_favorite_topic', group_id='my_favorite_group')
for msg in consumer:
    print (msg)