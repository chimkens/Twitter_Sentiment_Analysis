import logging
import logging
import sys
import json

# from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
# from pyflink.datastream.connectors import (FileSource, StreamFormat)

# run using: ./bin/flink run --python /Users/ESumitra/workspaces/teaching/flink/python/word_count.py
# https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/python/datastream/intro_to_datastream_api/
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors import StreamingFileSink
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common import Row
from model import sentiment_classifier_methods as model
import os
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import JsonRowSerializationSchema
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder


# export JAVA_HOME=/Users/christina/PycharmProjects/Twitter_Project/flinkapp/jdk-11.0.6+10
# conda install -c anaconda openjdk=11

def sentiment_analysis():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    dirname = os.path.dirname(__file__)
    jar_filepath = os.path.join(dirname, 'flink-sql-connector-kafka_2.12-1.14.0.jar')
    jar_filepath2 = os.path.join(dirname, 'flink-streaming-java_2.12-1.14.0.jar')
    env.add_jars("file://" + jar_filepath)
    env.add_jars("file://" + jar_filepath2)
    print("Retrieving flink sql connector kafka and streaming from: ", jar_filepath)

    #Example Serialized Tweet from Kafka Consumer source
    # {
    #     "data": {
    #         "id": "1469546771038625797",
    #         "text": "\ud83d\udc36 Eevee, the happiest dog you\u2019ll ever meet! \ud83d\udc36 https://t.co/B56xNb9VGu"
    #     },
    #     "matching_rules": [
    #         {
    #             "id": "1469546402103578625",
    #             "tag": "dog pictures"
    #         }
    #     ]
    # }
    #This will come in as a row with a data map and matching_rules map. See schema below for types declaration
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=Types.ROW_NAMED(["data", "matching_rules"], [Types.MAP(Types.STRING(), Types.STRING()), Types.OBJECT_ARRAY(Types.MAP(Types.STRING(), Types.STRING()))])).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='my_favorite_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'my_favorite_group'})


    ds = env.add_source(kafka_consumer)

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.TUPLE([Types.STRING(), Types.INT()])])).build()

    kafka_producer = FlinkKafkaProducer(
        topic='sentiments_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'sentiment_group'})

    #ds.add_sink(kafka_producer)

    # compute tweet preprocessing
    # val tweets: DataStream[Tweet] = lines.map(Tweet(_)).filter(_.isSuccess).map(_.get)
    # val scoredTweets = tweets.map(ClassifierModel("gbm_embeddings_hex").predict(_))
    # scoredTweets.print()
    # scoredTweets
    #   .map(x => x.toString())
    #   .addSink(kafkaProducer)

    # define execution logic
    #ds.flat_map(print)
    tweet_texts = ds.map(lambda tweet: tweet[0]['text']) #grabbing tweet text from data->text
    scored_tweets = tweet_texts.map(lambda tweet: model.classify(tweet))

    #count tweet by sentiments
    counts = scored_tweets.map(lambda sentiment: (sentiment, 1))\
        .key_by(lambda entry: entry[0])\
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    # print or write to sink
    counts.print()
    #counts.sink_to(kafka_producer)
    #counts.add_sink(kafka_producer)
    output_path = '.'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
        .build()
    ds.sink_to(file_sink)

    env.execute("sentiment analysis")


# main entry point
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    sentiment_analysis()
    logging.info("Finished.")
