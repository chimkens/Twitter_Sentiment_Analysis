# Python Flink Application
Python flink applications can be directly submit to the Flink session cluster.

## Submit a Flink job
To submit the flink job run the command from the flink base folder

```
./bin/flink run --python <path to python file>
```
e.g,
```
./bin/flink run --python /Users/ESumitra/flink/python/flinkapp/word_count.py
 $FLINK_HOME/bin/flink run --python word_count.py
```


Copyright 2021, Edward Sumitra