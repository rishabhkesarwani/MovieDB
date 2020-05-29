## Setup:
1. Use python 3.7.3 using python virtual environment. And install dependencies used in python files.
2. Install Kafka and start zookeeper and kafka server using [guide](https://kafka.apache.org/quickstart).
3. Create Kafka topics `media-movie`, `media-tv`, `cast-movie`, `cast-tv`.

## Run:
1. Run `multimedia_consumer.py` which will fetch multimedia ids from kafka topics `media-movie` and `media-tv` topics and produce cast ids from these multimedia in kafka topics `cast-movie` and `cast-tv` using command

```bash
python multimedia_producer.py
```

2. Run `multimedia_producer.py` which will fetch the pages of multimedia released or aired in Novemeber 2019 and produce the ids in kafka topics `media-movie`, `media-tv`.

```bash
python multimedia_consumer.py
```

3. Run the Spark Streaming program `cast_consumer.py` to combine the final topics to find out the result.

```bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 cast_consumer.py
```

4. Run the `verify.py` to verify the result without distributed computing. It takes a lot of time to finally output the result.