#Instructions:

#[kakfa-cluster]
Use below command to log in to Kafka cluster:
>peg ssh kafka-cluster 1

On Kafka-cluster, run
>python ./kafka_producer_user.py user-topic-6  1

On Kafka-cluster, run
>python ./kafka_producer_stock.py stock-topic-6 1


#[spark-cluster]
Use below command to log in to spark cluster:
>peg ssh spark-cluster 1

On spark-cluster, run
>/usr/local/spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,anguenot/pyspark-cassandra:0.7.0 --conf --master spark://10.0.0.7:7077 spark.cassandra.connection.host=10.0.0.8  kafka_spark_consumer.py


#[cassandra-cluster]
Use below command to log in to Cassandra cluster:  
>peg ssh cassandra-cluster 1  

On Cassandra node, run:
>python ./cass_flask.py
