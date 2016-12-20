# SparkStreamingHbase
kafka缓存数据，SparkStreaming接收处理导入Hbase
用于生成porduce文件，之后用Flume采集。
运行命令及配置如下所示
scala -classpath  producer.jar com.jacker.file.Producer 100
最后所带参数为生成文件的行数
#Flume 配置

agent.sources = loggersource
agent.channels = memoryChannel
agent.sinks = loggerSink

agent.sources.loggersource.channels = memoryChannel
agent.sources.loggersource.type=exec
agent.sources.loggersource.command =tail -F -n+1 /home/test.txt 

agent.sinks.loggerSink.type = org.apache.flume.sink.kafka.KafkaSink

agent.sinks.loggerSink.channel = memoryChannel
agent.sinks.loggerSink.kafka.topic = logger
agent.sinks.loggerSink.kafka.bootstrap.servers = localhost:9092
agent.sinks.loggerSink.kafka.flumeBatchSize = 20
agent.sinks.loggerSink.kafka.producer.acks = 1
agent.sinks.loggerSink.kafka.producer.linger.ms = 1
agent.sinks.loggerSink.kafka.producer.compression.type = snappy


agent.channels.memoryChannel.type = memory
agent.channels.memoryChannel.keep-alive = 60
agent.channels.memoryChannel.capacity = 1000000

#Flume启动命令
bin/flume-ng agent -n agent -c conf -f conf/flume-conf.properties &
#Kafka的启动命令
bin/kafka-server-start.sh config/server.properties &


#Kafka缓存数据之后，将通过Spark Streaming接收数据。之后存入到hbase中
