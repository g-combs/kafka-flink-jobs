package com.kafka.flink;

public class Constants {

    public final static String ARG_ZOOKEEPER_CONNECT = "zookeeper.connect";
    public final static String ARG_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public final static String ARG_BROKER_LIST = "broker.list";
    public final static String ARG_GROUP_ID = "group.id";
    public final static String ARG_TOPIC = "topic";

    public final static String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public final static String DEFAULT_ZOOKEEPER_CONNECT = "localhost:2181";
    public final static String DEFAULT_BROKER_LIST = "localhost:9092";

    public final static String DEFAULT_TOPIC = "transactions";
    public final static String DEFAULT_GROUP_ID = "transaction-consumer-group";

}
