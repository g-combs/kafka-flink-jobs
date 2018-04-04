package com.kafka.flink.transaction;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import com.kafka.flink.transaction.source.Transaction;
import com.kafka.flink.transaction.source.TransactionSchema;

import java.util.Properties;

import static com.kafka.flink.Constants.*;

/**
 *
 * Starting Producer:
 *  mvn exec:java -Dexec.mainClass=com.kafka.flink.transaction.TransactionConsumer
 */
public class TransactionConsumer {

    public static void main(String[] args) throws Exception {
        // Setup Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Consumer Topic Stream
        Properties properties = buildProperties(args);
        final String topic = properties.getProperty(ARG_TOPIC);
        DataStream<Transaction> stream = env.addSource(new FlinkKafkaConsumer010<>(topic, new TransactionSchema(), properties));

        // TODO: Add Operations/Functions/Transformations (What could be done here with transactions?)

        env.execute();
    }

    private static Properties buildProperties(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        Properties properties = new Properties();
        properties.put(ARG_TOPIC, parameterTool.get(ARG_TOPIC, DEFAULT_TOPIC));
        properties.put(ARG_GROUP_ID, parameterTool.get(ARG_GROUP_ID, DEFAULT_GROUP_ID));
        properties.put(ARG_ZOOKEEPER_CONNECT, parameterTool.get(ARG_ZOOKEEPER_CONNECT, DEFAULT_ZOOKEEPER_CONNECT));
        properties.put(ARG_BOOTSTRAP_SERVERS, parameterTool.get(ARG_BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_SERVERS));

        return properties;
    }
}
