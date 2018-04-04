package com.kafka.flink.transaction;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;

import com.kafka.flink.transaction.source.Transaction;
import com.kafka.flink.transaction.source.TransactionGenerator;
import com.kafka.flink.transaction.source.TransactionSchema;

import static com.kafka.flink.Constants.*;

/**
 *
 * Starting Producer:
 *  mvn exec:java -Dexec.mainClass=com.kafka.flink.text.TransactionProducer
 */
public class TransactionProducer {

    public static void main(String[] args) throws Exception {
        // Setup the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Add a source
        DataStream<Transaction> stream = env.addSource(new TransactionGenerator());

        // Write to topic on Kafka Stream
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String topic = parameterTool.get(ARG_TOPIC, DEFAULT_TOPIC);
        final String brokerList = parameterTool.get(ARG_BROKER_LIST, DEFAULT_BROKER_LIST);
        stream.addSink(new FlinkKafkaProducer09<>(brokerList, topic, new TransactionSchema()));

        env.execute();
    }
}
