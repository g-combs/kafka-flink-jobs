package com.kafka.flink.text;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.lang.Exception;
import java.lang.String;
import java.util.Properties;

/**
 *
 * Starting Consumer:
 *  mvn exec:java -Dexec.mainClass=com.kafka.flink.text.TextStreamConsumer
 */
public class TextStreamConsumer {

	public static void main(String[] args) throws Exception {
		// Setup Execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("group.id", "flinkConsumer");
        properties.setProperty("bootstrap.servers", "localhost:9092");
    	properties.setProperty("zookeeper.connect", "localhost:2181");

    	// Consume From topic on Kafka Stream
    	final String topic = "text-stream-demo";
		DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties));

		// Add Operations/Functions/Transformations
		StringMapFunction stringMapFunction = new StringMapFunction();
    	stream.rebalance().map(stringMapFunction).print();

    	env.execute();
	}

	/**
	 * Simple Class to generate data
	 **/
  	public static class StringMapFunction implements MapFunction<String, String> {
		private static final long serialVersionUID = -6867736771747690202L;

		@Override
		public String map(String value) throws Exception {
			return "Consumer Receiving: " + value;
		}
  	}
}
