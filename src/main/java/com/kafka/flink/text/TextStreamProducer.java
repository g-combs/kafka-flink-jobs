package com.kafka.flink.text;

 import org.apache.flink.api.common.serialization.SimpleStringSchema;

 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.streaming.api.functions.source.SourceFunction;
 import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

/**
 *
 * Starting Producer:
 *  mvn exec:java -Dexec.mainClass=com.kafka.flink.text.TextStreamProducer
 */
public class TextStreamProducer {


	public static void main(String[] args) throws Exception {
		// Setup the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Add a source
        DataStream<String> stream = env.addSource(new SimpleStringGenerator());

        // Write to topic on Kafka Stream
        final String topic = "text-stream-demo";
        final String bootstrapServers = "localhost:9092";
        stream.addSink(new FlinkKafkaProducer010<>(bootstrapServers, topic, new SimpleStringSchema()));

        env.execute();
	}

	/**
     * SourceFunction to generate simple String data
     */
	public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 119007289730474249L;
        private static boolean running = true;
        private static long counter = 0;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(running) {
                System.out.println("Producer Sending: Message " + (++counter));
                ctx.collect("Message " + counter);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
