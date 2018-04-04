package com.kafka.flink.wiki;

 import org.apache.flink.api.common.functions.FoldFunction;
 import org.apache.flink.api.java.functions.KeySelector;
 import org.apache.flink.api.java.tuple.Tuple2;
 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.datastream.KeyedStream;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.streaming.api.windowing.time.Time;

 import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
 import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

/**
 * The <code>WikiEditConsumer</code> is a standalone Flink Consumer, meaning
 * that unlike the other examples within this project it does not consume
 * directly from a topic on Kafka.
 *
 * Starting Producer:
 *  mvn exec:java -Dexec.mainClass=com.kafka.flink.wiki.WikiEditConsumer
 */
public class WikiEditConsumer {

	public static void main(String[] args) throws Exception {
        // Setup Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Consume Directly From Source (Not Kafka Here)
        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());

		KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent event) {
              return event.getUser();
            }
        });

        // Add Operations/Functions/Transformations
        DataStream<Tuple2<String, Long>> result = keyedEdits
          .timeWindow(Time.seconds(5))
          .fold(new Tuple2<>("", 0L), new WikiFoldFunction());

        result.print();
        env.execute();
	}

	/**
	 *
	 */
	public static final class WikiFoldFunction implements FoldFunction<WikipediaEditEvent, Tuple2<String, Long>> {

		@Override
		public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
			acc.f0 = event.getUser();
			acc.f1 += event.getByteDiff();
			return acc;
		}
	}
}
