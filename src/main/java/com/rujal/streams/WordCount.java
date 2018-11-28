package com.rujal.streams;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class WordCount {
	
	public static void main(String[] args) throws Exception{
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String,String> source = builder.stream("streams-plaintext-input");
		source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
		.groupBy((key,value) -> value).count(Materialized.as("counts-store"))
		.toStream()
		.to("streams-wordcount-output",Produced.with(Serdes.String(),Serdes.Long()));
		
		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology,prop);
		final CountDownLatch latch= new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
		try {
			System.out.println("starting stream");
			streams.start();
			latch.await();
		}catch(Throwable e) {
			e.printStackTrace();
			System.out.println("stopped");
			System.exit(1);
		}
	}
}
