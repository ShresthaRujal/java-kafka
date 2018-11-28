package com.rujal.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class Pipe {

	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-pipe");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> source = builder.stream("streams-plaintext-input");
		source.to("stream-pipe-output");
		final Topology topology = builder.build();
		System.out.println(topology.describe());
		
		final KafkaStreams streams = new KafkaStreams(topology, prop);
		final CountDownLatch latch = new CountDownLatch(1);
		
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
		
		try {
			streams.start();
			latch.await();
		}catch(Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
	
}
