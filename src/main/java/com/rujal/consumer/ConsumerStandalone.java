package com.rujal.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerStandalone {

	public Consumer<String, Object> createConsumer() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		map.put(ConsumerConfig.GROUP_ID_CONFIG, "24");
		map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		Consumer<String, Object> consumer = new KafkaConsumer<String, Object>(map);
		consumer.subscribe(Collections.singletonList("tests"));
		return consumer;
	}

	public static void main(String[] args) {
		ConsumerStandalone consumerController = new ConsumerStandalone();
		Consumer<String, Object> consmer = consumerController.createConsumer();
		ConsumerRecords<String, Object> records = consmer.poll(1000);
		int noMessageFound = 0;
		if (records.count() == 0) {

			noMessageFound++;
			if (noMessageFound > 1) {
				System.out.println("no message");
			}
		}
		// print each record.
		records.forEach(record -> {
			System.out.println("Record Key " + record.key());
			System.out.println("Record value " + record.value());
			System.out.println("Record partition " + record.partition());
			System.out.println("Record offset " + record.offset());
		});
		// commits the offset of record to broker.
		consmer.commitAsync();

		consmer.close();

	}
}
