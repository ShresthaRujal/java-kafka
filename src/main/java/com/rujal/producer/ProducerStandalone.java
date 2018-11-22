package com.rujal.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerStandalone {

	public KafkaProducer<String, Object> producerFactory() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.ACKS_CONFIG, "1");
		return new KafkaProducer<String, Object>(map);
	}

	public void produceMessage() {

		KafkaProducer<String, Object> pf = producerFactory();
		//topc key and message
		ProducerRecord<String, Object> record = new ProducerRecord<String, Object>("tests", "3", "Java kafak test2");
		try {
			RecordMetadata metadata = pf.send(record).get();
			System.out.println("Record sent with key 3 to partition " + metadata.partition() + " with offset "
					+ metadata.offset());
		} catch (InterruptedException e) {
			System.out.println("Error :");
			System.out.println(e);
			e.printStackTrace();
		} catch (ExecutionException e) {
			System.out.println("Error :");
			System.out.println(e);
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		ProducerStandalone producerController = new ProducerStandalone();
		producerController.produceMessage();
	}
}
