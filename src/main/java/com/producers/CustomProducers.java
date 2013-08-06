package com.producers;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class CustomProducers extends Thread {

	private final Producer<Integer, String> producer;
	private final String topic;

	public CustomProducers(String topic) {
		// Use random partitioner. Don't need the key type. Just set it to
		// Integer. The message is of type String.
		producer = new Producer<Integer, String>(new ProducerConfig(ServerUtil.getConfigProperties()));
		this.topic = topic;
	}

	public void produceStream() {
		int messageCount = 1;
		while (true) {
			String messageStr = "MessageNo_" + messageCount;
			producer.send(new ProducerData<Integer, String>(topic, messageStr));
			messageCount++;
		}
	}

}
