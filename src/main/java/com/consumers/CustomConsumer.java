package com.consumers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;

import com.kafka.KafkaProperites;
import com.producers.ServerUtil;

public class CustomConsumer extends Thread {

	private final ConsumerConnector consumer;
	private final String topic;
	private static Properties prop;

	public CustomConsumer(String topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		prop = ServerUtil.getConfigProperties();
		prop.put("groupid", KafkaProperites.groupId);
		return new ConsumerConfig(prop);
	}

	public void consumeBySimpleConsumer() {
		// create a consumer to connect to the kafka server running on
		// localhost, port 9092, socket timeout of 10 secs, socket receive
		// buffer of ~1MB
		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 1024000);

		long offset = 0;
		while (true) {
			// create a fetch request for topic “test”, partition 0, current
			// offset, and fetch size of 1MB
			FetchRequest fetchRequest = new FetchRequest(KafkaProperites.topic1, 0, offset, 1000000);

			// get the message set from the consumer and print them out
			ByteBufferMessageSet messages = consumer.fetch(fetchRequest);
			for (MessageAndOffset msg : messages) {
				System.out.println("consumed: " + Utils.toString(msg.message().payload(), "UTF-8"));
				// advance the offset after consuming each message
				offset = msg.offset();
			}
		}
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));

		/* 
		 * The below lines of code is commented as it does not comply with the 0.8 version
		 * works with the 0.7 version of Kafka
		 *
		Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<Message> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<Message> it = stream.iterator();
		while (it.hasNext()) {
			System.out.println(Common.getMessage(it.next().message()));
		}
		
		*/
	}
}
