package com.consumers;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;

public class SimpleConsumerWithPartition {

	private static void prepareConsumer(SimpleConsumer consumer, String topicName, int partition) {
		long offset = 0;

		System.out.println("kafka.api.OffsetRequest.LatestTime() = " + kafka.api.OffsetRequest.LatestTime());
		System.out.println("kafka.api.OffsetRequest.EarliestTime() = " + kafka.api.OffsetRequest.EarliestTime());

		while (true) {
			FetchRequest fetchrequest = new FetchRequest(topicName, 0, offset, 1000000);
			ByteBufferMessageSet messages = consumer.fetch(fetchrequest);
			for (MessageAndOffset msg : messages) {
				System.out.println("consumed: " + Utils.toString(msg.message().payload(), "UTF-8"));
				offset = msg.offset();
			}

		}
	}

	public static void main(String[] args) {

		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 1024000);
		// long offset = 0;
		// while (true) {
		// FetchRequest fetchrequest = new FetchRequest("test-topic-message1",
		// 0, offset, 1000000);
		// ByteBufferMessageSet messages = consumer.fetch(fetchrequest);
		// for (MessageAndOffset msg : messages) {
		// System.out.println("consumed: " +
		// Utils.toString(msg.message().payload(), "UTF-8"));
		// offset = msg.offset();
		// }
		//
		// }

		SimpleConsumerWithPartition.prepareConsumer(consumer, "test-topic-message1", 1);

	}

}
