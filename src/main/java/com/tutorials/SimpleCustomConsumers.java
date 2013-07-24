package com.tutorials;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;

public class SimpleCustomConsumers {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 1024000);
		long offset = 0;
		while (true) {
			FetchRequest fetchrequest = new FetchRequest("test-topic", 0, offset, 1000000);
			ByteBufferMessageSet messages = consumer.fetch(fetchrequest);
			for (MessageAndOffset msg : messages) {
				System.out.println("consumed: " + Utils.toString(msg.message().payload(), "UTF-8"));
				offset = msg.offset();
			}

		}

	}

}
