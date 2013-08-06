package com.demo;

import com.consumers.CustomConsumer;
import com.kafka.KafkaProperites;

public class CustomProducerConsumerDemoImpl {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {

			// ServerUtil.startserver();
			// CustomProducers producerThread1 = new
			// CustomProducers(KafkaProperites.topic1);
			// producerThread1.produceStream();

			CustomConsumer consumerThread1 = new CustomConsumer(KafkaProperites.topic1);
			consumerThread1.run();
			// consumerThread1.consumeBySimpleConsumer();

			// System.out.println("Testing single fetch");
			// SimpleConsumer simpleConsumer = new SimpleConsumer("127.0.0.1",
			// 9092, 10000, 1024000);
			// FetchRequest req = new FetchRequest(KafkaProperites.topic1, 0,
			// 0L, 100000000);
			// ByteBufferMessageSet messageSet = simpleConsumer.fetch(req);
			// Common.printMessages(messageSet);

			System.out.println("-- End Of Consumer --");

		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			System.out.println("-- Consumer Finally Executed --");
		}

	}

}
