package com.producers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class ProducersStream {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Producer<String, String> producer = null;
		try {
			ProducerUtil.prepare();
			// ProducerConfig config = new
			// ProducerConfig(ProducerUtil.getConfigProperties());
			// Producer<String, String> producer = new Producer<String,
			// String>(config);

			ProducerConfig config = new ProducerConfig(ProducerUtil.getConfigProperties());
			producer = new Producer<String, String>(config);

			List<String> messagesTopic1 = new ArrayList<String>();
			long count = 0;
			while (count <= 3000000) {
				// messagesTopic1.add("_for_topic1_" + count++);
				// System.out.println("Count=" + count++);
				producer.send(new ProducerData<String, String>("test-topic-1", "Message_" + count));
				count++;

			}

			System.out.println("++++++++ Producer Completed With Count ++++++" + count);

			// System.out.println("==>>" + messagesTopic1.size());
			// ProducerData<String, String> data1 = new ProducerData<String,
			// String>("test-topic-1", messagesTopic1);
			// producer.send(data1);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
