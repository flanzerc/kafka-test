package com.producers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class ProducersWithPartition {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// ProducersWithPartition.prepare();
			ServerUtil.startserver();

			// ProducerConfig config = new
			// ProducerConfig(ProducersWithPartition.getConfigProperties());
			ProducerConfig config = new ProducerConfig(ServerUtil.getConfigProperties());
			Producer<String, String> producer = new Producer<String, String>(config);

			List<String> messagesTopic1 = new ArrayList<String>();
			messagesTopic1.add("this is test-message1_for_topic1 with partition");
			messagesTopic1.add("this is test-message2_for_topic1 with partition");

			List<String> messagesTopic2 = new ArrayList<String>();
			messagesTopic2.add("this is test-message2_for_topic2");
			messagesTopic2.add("this is test-message2_for_topic2");

			ProducerData<String, String> data1 = new ProducerData<String, String>("test-topic-message1", "1",
					messagesTopic1);
			ProducerData<String, String> data2 = new ProducerData<String, String>("test-topic-message2", messagesTopic2);

			List<ProducerData<String, String>> dataForMultipleTopics = new ArrayList<ProducerData<String, String>>();
			dataForMultipleTopics.add(data1);
			dataForMultipleTopics.add(data2);

			producer.send(dataForMultipleTopics);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
