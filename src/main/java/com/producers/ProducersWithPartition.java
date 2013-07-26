package com.producers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaServer;

import com.kafka.KafkaServerUtils;
import com.zookeeper.ZookeeperUtils;

public class ProducersWithPartition {

	public static Properties getConfigProperties() {
		Properties props = new Properties();

		props.put("zk.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		props.put("brokerid", "1");
		props.put("port", "9092");
		props.put("log.dir", KafkaServerUtils.kafkaLogDir.toString());

		return props;
	}

	private static void prepare() throws IOException, InterruptedException {

		ZookeeperUtils.startZooKeeper();

		KafkaServer kafkaServer = KafkaServerUtils.getKafkaServerInstance(ProducersWithPartition.getConfigProperties());
		kafkaServer.startup();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ProducersWithPartition.prepare();

			ProducerConfig config = new ProducerConfig(ProducersWithPartition.getConfigProperties());
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
