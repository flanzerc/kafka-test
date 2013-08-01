package com.producers;

import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaServer;

import com.kafka.KafkaServerUtils;
import com.zookeeper.ZookeeperUtils;

public class ProducerUtil {

	public static Properties getConfigProperties() {
		Properties props = new Properties();

		props.put("zk.connect", "localhost:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		props.put("brokerid", "1");
		props.put("port", "9092");
		props.put("log.dir", KafkaServerUtils.kafkaLogDir.toString());

		return props;
	}

	public static void prepare() throws IOException, InterruptedException {

		ZookeeperUtils.startZooKeeper();

		KafkaServer kafkaServer = KafkaServerUtils.getKafkaServerInstance(ProducerUtil.getConfigProperties());
		kafkaServer.startup();
	}

}
