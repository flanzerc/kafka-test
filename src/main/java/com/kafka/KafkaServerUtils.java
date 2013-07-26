package com.kafka;

import java.io.File;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

public class KafkaServerUtils {

	private static final String dataDirectory = System.getProperty("java.io.tmpdir");
	public static final File kafkaLogDir = new File(dataDirectory, "kafkaTestServerLogDir").getAbsoluteFile();

	public static KafkaServer getKafkaServerInstance(Properties props) {

		KafkaConfig kafkaConfig = new KafkaConfig(props);
		KafkaServer kafkaServer = new KafkaServer(kafkaConfig);

		return kafkaServer;
	}

}
