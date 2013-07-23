package com.tutorials;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class TestClass {

	private static final int zkClientPort = 2181;
	private static final int numConnection = 500;
	private final int tickTime = 200;
	private static final String dataDirectory = System.getProperty("java.io.tmpdir");
	private static final File kafkaLogDir = new File(dataDirectory, "kafkaTestServerLogDir").getAbsoluteFile();

	private ZooKeeperServer getZKInstgance() {

		File dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
		ZooKeeperServer server = null;
		try {
			server = new ZooKeeperServer(dir, dir, tickTime);

		} catch (IOException e) {
			System.out.println("Error opening Zookeeper ..");
			e.printStackTrace();

		}
		return server;

	}

	private static KafkaServer getKafkaServer(Properties props) {

		KafkaConfig kafkaConfig = new KafkaConfig(props);
		KafkaServer kafkaServer = new KafkaServer(kafkaConfig);

		return kafkaServer;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		ZooKeeperServer server = new TestClass().getZKInstgance();
		Factory factory = null;
		KafkaServer kafkaServer = null;
		try {
			factory = new NIOServerCnxn.Factory(new InetSocketAddress(zkClientPort), numConnection);
			factory.startup(server);
			System.out.println("Zookeeper started ...");

			// starting up kafka server
			Properties props = new Properties();

			props.put("zk.connect", "localhost:2181");
			props.put("serializer.class", "kafka.serializer.StringEncoder");

			props.put("brokerid", "1");
			props.put("port", "9092");
			props.put("log.dir", kafkaLogDir.toString());

			kafkaServer = TestClass.getKafkaServer(props);
			kafkaServer.startup();
			System.out.println("Kafka Server started ...");

			// preparing the producers

			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(config);

			ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
			producer.send(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// factory.shutdown();
		}

	}
}
