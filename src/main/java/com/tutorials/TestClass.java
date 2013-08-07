package com.tutorials;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.google.common.collect.ImmutableMap;

public class TestClass {

	private static final int zkClientPort = 2181;
	private static final int numConnection = 500;
	private final int tickTime = 20;
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

			props.put("brokerid", "4");
			props.put("port", "9092");
			props.put("log.dir", kafkaLogDir.toString());
			// props.put("zk.sessiontimeout.ms", "2000000");
			// props.put("zk.maxSessionTimeout.ms", "200");

			kafkaServer = TestClass.getKafkaServer(props);
			kafkaServer.startup();
			System.out.println("Kafka Server started ...");

			// preparing the producers

			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(config);

			// sending single message
			// ProducerData<String, String> data = new ProducerData<String,
			// String>("test-topic", "test-message");

			List<String> messages = new ArrayList<String>();
			messages.add("this is test-message1");
			messages.add("this is test-message2");

			// sending multiple messages
			ProducerData<String, String> data = new ProducerData<String, String>("test-topic", messages);
			// ProducerData<String, String> data2 = new ProducerData<String,
			// String>("test-topic", messages);

			producer.send(data);

			// preparing consumer
			Properties conprops = new Properties();
			conprops.put("zk.connect", "localhost:2181");
			// conprops.put("zk.connectiontimeout.ms", "100");
			conprops.put("groupid", "test_group");

			ConsumerConfig consumerConfig = new ConsumerConfig(conprops);
			ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
			// create 4 partitions of the stream for topic “test”, to allow 4
			// threads to consume

			Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(ImmutableMap.of(
					"test-topic", 4));

			List<KafkaStream<Message>> streams = topicMessageStreams.get("test-topic");
			// create list of 4 threads to consume from each of the partitions
			ExecutorService executor = Executors.newFixedThreadPool(4);

			// consume the messages in the threads
			for (final KafkaStream<Message> stream : streams) {
				executor.submit(new Runnable() {
					public void run() {
						for (MessageAndMetadata msgAndMetadata : stream) {
							// process message (msgAndMetadata.message())
							System.out.println(msgAndMetadata.message());
						}
					}
				});
			}

		} catch (IOException e) {

			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// kafkaServer.shutdown();
			// System.out.println("Kafka shutdown");
			// factory.shutdown();
			// System.out.println("Zookeeper shutdown");

		}

	}
}
