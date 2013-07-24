package com.tutorials;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import scala.actors.threadpool.ExecutorService;
import scala.actors.threadpool.Executors;

import com.google.common.collect.ImmutableMap;

public class TestConsumers {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// preparing consumer
		Properties conprops = new Properties();
		conprops.put("zk.connect", "localhost:2181");
		conprops.put("zk.connectiontimeout.ms", "1000");
		conprops.put("groupid", "test_group");
		conprops.put("groupid", "test_group");

		ConsumerConfig consumerConfig = new ConsumerConfig(conprops);
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
		// create 4 partitions of the stream for topic “test”, to allow 4
		// threads to consume
		Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector
				.createMessageStreams(ImmutableMap.of("test-topic", 4));

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

	}

}
