package example;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class TestProducer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// long events = Long.parseLong(args[0]);
		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		// for (long nEvents = 0; nEvents < events; nEvents++) {
		// long runtime = new Date().getTime();
		// String ip = "192.168.2." + rnd.nextInt(255);
		// String msg = runtime + ",www.example.com," + ip;
		// KeyedMessage<String, String> data = new KeyedMessage<String,
		// String>("page_visits", ip, msg);
		// producer.send(data);
		// }

		ProducerData<String, String> data = new ProducerData<String, String>("test-topic", "test-message");
		producer.send(data);
		producer.close();
	}
}
