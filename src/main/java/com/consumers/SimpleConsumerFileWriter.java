package com.consumers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import com.utils.Common;

public class SimpleConsumerFileWriter {

	private FileWriter writer;
	private BufferedWriter bufferedWriter;
	private File file;
	private final String filename = "src/main/resources/output.txt";

	public SimpleConsumerFileWriter() {

	}

	private void createConsumerWithErrorHandling(String topicName) throws IOException {

		file = new File(filename);
		try {

			if (!file.exists()) {
				file.createNewFile();
			}

			writer = new FileWriter(file.getAbsoluteFile());
			bufferedWriter = new BufferedWriter(writer);

			SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000, 1024000);
			FetchRequest fetchrequest = new FetchRequest(topicName, 0, 0, Integer.MAX_VALUE);
			ByteBufferMessageSet messageSet = consumer.fetch(fetchrequest);

			for (MessageAndOffset msgAndOffset : messageSet) {
				long currentOffset = msgAndOffset.offset();
			}

			Common.printMessages(messageSet);

			// old code segment http://kafka.apache.org/07/quickstart.html

			// long offset = 0;
			// long maxRead = 3000000;
			//
			// System.out.println("About to consume topic");
			// while (true) {
			// FetchRequest fetchrequest = new FetchRequest(topicName, 0,
			// offset, 1000000);
			//
			// ByteBufferMessageSet messages = consumer.fetch(fetchrequest);
			// for (MessageAndOffset msg : messages) {
			//
			// bufferedWriter.write(Utils.toString(msg.message().payload(),
			// "UTF-8"));
			// bufferedWriter.newLine();
			//
			// offset = msg.offset();
			// maxRead--;
			// }
			//
			// }

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Closing writer ..");
			bufferedWriter.close();
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			new SimpleConsumerFileWriter().createConsumerWithErrorHandling("test-topic-1");
		} catch (IOException e) {
			System.out.println("IOException in SimpleConsumerFileWriter ..");
			e.printStackTrace();
		}
	}

}
