package com.producers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProducersStream {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ProducerUtil.prepare();
			// ProducerConfig config = new
			// ProducerConfig(ProducerUtil.getConfigProperties());
			// Producer<String, String> producer = new Producer<String,
			// String>(config);

			List<String> messagesTopic1 = new ArrayList<String>();
			long count = 0;
			while (count <= Long.MAX_VALUE) {
				messagesTopic1.add("this is test-message" + count + "_for_topic1");
				// System.out.println("Count=" + count++);
			}

			System.out.println("==>>" + messagesTopic1.size());

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
