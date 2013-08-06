package com.producers;

import java.io.IOException;

import com.kafka.KafkaProperites;

public class CustomProducerImpl {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ServerUtil.startserver();
			CustomProducers producerThread1 = new CustomProducers(KafkaProperites.topic1);
			producerThread1.produceStream();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
