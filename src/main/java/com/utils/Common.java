package com.utils;

import java.nio.ByteBuffer;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

public class Common {

	public static String getMessage(Message message) {
		ByteBuffer buffer = message.payload();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	public static void printMessages(ByteBufferMessageSet messageSet) {
		for (MessageAndOffset messageAndOffset : messageSet) {
			System.out.println(Common.getMessage(messageAndOffset.message()));
		}
	}

}
