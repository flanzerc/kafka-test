package example;

import java.io.File;

public class Dummy {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(System.getProperty("java.io.tmpdir"));

		File dir = new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsoluteFile();
		System.out.println(dir.toString());

	}

}
