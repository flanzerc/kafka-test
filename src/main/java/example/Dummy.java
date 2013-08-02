package example;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Dummy {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(System.getProperty("java.io.tmpdir"));

		File dir = new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsoluteFile();
		System.out.println(dir.toString());

		List msg = new ArrayList();
		long count = 0;
		while (count <= 20000000) {
			msg.add(count++);
		}

		System.out.println(":: " + msg.size());

	}

}
