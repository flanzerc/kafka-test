package com.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class ZookeeperUtils {

	private static final int zkClientPort = 2181;
	private static final int numConnection = 500;
	private static final int tickTime = 20;
	private static final String dataDirectory = System.getProperty("java.io.tmpdir");

	private static ZooKeeperServer getZKInstance() {

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

	public static void startZooKeeper() throws IOException, InterruptedException {
		ZooKeeperServer server = ZookeeperUtils.getZKInstance();
		Factory zkfactory = new NIOServerCnxn.Factory(new InetSocketAddress(zkClientPort), numConnection);
		zkfactory.startup(server);
		System.out.println("ZooKeeper server started ..");
	}

}
