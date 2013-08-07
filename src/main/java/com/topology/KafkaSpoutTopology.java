package com.topology;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.bolts.WordConcatenator;
import com.bolts.WordWriter;

public class KafkaSpoutTopology {

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String zkRoot = new File(System.getProperty("java.io.tmpdir"), "zookeeper").getAbsoluteFile().toString();
		System.out.println("====" + zkRoot);

		// ServerUtil.startserver();

		TopologyBuilder builder = new TopologyBuilder();

		// List<HostPort> hosts = new ArrayList<HostPort>();
		// hosts.add(new HostPort("1", 9092));

		String zookeepers = "localhost:2181";

		// SpoutConfig spoutConfig = new SpoutConfig(new
		// KafkaConfig.StaticHosts(hosts, partitionsPerHost), "topic1", zkRoot,
		// "id");

		// SpoutConfig spoutConfig = new SpoutConfig(new
		// SpoutConfig.ZkHosts(zookeepers, "/brokers"), "topic1", zkRoot, "0");
		// SpoutConfig spoutConfig = new SpoutConfig(new
		// SpoutConfig.ZkHosts(zookeepers, "/brokers"), "topic1", zkRoot, "1");

		// spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// spoutConfig.zkServers = ImmutableList.of("localhost");
		// spoutConfig.zkPort = 2181;

		List<String> hosts = new ArrayList<String>();
		hosts.add("localhost");

		SpoutConfig spoutConfig = new SpoutConfig(StaticHosts.fromHostString(hosts, 1), "topic1", "/kafkastorm", "1");
		// spoutConfig.scheme = new StringScheme();

		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		spoutConfig.forceStartOffsetTime(-1);
		// spoutConfig.zkServers = new ArrayList<String>() {
		// {
		// add("localhost");
		// }
		// };
		// spoutConfig.zkPort = 2181;

		builder.setSpout("kafka-msg-reader", new storm.kafka.KafkaSpout(spoutConfig));
		builder.setBolt("word-concatenator", new WordConcatenator()).shuffleGrouping("kafka-msg-reader");
		builder.setBolt("word-writer", new WordWriter()).shuffleGrouping("word-concatenator");

		Config conf = new Config();
		// conf.put("wordsFile", args[0]);
		conf.put("wordsFile", "src/main/resources/words.txt");
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5000);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Word-Concatenetor-Toplogie", conf, builder.createTopology());

	}

}
