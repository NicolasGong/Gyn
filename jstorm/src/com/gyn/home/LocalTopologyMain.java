package com.gyn.home;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by gongyining on 2016/4/23.
 */
public class LocalTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new FileReaderSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCountBolt(), 2).fieldsGrouping("word-normalizer", new Fields("word"));
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogies", conf, builder.createTopology());
        Thread.sleep(30000);
        cluster.shutdown();

    }
}
