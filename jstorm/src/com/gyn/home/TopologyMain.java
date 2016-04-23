package com.gyn.home;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by gongyining on 2016/4/23.
 */
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new FileReaderSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCountBolt(), 2).fieldsGrouping("word-normalizer", new Fields("word"));
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        StormSubmitter.submitTopology("Count-Word-Topology", conf, builder.createTopology());
    }
}
