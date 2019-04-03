package com.aura.bigdata.storm.kafka;

import com.aura.bigdata.storm.util.MyStormUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * kafka和storm的整合
 * kafka作用，数据的提供方，也就是相当于Spout
 *
 *
 */
public class KafkaRealTimeOrderSumTopology {

    static class OrderBolt extends BaseRichBolt {
        private Map conf;
        private TopologyContext context;
        private OutputCollector collector;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            String line = new String(input.getBinary(0));
            System.out.println("kafka: " + line);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    private static KafkaSpout createKafkaSpout() {
        String zkStr = "bigdata01:2181,bigdata02:2181,bigdata03:2181";
        BrokerHosts hosts = new ZkHosts(zkStr);
        String topic = "kafka-storm";
        String zkRoot = "/brokers/kafka";
        String id = "id_storm_kafka";
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
        return kafkaSpout;
    }

    public static void main(String[] args) throws  Exception {
        //第一步：构建Topology构建器，用于组织Storm的作业，最终形成一个DAG(有向无环图)
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("id_spout", createKafkaSpout());
        tb.setBolt("id_bolt", new OrderBolt()).shuffleGrouping("id_spout");//流分组

        //第二步：使用TopologyBuilder构建Topology
        StormTopology stormTopology = tb.createTopology();
        //第三步：提交作业，可以使用集群模式、本地模式
        //Topology名称
        String topologyName = KafkaRealTimeOrderSumTopology.class.getSimpleName();
        //storm配置信息
        Config config = new Config();
        if(args == null || args.length < 1) {
            //本地模式LocalCluster
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topologyName, config, stormTopology);
        } else {
            StormSubmitter.submitTopology(topologyName, config, stormTopology);
        }
    }
}
