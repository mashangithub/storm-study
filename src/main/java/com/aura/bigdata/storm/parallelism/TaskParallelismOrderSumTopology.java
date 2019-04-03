package com.aura.bigdata.storm.parallelism;

import com.aura.bigdata.storm.util.MyStormUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
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
 * 调整Task级别的并行度
 *
 */
public class TaskParallelismOrderSumTopology {

    /**
     * spout只负责一件事，生成数据
     */
    static class NumSpout extends BaseRichSpout {

        /**
         * 可以理解为mapreduce中的setup方法，核心作用就是用来做初始化
         * @param conf  当前spout对应的配置信息
         * @param context   应用上下文
         * @param collector 发送spout中采集到的数据
         */
        private Map conf;
        private TopologyContext context;
        private SpoutOutputCollector collector;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }
        long num = 0;
        /**
         * 当前nextTuple相当于mapreduce中的map或者reduce方法，生成一条记录，该方法会被持续不断的调用
         */
        @Override
        public void nextTuple() {
            System.out.println(MyStormUtil.dateFormat() + "-->OLD 李商城产生订单交易金额为：" + num + "￥");
            this.collector.emit(new Values(num++));
            MyStormUtil.sleep(1000);
        }

        /**
         * 生命nextTuple通过collector发送出现的数据的别名，方便下游处理
         * 需要说明一点的是declare中的Fields和collector中发送的数据要一一对应。
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

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
        long sum = 0;
        /**
         * 核心的处理单元，数据会接收上一个组件(spout的nextTuple发送出来，或者bolt的execute方法发送出来)发送出来的数据，进行业务处理
         * @param input
         */
        @Override
        public void execute(Tuple input) {
            long num = input.getLongByField("num");
            sum += num;
            System.out.println("=====>" + MyStormUtil.dateFormat() + "-->OLD 李商城实时订单总交易金额为：" + sum + "￥");
            MyStormUtil.sleep(1000);
        }

        /**
         *  该方法不一定要复写，如果该bolt没有下游处理业务，就不需要重写
         * @param declarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    /**
     *
     * 1        5       5task
     *
     */
    public static void main(String[] args) throws  Exception {
        //第一步：构建Topology构建器，用于组织Storm的作业，最终形成一个DAG(有向无环图)
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("id_spout", new NumSpout(), 2).setNumTasks(5);//使用重载的函数用来设置executor的并行度
        tb.setBolt("id_bolt", new OrderBolt(), 2).setNumTasks(6).shuffleGrouping("id_spout");//流分组

        //第二步：使用TopologyBuilder构建Topology
        StormTopology stormTopology = tb.createTopology();
        //第三步：提交作业，可以使用集群模式、本地模式
        //Topology名称
        String topologyName = TaskParallelismOrderSumTopology.class.getSimpleName();
        //storm配置信息
        Config config = new Config();
        //之前1个worker的时候，有3个executor 3个task
        if(args == null || args.length < 1) {
            //本地模式LocalCluster
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topologyName, config, stormTopology);
        } else {
            StormSubmitter.submitTopology(topologyName, config, stormTopology);
        }
    }
}
