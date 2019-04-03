package com.aura.bigdata.storm.local;

import com.aura.bigdata.storm.util.MyStormUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 实时统计目录中新增文件中的单词次数
 * 监听：E:/data/storm下的新增文件
 */
public class FileWordsTopology {

    /**
     * 第一步：监听指定目录下面的新增文件，读取新增文件的内容，发送给下游bolt做处理
     * File[] files = File.listFiles(FileFilter);
     * /hello.txt.completed
     * /hello.log.completed
     * for(File file : files) {
     *     BufferedReader br = new BufferedReader(new FileReader(file));
     *     String line = null;
     *     while((line = br.readLine()) != null) {
     *       collector.emit(line);
     *     }
     *     ...该file文件已经被读取完毕,重命名文件
     * }
     */


//    这是用BaseRichSpout来写的相当于MR的程序！！！
    static class FileSpout extends BaseRichSpout {
        private Map conf;
        private TopologyContext context;
        private SpoutOutputCollector collector;
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            File directory = new File("E:/data/storm");
            try {
                //第二个参数，表示要读取的文件的扩展名，只读取以.log或者.txt结尾的文件
                Collection<File> files = FileUtils.listFiles(directory, new String[]{"log", "txt"}, true);
                for(File file : files) {
                    List<String> lines = FileUtils.readLines(file, "UTF-8");
                    for(String line : lines) {
                        this.collector.emit(new Values(line));
                    }
                    //读取文件结束，该文件重命名---hello.log--->hello.log.1232423423423
                    File destFile = new File(file.getParent(), file.getName() + "." + System.currentTimeMillis());
                    FileUtils.moveFile(file, destFile);
                }
                MyStormUtil.sleep(1000);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    static class SplitBolt extends BaseRichBolt {
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
        public void execute(Tuple input) {//读到的就是spout发送过来的line数据
            String line = input.getString(0);
//            input.getStringByField("line");
            String[] words = line.split("\\s+");//\s代表空白符 比如空格 \t \r \n +代表至少出现一次
            for (String word : words) {
                this.collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    static class WCBolt extends BaseRichBolt {
        private Map conf;
        private TopologyContext context;
        private OutputCollector collector;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        Map<String, Integer> map = new HashMap<>();
        @Override
        public void execute(Tuple input) {//读到的就是SplitBolt发送过来的word和count数据
            String word = input.getStringByField("word");
            Integer count = input.getIntegerByField("count");
//            map.put(word, map.getOrDefault(word, 0) + count);
            Integer dbCount = map.get(word);
//            if(dbCount == null) {//map中不存在word
//                dbCount = 0;
//            }
//            //最新的word对应的count：count+dbCount
//            map.put(word, dbCount + count);
            map.put(word, map.getOrDefault(word, 0) + count);

            System.out.println("==============统计结果========================");
            map.forEach((k, v) -> {
                System.out.println(k + "===>" + v);
            });
            System.out.println("=============================================");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("id_file_spout", new FileSpout());
        tb.setBolt("id_split_bolt", new SplitBolt()).shuffleGrouping("id_file_spout");
        tb.setBolt("id_statis_bolt", new WCBolt()).shuffleGrouping("id_split_bolt");

        StormTopology stormTopology = tb.createTopology();

        LocalCluster lc = new LocalCluster();
        String topologyName = FileWordsTopology.class.getSimpleName();
        Config config = new Config();
        lc.submitTopology(topologyName, config, stormTopology);
    }
}
