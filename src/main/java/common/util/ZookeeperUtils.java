package common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.protocol.BrokersInfo;
import kafka.consumer.ConsumerThreadId;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.*;

/**
 * Created by dubin on 30/09/2017.
 */
public class ZookeeperUtils {

    private final static  String BROKER_IDS_PATH = "/brokers/ids";
    private final static String BROKER_TOPICS_PATH = "/brokers/topics";
    private final static String CONSUMERS_PATH = "/consumers";

    private final static Logger logger = Logger.getLogger(ZookeeperUtils.class);

    private static Config conf = ConfigFactory.load();

    public static String getZkAddr() {
        return conf.getString("kafka.zkAddr");
    }

    public static List<BrokersInfo> getAllBrokersInfo() {
        String zkAddr = conf.getString("kafka.zkAddr");
        List<BrokersInfo> res = new ArrayList<>();
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkAddr, Integer.MAX_VALUE, 100000, ZKStringSerializer$.MODULE$);
            if (ZkUtils.apply(zkClient, false).pathExists(BROKER_IDS_PATH)) {
                Seq<String> subBrokerIdsPaths = ZkUtils.apply(zkClient, false).getChildren(BROKER_IDS_PATH);
                List<String> brokerIds = JavaConversions.seqAsJavaList(subBrokerIdsPaths);
                int id = 0;
                for (String ids: brokerIds) {
                    try {
                        Tuple2<Option<String>, Stat> tuple = ZkUtils.apply(zkClient, false).readDataMaybeNull(BROKER_IDS_PATH + "/" + ids);
                        BrokersInfo brokersInfo = new BrokersInfo();
                        int port = JSON.parseObject(tuple._1.get()).getInteger("port");
                        brokersInfo.setHost(JSON.parseObject(tuple._1.get()).getString("host"));
                        brokersInfo.setPort(JSON.parseObject(tuple._1.get()).getInteger("port"));
                        brokersInfo.setJmxPort(JSON.parseObject(tuple._1.get()).getInteger("jmx_port"));
                        brokersInfo.setId(++id);
                        res.add(brokersInfo);
                    } catch (Exception e){
                        logger.error("get sub broker info failed" + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("get Brokers info failed" + e.getMessage());
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return res;
    }

    public static List<String> getAllTopics() {
        String zkAddr = conf.getString("kafka.zkAddr");
        List<String> res = new ArrayList<>();
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkAddr, Integer.MAX_VALUE, 100000, ZKStringSerializer$.MODULE$);
            if (ZkUtils.apply(zkClient, false).pathExists(BROKER_TOPICS_PATH)) {
                Seq<String> subBrokerTopicsPaths = ZkUtils.apply(zkClient, false).getChildren(BROKER_TOPICS_PATH);
                res = JavaConversions.seqAsJavaList(subBrokerTopicsPaths);
            }
        } catch (Exception e) {
            logger.error("get Brokers info failed" + e.getMessage());
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return res;
    }

    public static Set<String> getActiveTopics() {
        String zkAddr = conf.getString("kafka.zkAddr");
        Set<String> res = new HashSet<>();
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkAddr, Integer.MAX_VALUE, 100000, ZKStringSerializer$.MODULE$);
            Seq<String> subConsumerPaths = ZkUtils.apply(zkClient, false).getChildren(CONSUMERS_PATH);
            List<String> groups = JavaConversions.seqAsJavaList(subConsumerPaths);
            for (String group : groups) {
                scala.collection.mutable.Map<String, scala.collection.immutable.List<ConsumerThreadId>> topics = ZkUtils.apply(zkClient, false).getConsumersPerTopic(group, false);
                for (Map.Entry<String, ?> entry : JavaConversions.mapAsJavaMap(topics).entrySet()) {
                    String topic = entry.getKey();
                    System.out.println(topic);
                    res.add(topic);
                }
            }
        } catch (Exception e) {
            logger.error("get Brokers info failed" + e.getMessage());
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        return res;
    }
}
