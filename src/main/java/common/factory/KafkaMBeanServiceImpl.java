package common.factory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.protocol.MBeanInfo;
import common.util.StrUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by dubin on 29/09/2017.
 */
public class KafkaMBeanServiceImpl implements KafkaMBeanService {

    private Config conf = ConfigFactory.load();
    private Logger logger = LogManager.getLogger(KafkaMBeanServiceImpl.class);
    private String jmxUrl = "service:jmx:rmi:///jndi/rmi://%s/jmxrmi";

    /**
     * Bytes in per second from Kafka JMX MBean
     *
     * @param url
     * @return
     */
    @Override
    public MBeanInfo bytesInPerSec(String url) {
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";
        return common(url, mbean);
    }

    /**
     * Bytes in per second per topic from Kafka JMX MBean
     *
     * @param url
     * @param topic
     * @return
     */
    @Override
    public MBeanInfo bytesInPerSec(String url, String topic) {
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=" + topic;
        return common(url, topic);
    }

    /**
     * Bytes out per second from Kafka JMX MBean
     *
     * @param url
     * @return
     */
    @Override
    public MBeanInfo bytesOutPerSec(String url) {
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
        return common(url, mbean);
    }

    /**
     * Bytes out per second per topic from Kafka JMX MBean
     *
     * @param url
     * @param topic
     * @return
     */
    @Override
    public MBeanInfo bytesOutPerSec(String url, String topic) {
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=" + topic;
        return common(url, mbean);
    }

    /**
     * Get brokers topic all partitions log end offset
     *
     * @param url
     * @param topic
     * @return
     */
    @Override
    public Map<Integer, Long> logEndOffset(String url, String topic) {
        String mbean = "kafka.log:type=Log,name=LogEndOffset,topic=" + topic + ",partition=*";
        JMXConnector connector = null;
        Map<Integer, Long> endOffsets = new HashMap<>();
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(jmxUrl, url));
            connector = JMXConnectorFactory.connect(jmxSeriverUrl);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            Set<ObjectName> objectNames = mbeanConnection.queryNames(new ObjectName(mbean), null);
            for (ObjectName objectName : objectNames) {
                int partition = Integer.valueOf(objectName.getKeyProperty("partition"));
                Object value = mbeanConnection.getAttribute(new ObjectName(mbean), "Value");
                if (value != null) {
                    endOffsets.put(partition, Long.valueOf(value.toString()));
                }
            }
        } catch (Exception e) {
            logger.error("JMX service url[" + url + "] create has error,msg is " + e.getMessage());
        } finally {
            try {
                if (connector != null) {
                    connector.close();
                }
            } catch (Exception e) {
                logger.error("Close JMXConnector[" + url + "] has error,msg is " + e.getMessage());
            }
        }
        return endOffsets;
    }

    /**
     * Bytes in per second from all topics
     *
     * @param uri
     * @return
     */
    @Override
    public MBeanInfo messagesInPerSec(String uri) {
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
        return common(uri, mbean);
    }

    /**
     * Bytes out per second from one topic
     *
     * @param uri
     * @param topic
     * @return
     */
    @Override
    public MBeanInfo messagesInPerSec(String uri, String topic) {
        String mbean = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=" + topic;
        return common(uri, mbean);
    }


    private MBeanInfo common(String uri, String mbean) {

        JMXConnector connector = null;
        MBeanInfo MBeanInfo = new MBeanInfo();
        try {
            JMXServiceURL jmxSeriverUrl = new JMXServiceURL(String.format(jmxUrl, uri));
            connector = JMXConnectorFactory.connect(jmxSeriverUrl);
            MBeanServerConnection mbeanConnection = connector.getMBeanServerConnection();
            Object fifteenMinuteRate = mbeanConnection.getAttribute(new ObjectName(mbean), conf.getString("kafka.mbean.jmx.mbean.fifteenMinuteRate"));
            Object fiveMinuteRate = mbeanConnection.getAttribute(new ObjectName(mbean), conf.getString("kafka.mbean.jmx.mbean.fiveMinuteRate"));
            Object meanRate = mbeanConnection.getAttribute(new ObjectName(mbean), conf.getString("kafka.mbean.jmx.mbean.meanRate"));
            Object oneMinuteRate = mbeanConnection.getAttribute(new ObjectName(mbean), conf.getString("kafka.mbean.jmx.mbean.oneMinuteRate"));
            MBeanInfo.setFifteenMinute(StrUtils.numberic(fifteenMinuteRate.toString()));
            MBeanInfo.setFiveMinute(StrUtils.numberic(fiveMinuteRate.toString()));
            MBeanInfo.setMeanRate(StrUtils.numberic(meanRate.toString()));
            MBeanInfo.setOneMinute(StrUtils.numberic(oneMinuteRate.toString()));
        } catch (Exception e) {
            logger.error("JMX service url[" + uri + "] create has error,msg is " + e.getMessage());
        } finally {
            if (connector != null) {
                try {
                    connector.close();
                } catch (Exception e) {
                    logger.error("Close JMXConnector[" + uri + "] has error,msg is " + e.getMessage());
                }
            }
        }
        return MBeanInfo;
    }
}
