package common.factory;

import common.protocol.MBeanInfo;

import java.util.Map;

/**
 * Created by dubin on 29/09/2017.
 */
public interface KafkaMBeanService {

    /**
     * Bytes in per second from Kafka JMX MBean
     * @param url
     * @return
     */
    MBeanInfo bytesInPerSec(String url);

    /**
     * Bytes in per second per topic from Kafka JMX MBean
     * @param url
     * @param topic
     * @return
     */
    MBeanInfo bytesInPerSec(String url, String topic);

    /**
     * Bytes out per second from Kafka JMX MBean
     * @param url
     * @return
     */
    MBeanInfo bytesOutPerSec(String url);

    /**
     * Bytes out per second per topic from Kafka JMX MBean
     * @param url
     * @param topic
     * @return
     */
    MBeanInfo bytesOutPerSec(String url, String topic);

    /**
     * Get brokers topic all partitions log end offset
     * @param uri
     * @param topic
     * @return
     */
    Map<Integer, Long> logEndOffset(String uri, String topic);

    /**
     * Bytes in per second from all topics
     * @param uri
     * @return
     */
    MBeanInfo messagesInPerSec(String uri);

    /**
     * Bytes out per second from one topic
     * @param uri
     * @param topic
     * @return
     */
    MBeanInfo messagesInPerSec(String uri, String topic);
}
