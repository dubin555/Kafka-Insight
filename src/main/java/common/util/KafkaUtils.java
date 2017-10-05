package common.util;

import common.protocol.BrokersInfo;
import kafka.consumer.Consumer;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import scala.Array;

import java.util.Properties;

/**
 * Created by dubin on 02/10/2017.
 */
public class KafkaUtils {

    public static KafkaConsumer<Array<Byte>, Array<Byte>> createNewKafkaConsumer(BrokersInfo brokersInfo, String group) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokersInfo.getHost() + ":" + brokersInfo.getPort());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "300000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new KafkaConsumer<>(props);
    }

    public static ConsumerConnector createConsumerConnector(String zkAddr, String group) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");
        props.put(KafkaConfig.ZkConnectProp(), zkAddr);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(props));
        return consumerConnector;
    }
}
