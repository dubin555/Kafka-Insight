package core;

import common.protocol.BrokersInfo;
import common.protocol.KeyAndValueSchemasInfo;
import common.protocol.MessageValueStructAndVersionInfo;
import common.protocol.OffsetInfo;
import common.util.KafkaUtils;
import common.util.ZookeeperUtils;
import kafka.common.OffsetMetadata;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.coordinator.GroupTopicPartition;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import kafka.common.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.log4j.Logger;
import scala.Array;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dubin on 02/10/2017.
 */
public class KafkaOffsetGetter {

    private final static Logger logger = Logger.getLogger(KafkaOffsetGetter.class);

    public static Map<GroupTopicPartition, OffsetAndMetadata> kafkaConsumerOffsets = new ConcurrentHashMap<>();

    public static Map<TopicPartition, Long> logEndOffsetMap = new ConcurrentHashMap<>();

    static {
        List<BrokersInfo> brokersInfoList = ZookeeperUtils.getAllBrokersInfo();
        ExecutorService es = Executors.newCachedThreadPool();
        es.submit(new ConsumerOffsetListener(ZookeeperUtils.getZkAddr()));
        for (BrokersInfo brokersInfo: brokersInfoList) {
            es.submit(new LogOffsetListener(brokersInfo));
        }
    }

    public static List<OffsetInfo> getOffsetQuarz() {

        Map<String, Map<String, List<OffsetInfo>>> groupTopicPartitionListMap = new ConcurrentHashMap<>();

        for (Map.Entry<GroupTopicPartition, OffsetAndMetadata> entry: kafkaConsumerOffsets.entrySet()) {
            GroupTopicPartition groupTopicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            String group = groupTopicPartition.group();
            TopicPartition topicPartition = groupTopicPartition.topicPartition();
            String topic = topicPartition.topic();
            int partition = topicPartition.partition();
            Long committedOffset = offsetAndMetadata.offset();

            if (!logEndOffsetMap.containsKey(topicPartition)) {
                logger.error("The logEndOffsetMap not contains " + topicPartition);
                return null;
            }
            long logSize = logEndOffsetMap.get(topicPartition);

            // May the refresh operation thread take some time to update
            logSize = logSize >= committedOffset ? logSize : committedOffset;
            long lag = committedOffset == -1 ? 0 : (logSize - committedOffset);

            OffsetInfo offsetInfo = new OffsetInfo();
            offsetInfo.setGroup(group);
            offsetInfo.setTopic(topic);
            offsetInfo.setCommittedOffset(committedOffset);
            offsetInfo.setLogSize(logSize);
            offsetInfo.setLag(lag);
            offsetInfo.setTimestamp(offsetAndMetadata.commitTimestamp());

            if (!groupTopicPartitionListMap.containsKey(group)) {
                Map<String, List<OffsetInfo>> topicPartitionMap = new ConcurrentHashMap<>();
                groupTopicPartitionListMap.put(group, topicPartitionMap);
            }
            if (!groupTopicPartitionListMap.get(group).containsKey(topic)) {
                List<OffsetInfo> offsetInfos = new ArrayList<>();
                groupTopicPartitionListMap.get(group).put(topic, offsetInfos);
            }
            groupTopicPartitionListMap.get(group).get(topic).add(offsetInfo);

        }
        return flattenNestedMap(groupTopicPartitionListMap);
    }

    private static List<OffsetInfo> flattenNestedMap(Map<String, Map<String, List<OffsetInfo>>> groupTopicPartitionListMap) {

        List<OffsetInfo> res = new ArrayList<>();

        for (Map.Entry<String, Map<String, List<OffsetInfo>>> entry: groupTopicPartitionListMap.entrySet()) {
            String group = entry.getKey();
            Map<String, List<OffsetInfo>> topicPartitionListMap = entry.getValue();
            for (Map.Entry<String, List<OffsetInfo>> topicPartitionListEntry: topicPartitionListMap.entrySet()) {
                String topic = topicPartitionListEntry.getKey();
                List<OffsetInfo> offsetInfos = topicPartitionListEntry.getValue();
                long committedOffset = 0;
                long logSize = 0;
                long lag = 0;
                long timestamp = 0L;
                for (OffsetInfo offsetInfo: offsetInfos) {
                    committedOffset += offsetInfo.getCommittedOffset();
                    logSize += offsetInfo.getLogSize();
                    lag += offsetInfo.getLag();
                    timestamp = offsetInfo.getTimestamp();
                }
                OffsetInfo o = new OffsetInfo();
                o.setGroup(group);
                o.setTopic(topic);
                o.setCommittedOffset(committedOffset);
                o.setLogSize(logSize);
                o.setLag(lag);
                o.setTimestamp(timestamp);
                res.add(o);
            }
        }
        return res;
    }


    public static class ConsumerOffsetListener implements Runnable {

        private final static String CONSUMER_OFFSET_TOPIC = "__consumer_offsets";

        /** ============================ Start Filter ========================= */
        private static Schema OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", Type.STRING), new Field("topic", Type.STRING), new Field("partition", Type.INT32));
        private static Field KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group");
        private static Field KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic");
        private static Field KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition");

        private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", Type.INT64), new Field("metadata", Type.STRING, "Associated metadata.", ""),
                new Field("timestamp", Type.INT64));

        private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", Type.INT64), new Field("metadata", Type.STRING, "Associated metadata.", ""),
                new Field("commit_timestamp", Type.INT64), new Field("expire_timestamp", Type.INT64));

        private static Field VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset");
        private static Field VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata");
        private static Field VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp");

        private static Field VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset");
        private static Field VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata");
        private static Field VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp");
        /** ============================ End Filter ========================= */

        private String zkAddr;
        private String group = "kafka-offset-insight-group";

        public ConsumerOffsetListener(String zkAddr) {
            this.zkAddr = zkAddr;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            ConsumerConnector consumerConnector = KafkaUtils.createConsumerConnector(zkAddr, group);
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(CONSUMER_OFFSET_TOPIC, new Integer(1));
            KafkaStream<byte[], byte[]> offsetMsgStream = consumerConnector.createMessageStreams(topicCountMap).get(CONSUMER_OFFSET_TOPIC).get(0);

            ConsumerIterator<byte[], byte[]> it = offsetMsgStream.iterator();
            while (true) {

                MessageAndMetadata<byte[], byte[]> offsetMsg = it.next();
                if (ByteBuffer.wrap(offsetMsg.key()).getShort() < 2) {
                    try {
                        GroupTopicPartition commitKey = readMessageKey(ByteBuffer.wrap(offsetMsg.key()));
                        if (offsetMsg.message() == null) {
                            continue;
                        }
                        kafka.common.OffsetAndMetadata commitValue = readMessageValue(ByteBuffer.wrap(offsetMsg.message()));
                        kafkaConsumerOffsets.put(commitKey, commitValue);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /** Kafka offset memory in schema. */
        @SuppressWarnings("serial")
        private static Map<Integer, KeyAndValueSchemasInfo> OFFSET_SCHEMAS = new HashMap<Integer, KeyAndValueSchemasInfo>() {
            {
                KeyAndValueSchemasInfo ks0 = new KeyAndValueSchemasInfo();
                ks0.setKeySchema(OFFSET_COMMIT_KEY_SCHEMA_V0);
                ks0.setValueSchema(OFFSET_COMMIT_VALUE_SCHEMA_V0);
                put(0, ks0);

                KeyAndValueSchemasInfo ks1 = new KeyAndValueSchemasInfo();
                ks1.setKeySchema(OFFSET_COMMIT_KEY_SCHEMA_V0);
                ks1.setValueSchema(OFFSET_COMMIT_VALUE_SCHEMA_V1);
                put(1, ks1);
            }
        };

        private static KeyAndValueSchemasInfo schemaFor(int version) {
            return OFFSET_SCHEMAS.get(version);
        }

        /** Analysis of Kafka data in topic in buffer. */
        private static GroupTopicPartition readMessageKey(ByteBuffer buffer) {
            short version = buffer.getShort();
            Schema keySchema = schemaFor(version).getKeySchema();
            Struct key = (Struct) keySchema.read(buffer);
            String group = key.getString(KEY_GROUP_FIELD);
            String topic = key.getString(KEY_TOPIC_FIELD);
            int partition = key.getInt(KEY_PARTITION_FIELD);
            return new GroupTopicPartition(group, new TopicPartition(topic, partition));
        }

        /** Analysis of buffer data in metadata in Kafka. */
        private static OffsetAndMetadata readMessageValue(ByteBuffer buffer) {
            MessageValueStructAndVersionInfo structAndVersion = readMessageValueStruct(buffer);
            if (structAndVersion.getValue() == null) {
                return null;
            } else {
                if (structAndVersion.getVersion() == 0) {
                    long offset = structAndVersion.getValue().getLong(VALUE_OFFSET_FIELD_V0);
                    String metadata = structAndVersion.getValue().getString(VALUE_METADATA_FIELD_V0);
                    long timestamp = structAndVersion.getValue().getLong(VALUE_TIMESTAMP_FIELD_V0);
                    return new OffsetAndMetadata(new OffsetMetadata(offset, metadata), timestamp, timestamp);
                } else if (structAndVersion.getVersion() == 1) {
                    long offset = structAndVersion.getValue().getLong(VALUE_OFFSET_FIELD_V1);
                    String metadata = structAndVersion.getValue().getString(VALUE_METADATA_FIELD_V1);
                    long commitTimestamp = structAndVersion.getValue().getLong(VALUE_COMMIT_TIMESTAMP_FIELD_V1);
                    return new OffsetAndMetadata(new OffsetMetadata(offset, metadata), commitTimestamp, commitTimestamp);
                } else {
                    throw new IllegalStateException("Unknown offset message version: " + structAndVersion.getVersion());
                }
            }
        }

        /** Analysis of struct data structure in metadata in Kafka. */
        private static MessageValueStructAndVersionInfo readMessageValueStruct(ByteBuffer buffer) {
            MessageValueStructAndVersionInfo mvs = new MessageValueStructAndVersionInfo();
            if (buffer == null) {
                mvs.setValue(null);
                mvs.setVersion(Short.valueOf("-1"));
            } else {
                short version = buffer.getShort();
                Schema valueSchema = schemaFor(version).getValueSchema();
                Struct value = (Struct) valueSchema.read(buffer);
                mvs.setValue(value);
                mvs.setVersion(version);
            }
            return mvs;
        }
    }


    public static class LogOffsetListener implements Runnable{

        private BrokersInfo brokersInfo;

        public LogOffsetListener(BrokersInfo brokersInfo) {
            this.brokersInfo = brokersInfo;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            String group = "kafka-insight-logOffsetListener";
            int sleepTime = 60000;
            KafkaConsumer<Array<Byte>, Array<Byte>> kafkaConsumer = null;

            while (true) {

                try {
                    if (null == kafkaConsumer) {
                        kafkaConsumer = KafkaUtils.createNewKafkaConsumer(brokersInfo, group);
                    }

                    Map<String, List<PartitionInfo>> topicPartitionsMap = kafkaConsumer.listTopics();
                    for (List<PartitionInfo> partitionInfoList : topicPartitionsMap.values()) {
                        for (PartitionInfo partitionInfo : partitionInfoList) {
                            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                            Collection<TopicPartition> topicPartitions = Arrays.asList(topicPartition);
                            kafkaConsumer.assign(topicPartitions);
                            kafkaConsumer.seekToEnd(topicPartitions);
                            Long logEndOffset = kafkaConsumer.position(topicPartition);
                            logEndOffsetMap.put(topicPartition, logEndOffset);
                        }
                    }

                    Thread.sleep(sleepTime);

                } catch (Exception e) {
                    e.printStackTrace();
                    if (null != kafkaConsumer) {
                        kafkaConsumer.close();
                        kafkaConsumer = null;
                    }
                }
            }

        }
    }
}
