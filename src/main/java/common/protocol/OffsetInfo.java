package common.protocol;

/**
 * Created by dubin on 03/10/2017.
 */
public class OffsetInfo extends BaseProtocol{

    private String group;
    private String topic;
    private Long committedOffset;
    private Long logSize;
    private Long lag;
    private Long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getCommittedOffset() {
        return committedOffset;
    }

    public void setCommittedOffset(Long committedOffset) {
        this.committedOffset = committedOffset;
    }

    public Long getLogSize() {
        return logSize;
    }

    public void setLogSize(Long logSize) {
        this.logSize = logSize;
    }

    public Long getLag() {
        return lag;
    }

    public void setLag(Long lag) {
        this.lag = lag;
    }
}
