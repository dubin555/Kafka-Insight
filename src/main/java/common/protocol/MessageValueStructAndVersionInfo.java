package common.protocol;

import org.apache.kafka.common.protocol.types.Struct;

/**
 * Created by dubin on 04/10/2017.
 */
public class MessageValueStructAndVersionInfo extends BaseProtocol {

    private Struct value;
    private Short version;

    public Struct getValue() {
        return value;
    }

    public void setValue(Struct value) {
        this.value = value;
    }

    public Short getVersion() {
        return version;
    }

    public void setVersion(Short version) {
        this.version = version;
    }
}
