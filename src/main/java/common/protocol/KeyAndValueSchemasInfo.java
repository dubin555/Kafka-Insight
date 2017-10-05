package common.protocol;

import common.protocol.BaseProtocol;

import org.apache.kafka.common.protocol.types.Schema;

/**
 * Created by dubin on 04/10/2017.
 */
public class KeyAndValueSchemasInfo extends BaseProtocol{

    private Schema keySchema;
    private Schema valueSchema;

    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public void setValueSchema(Schema valueSchema) {
        this.valueSchema = valueSchema;
    }

}
