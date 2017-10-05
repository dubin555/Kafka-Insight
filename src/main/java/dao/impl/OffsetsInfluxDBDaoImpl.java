package dao.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.protocol.OffsetInfo;
import dao.OffsetsDao;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by dubin on 05/10/2017.
 */
public class OffsetsInfluxDBDaoImpl implements OffsetsDao {

    private final String influxDBUrl;
    private final List<OffsetInfo> offsetInfoList;
    private final String dbName;
    private static final Config conf = ConfigFactory.load();

    public OffsetsInfluxDBDaoImpl(List<OffsetInfo> offsetInfoList) {
        this.influxDBUrl = conf.getString("kafka.db.influx.url");
        this.dbName = conf.getString("kafka.db.influx.tableName.offset");
        this.offsetInfoList = offsetInfoList;
    }

    @Override
    public void insert() {
        InfluxDB influxDB = null;
        try {
            influxDB = InfluxDBFactory.connect(influxDBUrl);
            if (!influxDB.databaseExists(dbName)) {
                influxDB.createDatabase(dbName);
            }
            for (OffsetInfo offsetInfo : offsetInfoList) {
                String group = offsetInfo.getGroup();
                String topic = offsetInfo.getTopic();
                Long logSize = offsetInfo.getLogSize();
                Long offsets = offsetInfo.getCommittedOffset();
                Long lag = offsetInfo.getLag();
                Long timestamp = offsetInfo.getTimestamp();

                BatchPoints batchPoints = BatchPoints
                        .database(dbName)
                        .tag("group", group)
                        .tag("topic", topic)
                        .build();
                Point point = Point.measurement("offsetsConsumer")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .addField("logSize", logSize)
                        .addField("offsets", offsets)
                        .addField("lag", lag)
                        .build();
                batchPoints.point(point);
                influxDB.write(batchPoints);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (influxDB != null) {
                influxDB.close();
            }
        }

    }
}
