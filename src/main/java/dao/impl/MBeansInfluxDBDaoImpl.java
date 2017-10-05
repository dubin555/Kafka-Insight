package dao.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.protocol.MBeanInfo;
import dao.MBeansDao;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by dubin on 05/10/2017.
 */
public class MBeansInfluxDBDaoImpl implements MBeansDao {

    private final String influxDBUrl;
    private final List<MBeanInfo> mBeanInfoList;
    private final String dbName;
    private static final Config conf = ConfigFactory.load();

    public MBeansInfluxDBDaoImpl(List<MBeanInfo> mBeanInfoList) {
        this.influxDBUrl = conf.getString("kafka.db.influx.url");
        this.dbName = conf.getString("kafka.db.influx.tableName.mbean");
        this.mBeanInfoList = mBeanInfoList;
    }

    @Override
    public void insert() {

        InfluxDB influxDB = null;
        try {
            influxDB = InfluxDBFactory.connect(influxDBUrl);
            if (!influxDB.databaseExists(dbName)) {
                influxDB.createDatabase(dbName);
            }
            for (MBeanInfo mBeanInfo : mBeanInfoList) {
                String label = mBeanInfo.getLabel();
                String topic = mBeanInfo.getTopic();
                double oneMinute = mBeanInfo.getOneMinute();
                double fiveMinute = mBeanInfo.getFiveMinute();
                double fifteenMinute = mBeanInfo.getFifteenMinute();
                double meanRate = mBeanInfo.getMeanRate();


                BatchPoints batchPoints = BatchPoints
                        .database(dbName)
                        .tag("label", label)
                        .tag("topic", topic)
                        .build();
                Point point = Point.measurement("offsetsConsumer")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .addField("oneMinuteRate", oneMinute)
                        .addField("fiveMinuteRate", fiveMinute)
                        .addField("fifteenMinuteRate", fifteenMinute)
                        .addField("meanRate", meanRate)
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
