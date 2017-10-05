package task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.protocol.OffsetInfo;
import core.KafkaOffsetGetter;
import dao.OffsetsDao;
import dao.impl.OffsetsInfluxDBDaoImpl;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dubin on 04/10/2017.
 */
public class KafkaOffsetTask implements Runnable {

    private static Config conf = ConfigFactory.load();
    private static Logger logger = Logger.getLogger(KafkaOffsetTask.class);
    private static int secondsToSleep = conf.getInt("kafka.offset.task.freq");

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
        while (true) {
            try {
                List<OffsetInfo> offsetInfoList = KafkaOffsetGetter.getOffsetQuarz();
                OffsetsDao offsetsDao = new OffsetsInfluxDBDaoImpl(offsetInfoList);
                offsetsDao.insert();
            } catch (Exception e) {
                logger.error("Offset task error happen " + e.getMessage());
            } finally {
                try {
                    Thread.sleep(secondsToSleep * 1000);
                } catch (InterruptedException ex) {
                    logger.error("Offset task thread sleep error " + ex.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        KafkaOffsetTask task = new KafkaOffsetTask();
        ExecutorService es = Executors.newSingleThreadExecutor();
        es.submit(task);
    }
}
