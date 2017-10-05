package task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import common.factory.KafkaMBeanService;
import common.factory.KafkaMBeanServiceImpl;
import common.protocol.BrokersInfo;
import common.protocol.MBeanInfo;
import common.util.StrUtils;
import common.util.ZookeeperUtils;
import dao.MBeansDao;
import dao.impl.MBeansInfluxDBDaoImpl;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dubin on 30/09/2017.
 */
public class KafkaMBeanTask implements Runnable{

    private static Config conf = ConfigFactory.load();
    private static Logger logger = Logger.getLogger(KafkaMBeanService.class);
    private static int secondsToSleep = conf.getInt("kafka.mbean.jmx.task.freq");

    public static List<MBeanInfo> runMBeanTask() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        List<String> taskItems = conf.getStringList("kafka.mbean.jmx.task.items");

        KafkaMBeanService kafkaMBeanService = new KafkaMBeanServiceImpl();

        List<Method> methodListNoTopic = new ArrayList<>();

        for (String taskItem: taskItems) {
            methodListNoTopic.add(kafkaMBeanService.getClass().getDeclaredMethod(taskItem, String.class));
        }

        List<BrokersInfo> brokersInfos = ZookeeperUtils.getAllBrokersInfo();

        List<MBeanInfo> res = new ArrayList<>();

        for (Method taskMethod: methodListNoTopic) {
            List<MBeanInfo> mBeanInfos = new ArrayList<>();
            for (BrokersInfo brokersInfo: brokersInfos) {
                String url = brokersInfo.getHost() + ":" + brokersInfo.getJmxPort();
                MBeanInfo mBeanInfo = (MBeanInfo) taskMethod.invoke(kafkaMBeanService, url);
                mBeanInfo.setTopic("all");
                mBeanInfo.setLabel(taskMethod.getName());
                mBeanInfos.add(mBeanInfo);
            }
            res.add(mergeMBeanInfoList(mBeanInfos));
        }

        return res;
    }

    /**
     * I do not know how to use function program in Java 8, easy to implement this function in Scala
     * Ugly!
     * @param mBeanInfos
     * @return
     */
    private static MBeanInfo mergeMBeanInfoList(List<MBeanInfo> mBeanInfos) {
        if ((mBeanInfos == null) || (mBeanInfos.size() == 0)) {return null;}
        double fifteenMinute = 0;
        double fiveMinute = 0;
        double meanRate = 0;
        double oneMinute = 0;
        for (MBeanInfo mBeanInfo: mBeanInfos) {
            fifteenMinute += mBeanInfo.getFifteenMinute();
            fiveMinute += mBeanInfo.getFiveMinute();
            meanRate += mBeanInfo.getMeanRate();
            oneMinute += mBeanInfo.getOneMinute();
        }
        MBeanInfo res = new MBeanInfo();
        res.setLabel(mBeanInfos.get(0).getLabel());
        res.setTopic(mBeanInfos.get(0).getTopic());
        res.setFifteenMinute(fifteenMinute);
        res.setFiveMinute(fiveMinute);
        res.setOneMinute(oneMinute);
        res.setMeanRate(meanRate);
        System.out.println(res);
        return res;
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
        try {
            List<MBeanInfo> r = runMBeanTask();
            MBeansDao mBeansDao = new MBeansInfluxDBDaoImpl(r);
            mBeansDao.insert();
        } catch (Exception e) {
            logger.error("MBean service error " + e.getMessage());

        }
        finally {
            try {
                // sleep for a while anyway
                Thread.sleep(secondsToSleep * 1000);
            } catch (InterruptedException e) {
                logger.error("Thread sleep error " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        KafkaMBeanTask kafkaMBeanTask = new KafkaMBeanTask();
        ExecutorService es = Executors.newSingleThreadExecutor();
        es.execute(kafkaMBeanTask);
    }
}
