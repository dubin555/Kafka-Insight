package app;

import task.KafkaMBeanTask;
import task.KafkaOffsetTask;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dubin on 05/10/2017.
 */
public class KafkaInsight {
    public static void main(String[] args) {
        ExecutorService es = Executors.newCachedThreadPool();
        es.submit(new KafkaMBeanTask());
        es.submit(new KafkaOffsetTask());
    }
}
