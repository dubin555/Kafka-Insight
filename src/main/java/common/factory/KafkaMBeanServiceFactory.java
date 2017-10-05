package common.factory;

/**
 * Created by dubin on 29/09/2017.
 */

public class KafkaMBeanServiceFactory {
    public KafkaMBeanService create() {
        return new KafkaMBeanServiceImpl();
    }
}
