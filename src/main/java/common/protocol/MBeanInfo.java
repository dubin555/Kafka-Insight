package common.protocol;

/**
 * Created by dubin on 29/09/2017.
 */
public class MBeanInfo extends BaseProtocol{

    private double fifteenMinute;
    private double fiveMinute;
    private double meanRate;
    private double oneMinute;
    private String label;
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public double getFifteenMinute() {
        return fifteenMinute;
    }

    public void setFifteenMinute(double fifteenMinute) {
        this.fifteenMinute = fifteenMinute;
    }

    public double getFiveMinute() {
        return fiveMinute;
    }

    public void setFiveMinute(double fiveMinute) {
        this.fiveMinute = fiveMinute;
    }

    public double getMeanRate() {
        return meanRate;
    }

    public void setMeanRate(double meanRate) {
        this.meanRate = meanRate;
    }

    public double getOneMinute() {
        return oneMinute;
    }

    public void setOneMinute(double oneMinute) {
        this.oneMinute = oneMinute;
    }

}
