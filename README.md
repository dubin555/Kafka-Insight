# Kafka-Insight
This code is for monitoring Kafka offsets
## Install
### Requirement
* Kafka 0.10.x
* Java 8
* InfluxDB

### Compile
```bash
mvn clean package
```
### Change the config file
Modify the "application.conf", at least, the below parts need to be modified.
* kafka.zkAddr, the Zookeeper address
* kafka.db.influx.url, the InfluxDB address

### Deploy
Run the main class "app.KafkaInsight" anyway you want. 
It is single point for now and will be suffering from single point failure. HA is part of the plan.

## Wiki
For the other doc of the code, please refer to [Wiki](https://github.com/dubin555/Kafka-Insight.wiki.git)