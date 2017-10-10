[![Build Status](https://travis-ci.org/dubin555/Kafka-Insight.svg?branch=master)](https://travis-ci.org/dubin555/Kafka-Insight)
![](https://img.shields.io/badge/language-java-orange.svg)
[![Hex.pm](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/dubin555/Kafka-Insight/master/LICENSE)
# Kafka-Insight
This code is for monitoring Kafka offsets
What it looks like? You can modify it anyway you want in Grafana, here is just my first version for demo.
![Consumer offset and mBean monitor](https://github.com/dubin555/Kafka-Insight/blob/master/png/consumer-offsets-demo1.png)
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
For the other doc of the code, please refer to [Wiki](https://github.com/dubin555/Kafka-Insight/wiki)