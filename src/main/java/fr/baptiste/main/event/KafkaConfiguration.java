package fr.baptiste.main.event;

import java.io.Serializable;
import java.util.Map;

/**
 * This class has the reponsibility to contain all the configuration element to allow the connexion to
 * a Kafka queue (as a consumer and producer)
 */
public class KafkaConfiguration implements Serializable {
    private final String topic;
    private final Map<String, Object> kafkaParams;

    public KafkaConfiguration(String topic, Map<String, Object> kafkaParams) {
        this.topic = topic;
        this.kafkaParams = kafkaParams;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, Object> getKafkaParams() {
        return kafkaParams;
    }
}
