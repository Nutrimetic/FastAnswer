package fr.baptiste.main.event;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Service de génération des kafka params
 */

public class KafkaParamsGenerator {
    private final String bootstrapServers;
    private final Integer sessionTimeout;
    private final Integer pollTimeout;
    private final Integer maxPollRecords;
    private final boolean autoCommit;

    /**
     *  @param bootstrapServers list of broker separated with a comma
     * @param sessionTimeout value in seconds
     * @param pollTimeout value in seconds
     * @param maxPollRecords default is 500
     * @param autoCommit default is true
     */
    public KafkaParamsGenerator(String bootstrapServers, Integer sessionTimeout, Integer pollTimeout, Integer maxPollRecords, boolean autoCommit) {
        this.bootstrapServers = bootstrapServers;
        this.sessionTimeout = sessionTimeout;
        this.pollTimeout = pollTimeout;
        this.maxPollRecords = maxPollRecords;
        this.autoCommit = autoCommit;
    }

    public Map<String, Object> buildKafkaParamsConsumer(Class<? extends Deserializer<?>> keyDeserializer,
                                                        Class<? extends Deserializer<?>> valueDeserializer,
                                                        String groupId) {
        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", bootstrapServers); //list of brokers (should be a list in case of broker down)
        params.put("key.deserializer", keyDeserializer); //class use to deserialize the key
        params.put("value.deserializer", valueDeserializer); //class use to deserialize the value
        params.put("max.poll.records", maxPollRecords); //max number of records return in a single poll()
        params.put("enable.auto.commit", autoCommit); //Allow auto commit or manual commit
        params.put("heartbeat.interval.ms", pollTimeout); //the heartBeat interval between poll()
        params.put("session.timeout.ms", sessionTimeout); //the heartbeat time before the coordinator start rebalancing
        params.put("group.id", groupId); //groupId

        return params;
    }

    public Map<String, Object> buildKafkaParamsProducer(Class<? extends Serializer<?>> keySerializer,
                                                        Class<? extends Serializer<?>> valueSerializer) {
        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", bootstrapServers); //list of brokers (should be a list in case of broker down)
        params.put("key.serializer", keySerializer); //class use to deserialize the key
        params.put("value.serializer", valueSerializer); //class use to deserialize the value
        params.put("max.poll.records", maxPollRecords); //max number of records return in a single poll()
        params.put("enable.auto.commit", autoCommit); //Allow auto commit or manual commit
        params.put("heartbeat.interval.ms", pollTimeout); //the heartBeat interval between poll()
        params.put("session.timeout.ms", sessionTimeout); //the heartbeat time before the coordinator start rebalancing

        return params;
    }
}
