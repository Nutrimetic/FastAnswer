package fr.baptiste.main.event.driving;

import fr.baptiste.main.application.port.ReaderStreamingPort;
import fr.baptiste.main.event.KafkaConfiguration;
import fr.baptiste.main.utility.SparkHelper;
import fr.baptiste.main.domain.Http;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Collections;

/**
 * This class has the reponsibility to provide JavaInputDStream who will consume Data from a Kafka queue.
 * This class is a driving adaptor in an Hexagonal architecture.
 */
public class KafkaReaderStreamingAdapter implements ReaderStreamingPort<ConsumerRecord<String, Http>> {
    private final SparkHelper sparkHelper;
    private final KafkaConfiguration kafkaConfiguration;

    public KafkaReaderStreamingAdapter(SparkHelper sparkHelper, KafkaConfiguration kafkaConfiguration) {
        this.sparkHelper = sparkHelper;
        this.kafkaConfiguration = kafkaConfiguration;
    }

    @Override
    public JavaInputDStream<ConsumerRecord<String, Http>> build() {
        return KafkaUtils.createDirectStream(
                sparkHelper.getStreamingContext(),
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(
                        Collections.singletonList(kafkaConfiguration.getTopic()),
                        kafkaConfiguration.getKafkaParams())
        );
    }
}
