package fr.baptiste.main.event.driven;

import fr.baptiste.main.application.port.WriterStreamingPort;
import fr.baptiste.main.domain.Http;
import fr.baptiste.main.event.KafkaConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This class has the reponsibility to write data to a Kafka queue.
 * This class is a driven adaptor in an Hexagonal architecture.
 */
public class KafkaWriterStreamingAdapter implements WriterStreamingPort<String, Http> {
    private final KafkaConfiguration kafkaConfiguration;
    protected transient KafkaProducer<String, Http> producer;

    public KafkaWriterStreamingAdapter(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
        this.producer = null;
    }

    @Override
    public void write(String key, Http value) {
        this.producer = getProducer();
        this.producer.send(new ProducerRecord<>(kafkaConfiguration.getTopic(), key, value));
    }

    protected KafkaProducer<String, Http> getProducer() {
        if (this.producer == null) {
            this.producer = new KafkaProducer<>(kafkaConfiguration.getKafkaParams());
        }
        return this.producer;
    }
}
