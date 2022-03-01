package fr.baptiste.main.application.processor;

import fr.baptiste.main.domain.Http;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Optional;

/**
 * This processor has no purpose for the moment. It is just an example of implementation.
 */
public class CyberSecurityProcessor implements Processor {
    @Override
    public Optional<Processor> getNext() {
        return Optional.empty();
    }

    @Override
    public boolean test(ConsumerRecord<String, Http> record) {
        return true;
    }
}
