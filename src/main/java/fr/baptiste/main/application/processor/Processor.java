package fr.baptiste.main.application.processor;

import fr.baptiste.main.domain.Http;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * The processor are responsible for the all the cybersecurity actions.
 *
 * The Processor follow the design pattern Chain of Responsibility. Each processor will call the next processor if the
 * message is considered valid. If at any point in the chain, a processor decide that a message is not correct, then
 * the processor should return false.
 * Every processor should be a singleton.
 */
public interface Processor extends Predicate<ConsumerRecord<String, Http>>, Serializable {
    Optional<Processor> getNext();
    @Override
    boolean test(ConsumerRecord<String, Http> record);
}
