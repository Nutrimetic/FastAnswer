package fr.baptiste.main.application.port;

import java.io.Serializable;

/**
 * This class has the responsiblity to send a message.
 * Respect the Port design pattern in an Hexagonal architecture.
 * @param <K> The class of the key
 * @param <K> The class of the value
 */
public interface WriterStreamingPort<K, V> extends Serializable {
    /**
     * This method write a data in the kafka queue
     */
    public void write(K key, V value);
}
