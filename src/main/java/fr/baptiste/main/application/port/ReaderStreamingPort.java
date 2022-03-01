package fr.baptiste.main.application.port;

import org.apache.spark.streaming.api.java.JavaInputDStream;

import java.io.Serializable;

/**
 * This class has the responsiblity to provide a JavaInputDStream, which is the spark object to read data in streaming.
 * Respect the Port design pattern in an Hexagonal architecture.
 * @param <K> The class of object to be received
 */
public interface ReaderStreamingPort<K> extends Serializable {
    /**
     * This method provide a JavaInputDStream
     * @return JavaInputDStream
     */
    JavaInputDStream<K> build();
}
