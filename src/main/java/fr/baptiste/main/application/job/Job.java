package fr.baptiste.main.application.job;

import fr.baptiste.main.application.port.ReaderStreamingPort;
import fr.baptiste.main.application.port.WriterStreamingPort;
import fr.baptiste.main.application.processor.Processor;
import fr.baptiste.main.domain.Http;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * This class has the responsibility to receive Http message from the Kafka queue and apply the chain of processor
 * on every message. The message from malware will be send back in a kafka queue to allow the sending program
 * to filter thoses messages.
 */
public class Job implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(Job.class);

    private final ReaderStreamingPort<ConsumerRecord<String, Http>> readerStreamingPort;
    private final WriterStreamingPort<String, Http> writerStreamingPort;
    private final Processor processor;

    public Job(ReaderStreamingPort<ConsumerRecord<String, Http>> readerStreamingPort,
               WriterStreamingPort<String, Http> writerStreamingPort, Processor processor) {
        this.readerStreamingPort = readerStreamingPort;
        this.writerStreamingPort = writerStreamingPort;
        this.processor = processor;
    }

    public void execute() {
        JavaInputDStream<ConsumerRecord<String, Http>> javaInputDStream = readerStreamingPort.build();
        javaInputDStream.foreachRDD(rdd -> //we loop through all the micro rdd in the Dstream
            rdd.foreach(record -> { //we loop through all the record in a RDD
                if(!processor.test(record)) {
                    //we only write on the queue the http message from malware
                    writerStreamingPort.write(record.key(), record.value());
                }
            })
        );
    }
}
