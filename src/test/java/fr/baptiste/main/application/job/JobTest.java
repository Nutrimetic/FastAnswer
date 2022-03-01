package fr.baptiste.main.application.job;

import fr.baptiste.main.application.port.ReaderStreamingPort;
import fr.baptiste.main.application.port.WriterStreamingPort;
import fr.baptiste.main.application.processor.Processor;
import fr.baptiste.main.domain.Http;
import fr.baptiste.main.domain.Method;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JobTest {

    @Test
    public void itShouldApplyTheProcessorsToTheRdd() {
        //GIVEN
        final ReaderStreamingPort<ConsumerRecord<String, Http>> readerStreamingPort = Mockito.mock(ReaderStreamingPort.class);
        final WriterStreamingPort<String, Http> writerStreamingPort = Mockito.mock(WriterStreamingPort.class);
        final Processor alwaysReturnFalse = getProcessorAlwaysReturnFalse();
        final Processor validateMethodIsGet = getProcessorValidateMethodIsGet(alwaysReturnFalse);

        JavaInputDStream javaInputDStream = Mockito.mock(JavaInputDStream.class);

        //WHEN
        Mockito.when(readerStreamingPort.build()).thenReturn(javaInputDStream);
        new Job(readerStreamingPort, writerStreamingPort, validateMethodIsGet).execute();

        //THEN
        verify(javaInputDStream, times(1)).foreachRDD(any(VoidFunction.class));
    }

    private Processor getProcessorValidateMethodIsGet(Processor alwaysReturnFalse) {
        return new Processor() {
            @Override
            public Optional<Processor> getNext() {
                return Optional.of(alwaysReturnFalse);
            }

            @Override
            public boolean test(ConsumerRecord<String, Http> record) {
                if (record.value().getMethod() != Method.GET) {
                    return true;
                }
                if (getNext().isPresent()) {
                    return getNext().get().test(record);
                }
                return false;
            }
        };
    }

    private Processor getProcessorAlwaysReturnFalse() {
        return new Processor() {
            @Override
            public Optional<Processor> getNext() {
                return Optional.empty();
            }

            @Override
            public boolean test(ConsumerRecord<String, Http> record) {
                return false;
            }
        };
    }

}