package fr.baptiste.main.event.driven;

import fr.baptiste.main.domain.Http;
import fr.baptiste.main.event.KafkaConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.*;

public class KafkaWriterStreamingAdapterTest {

    @Test
    public void itShouldCorrecltyCallSend() {
        //GIVEN
        String topic = "topic";
        final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration(topic, Collections.emptyMap());
        final KafkaProducer<String, Http> kafkaProducer = Mockito.mock(KafkaProducer.class);
        final Http http = Mockito.mock(Http.class);

        //WHEN
        new KafkaWriterStreamingAdapterMock(kafkaConfiguration, kafkaProducer).write("key", http);

        //THEN
        Mockito.verify(kafkaProducer, Mockito.times(1))
                .send(new ProducerRecord<String, Http>(topic, "key", http));
    }


    public class KafkaWriterStreamingAdapterMock extends KafkaWriterStreamingAdapter {

        public KafkaWriterStreamingAdapterMock(KafkaConfiguration kafkaConfiguration, KafkaProducer<String, Http> producer) {
            super(kafkaConfiguration);
            this.producer = producer;
        }

        protected KafkaProducer<String, Http> getProducer() {
            return this.producer;
        }
    }
}