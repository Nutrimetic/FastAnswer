package fr.baptiste.main;

import fr.baptiste.main.application.job.Job;
import fr.baptiste.main.application.port.ReaderStreamingPort;
import fr.baptiste.main.application.port.WriterStreamingPort;
import fr.baptiste.main.application.processor.CyberSecurityProcessor;
import fr.baptiste.main.domain.Http;
import fr.baptiste.main.event.KafkaConfiguration;
import fr.baptiste.main.event.KafkaParamsGenerator;
import fr.baptiste.main.event.deserialization.HttpDeserializer;
import fr.baptiste.main.event.driven.KafkaWriterStreamingAdapter;
import fr.baptiste.main.event.serialization.HttpSerializer;
import fr.baptiste.main.event.topic.TopicNamingFactory;
import fr.baptiste.main.event.driving.KafkaReaderStreamingAdapter;
import fr.baptiste.main.utility.SparkConfiguration;
import fr.baptiste.main.utility.SparkHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import java.util.Collections;

public class Main {
    public static void main(String[] args) {
        //  configuration
        //Kafka - reading
        final TopicNamingFactory topicNamingFactoryRequest = new TopicNamingFactory("companyTest", TopicNamingFactory.TopicRole.REQUEST);
        final KafkaParamsGenerator kafkaParamsGeneratorReader = new KafkaParamsGenerator("todo", 1, 1, 500, true);
        final KafkaConfiguration kafkaConfigurationReader = new KafkaConfiguration(topicNamingFactoryRequest.buildTopicName(), kafkaParamsGeneratorReader.buildKafkaParamsConsumer(StringDeserializer.class, HttpDeserializer.class, "groupId"));

        //Kafka - writing
        final TopicNamingFactory topicNamingFactoryAnswer = new TopicNamingFactory("companyTest", TopicNamingFactory.TopicRole.ANSWER);
        final KafkaParamsGenerator kafkaParamsGeneratorWriter = new KafkaParamsGenerator("todo", 1, 1, 500, true);
        final KafkaConfiguration kafkaConfigurationWriter = new KafkaConfiguration(topicNamingFactoryAnswer.buildTopicName(), kafkaParamsGeneratorWriter.buildKafkaParamsProducer(StringSerializer.class, HttpSerializer.class));

        //Spark
        final SparkConfiguration sparkConfiguration = new SparkConfiguration("fastAnswer", "local[*]", Collections.emptyMap());
        final Duration seconds = Durations.seconds(1L);

        //instanciation of Spark objects
        final SparkHelper sparkHelper = new SparkHelper(sparkConfiguration, seconds);

        //instanciation of Kafka objects
        final ReaderStreamingPort<ConsumerRecord<String, Http>> readerStreamingPort = new KafkaReaderStreamingAdapter(sparkHelper, kafkaConfigurationReader);
        final WriterStreamingPort<String, Http> writerStreamingPort = new KafkaWriterStreamingAdapter(kafkaConfigurationWriter);

        //instanciation of the processors
        final CyberSecurityProcessor cyberSecurityProcessor = new CyberSecurityProcessor();

        //instanciation of the job
        final Job job = new Job(readerStreamingPort, writerStreamingPort, cyberSecurityProcessor);

        job.execute();
    }
}
