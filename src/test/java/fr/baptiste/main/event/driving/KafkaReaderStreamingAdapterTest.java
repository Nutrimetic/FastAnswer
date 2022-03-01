package fr.baptiste.main.event.driving;

import fr.baptiste.main.SparkHelperForTest;
import fr.baptiste.main.event.KafkaConfiguration;
import fr.baptiste.main.utility.SparkHelper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaReaderStreamingAdapterTest {

    private static SparkHelper sparkHelper;

    @BeforeClass
    public static void setUp() {
        sparkHelper = SparkHelperForTest.getSparkHelper();
    }

    @Test
    public void itShouldBuildAValidateJavaInputDStream() {
        //GIVEN
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        KafkaConfiguration kafkaConfiguration = new KafkaConfiguration("topic0", kafkaParams);

        //WHEN
        KafkaReaderStreamingAdapter kafkaReaderStreaming = new KafkaReaderStreamingAdapter(sparkHelper, kafkaConfiguration);
        Object result = kafkaReaderStreaming.build();

        //THEN
        Assertions.assertThat(result).isInstanceOf(JavaInputDStream.class);
    }

}