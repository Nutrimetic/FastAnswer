package fr.baptiste.main.event.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.baptiste.main.domain.Http;
import fr.baptiste.main.event.exception.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HttpDeserializer implements Deserializer<Http> {
    private static final Logger log = LoggerFactory.getLogger(HttpDeserializer.class);
    private ObjectMapper objectMapper = new ObjectMapper();

    //used to implement configuration details - no need here
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Http deserialize(String s, byte[] bytes) {
        try {
            if (bytes == null) {
                log.info("trying to deserialize null value");
                return null;
            }
            return objectMapper.readValue(bytes, Http.class);
        } catch (Exception e) {
            throw new DeserializationException("Error during the deserialization of an http object", e);
        }
    }

    //use to dispose of some resources - no need here
    @Override
    public void close() {

    }
}
