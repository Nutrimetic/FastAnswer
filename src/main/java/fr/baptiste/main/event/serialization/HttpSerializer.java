package fr.baptiste.main.event.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.baptiste.main.domain.Http;
import fr.baptiste.main.event.exception.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class HttpSerializer implements Serializer<Http> {
    private static final Logger log = LoggerFactory.getLogger(HttpSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    //used to implement configuration details - no need here
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Http http) {
        if (http == null) {
            log.info("trying to serialize null value");
            return null;
        }
        try {
            return objectMapper.writeValueAsString(http).getBytes();
        } catch (JsonProcessingException e) {
            throw new SerializationException(String.format("Error during the serialization of the following http object : %s", http), e);
        }
    }

    //use to dispose of some resources - no need here
    @Override
    public void close() {

    }
}
