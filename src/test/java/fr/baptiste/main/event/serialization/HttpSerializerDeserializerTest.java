package fr.baptiste.main.event.serialization;

import fr.baptiste.main.domain.Http;
import fr.baptiste.main.domain.Method;
import fr.baptiste.main.event.deserialization.HttpDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class HttpSerializerDeserializerTest {

    @Test
    public void itShouldSerializeAndDeserializeAnHttpObject() {
        //GIVEN
        final Http http = new Http("13.66.139.0",
                "19/Dec/2020:13:57:26 +0100",
                Method.GET,
                "/index.php?option=com_phocagallery&view=category&id=1:almhuette-raith&Itemid=53",
                200,
                "32653",
                "-",
                "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)");
        final HttpSerializer httpSerializer = new HttpSerializer();
        final HttpDeserializer httpDeserializer = new HttpDeserializer();

        //WHEN
        byte[] data = httpSerializer.serialize("topic", http);
        Http result = httpDeserializer.deserialize("topic", data);

        //THEN
        Assertions.assertThat(result).isEqualTo(http);
    }

}