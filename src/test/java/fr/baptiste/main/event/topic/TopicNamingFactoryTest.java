package fr.baptiste.main.event.topic;

import fr.baptiste.main.event.topic.TopicNamingFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TopicNamingFactoryTest {
    @Test
    public void itShouldBuildTopicName() {
        //GIVEN
        TopicNamingFactory topicNamingFactory = new TopicNamingFactory("companyTest", TopicNamingFactory.TopicRole.ANSWER);

        //WHEN
        String result = topicNamingFactory.buildTopicName();

        //THEN
        Assertions.assertThat(result).isEqualTo("companytest.answer");
    }
}