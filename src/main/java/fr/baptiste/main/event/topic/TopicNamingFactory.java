package fr.baptiste.main.event.topic;

/**
 * This class has the responsibility of creating the topic name through a certain number of parameters like
 * companyName, topicRole, etc. All topics created should respect a norm defined in this class.
 *
 * Exemple : companynametest.request
 */
public class TopicNamingFactory {
    private final String companyName;
    private final TopicRole topicRole;

    public TopicNamingFactory(String companyName, TopicRole topicRole) {
        this.companyName = companyName;
        this.topicRole = topicRole;
    }

    public String buildTopicName() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(companyName.toLowerCase());
        stringBuilder.append(".");
        stringBuilder.append(topicRole.name().toLowerCase());

        return stringBuilder.toString();
    }

    public enum TopicRole {
       REQUEST,
       ANSWER;
   }
}
