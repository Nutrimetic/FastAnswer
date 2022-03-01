package fr.baptiste.main.utility;

import java.io.Serializable;
import java.util.Map;

public class SparkConfiguration implements Serializable {
    private final String applicationName;
    private final String master;
    private final Map<String, String> confValues;

    public SparkConfiguration(String applicationName, String master, Map<String, String> confValues) {
        this.applicationName = applicationName;
        this.master = master;
        this.confValues = confValues;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getMaster() {
        return master;
    }

    public Map<String, String> getConfValues() {
        return confValues;
    }
}
