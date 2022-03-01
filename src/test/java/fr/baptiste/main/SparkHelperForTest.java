package fr.baptiste.main;

import fr.baptiste.main.utility.SparkConfiguration;
import fr.baptiste.main.utility.SparkHelper;
import org.apache.spark.streaming.Durations;

import java.util.Collections;

public class SparkHelperForTest {
    private static SparkHelper sparkHelper;

    public static SparkHelper getSparkHelper() {
        if(sparkHelper == null) {
            sparkHelper = new SparkHelper(new SparkConfiguration("test", "local[*]", Collections.emptyMap()), Durations.seconds(180L));
        }
        return sparkHelper;
    }
}
