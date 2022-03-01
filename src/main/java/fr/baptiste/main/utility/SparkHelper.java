package fr.baptiste.main.utility;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

/**
 * This class has the responsibility to implement the SparkSession (the application should instanciate one and only one
 * SparkSession). This class contain the configuration for the SparkSession
 */
public class SparkHelper implements Serializable {
    protected SparkConfiguration sparkConfiguration;

    protected SparkSession sparkSession;
    protected SparkConf sparkConf;
    protected JavaStreamingContext javaStreamingContext;

    private final Duration duration;

    public SparkHelper(SparkConfiguration sparkConfiguration, Duration duration) {
        this.sparkConfiguration = sparkConfiguration;
        this.duration = duration;
    }

    public SparkSession getSparkSession() {
        if (sparkSession == null) {
            sparkSession = getOrCreate();
        }
        return sparkSession;
    }

    public SparkConf getSparkConf() {
        if (this.sparkConf == null) {
            this.sparkConf = new SparkConf().setMaster(sparkConfiguration.getMaster()).setAppName(sparkConfiguration.getApplicationName());
            if (sparkConfiguration.getConfValues() != null) {
                sparkConfiguration.getConfValues().forEach((key, value) -> sparkConf.set(key, value));
            }
        }
        return sparkConf;
    }

    private SparkSession getOrCreate() {
        return SparkSession
                .builder()
                .appName(sparkConfiguration.getApplicationName()) //set the name of application which will be show in the Spark web ui
                .config(getSparkConf())
                //set the url to connect to.
                //Exemple : local to run locally, local[4] ro run locally with 4 core or directly the spark cluster url
                .master(sparkConfiguration.getMaster())
                .getOrCreate();
    }

    public JavaStreamingContext getStreamingContext() {
        if (javaStreamingContext == null) {
            javaStreamingContext = new JavaStreamingContext(getSparkConf(), duration);
        }
        return javaStreamingContext;
    }
}
