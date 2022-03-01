package fr.baptiste.main.utility;

import fr.baptiste.main.SparkHelperForTest;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkHelperTest {

    private static SparkHelper sparkHelper;

    @BeforeClass
    public static void setUp() {
        sparkHelper = SparkHelperForTest.getSparkHelper();
    }

    @Test
    public void itShouldCorrectlyInstanciateASparkSession() {
        //GIVEN

        //WHEN
        Object sparkSession = sparkHelper.getSparkSession();

        //THEN
        Assertions.assertThat(sparkSession).isExactlyInstanceOf(SparkSession.class);
    }

    @Test
    public void itShouldCorrectlyInstanciateASparkConf() {
        //GIVEN

        //WHEN
        Object sparkConf = sparkHelper.getSparkConf();

        //THEN
        Assertions.assertThat(sparkConf).isExactlyInstanceOf(SparkConf.class);
    }
}