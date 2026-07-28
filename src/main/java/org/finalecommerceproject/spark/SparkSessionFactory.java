package org.finalecommerceproject.spark;

import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

    private static SparkSession sparkSession;

    private SparkSessionFactory() {
    }

    public static SparkSession getSession() {

        if (sparkSession == null) {

            sparkSession = SparkSession.builder()
                    .appName("Ecommerce Data Engineering Project")
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "4")
                    .config("spark.driver.memory", "2g")
                    .config("spark.executor.memory", "2g")
                    .getOrCreate();

            sparkSession.sparkContext().setLogLevel("ERROR");
        }

        return sparkSession;
    }

    public static void stopSession() {

        if (sparkSession != null) {
            sparkSession.stop();
            sparkSession = null;
        }
    }
}