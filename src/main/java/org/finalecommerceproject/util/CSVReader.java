package org.finalecommerceproject.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVReader {

    private CSVReader() {
    }

    public static Dataset<Row> readCSV(SparkSession spark, String fileName) {

        return spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/" + fileName);

    }

}