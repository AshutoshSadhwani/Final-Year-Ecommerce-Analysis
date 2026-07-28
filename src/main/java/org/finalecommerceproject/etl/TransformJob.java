package org.finalecommerceproject.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class TransformJob {

    /*
        PRODUCTS TABLE
     */
    public static Dataset<Row> transformProducts(Dataset<Row> products) {

        return products

                // Remove duplicate products
                .dropDuplicates()

                // Remove rows having NULL values
                .na().drop()

                // Department in Upper Case
                .withColumn("department",
                        upper(col("department")))

                // Category in Upper Case
                .withColumn("category",
                        upper(col("category")))

                // Brand in Upper Case
                .withColumn("brand",
                        upper(col("brand")));
    }


    /*
        CUSTOMERS TABLE
     */
    public static Dataset<Row> transformCustomers(Dataset<Row> customers) {

        return customers

                .dropDuplicates()

                .na().drop()

                .withColumn("customer_name",
                        initcap(col("customer_name")))

                .withColumn("gender",
                        upper(col("gender")))

                .withColumn("state",
                        upper(col("state")))
                .withColumn(
                        "loyalty_scheme",
                        upper(col("loyalty_scheme"))
                );
        // when want to create new col as loyalty_flag
//        customers = customers.withColumn(
//                "loyalty_flag",
//                when(col("loyalty_scheme").equalTo("Gold"), "YES")
//                        .otherwise("NO")
//        );
    }


    /*
        ORDERS TABLE
     */
    public static Dataset<Row> transformOrders(Dataset<Row> orders) {

        return orders

                .dropDuplicates()

                .na().drop()

                // Convert order date into DateType
                .withColumn("order_date",
                        to_date(col("order_date"),
                                "yyyy-MM-dd"))

                // Month
                .withColumn("month",
                        month(col("order_date")))

                // Year
                .withColumn("year",
                        year(col("order_date")))

                // Quarter
                .withColumn("quarter",
                        quarter(col("order_date")));
    }


    /*
        REGIONS TABLE
     */
    public static Dataset<Row> transformRegions(Dataset<Row> regions) {

        return regions

                .dropDuplicates()

                .na().drop()

                .withColumn("state",
                        upper(col("state")))

                .withColumn("region",
                        upper(col("region")));
    }


    /*
        FESTIVALS TABLE
     */
    public static Dataset<Row> transformFestivals(Dataset<Row> festivals) {

        return festivals

                .dropDuplicates()

                .na().drop()

                .withColumn("festival_name",
                        initcap(col("event_name")))

                .withColumn("holiday_flag",
                        upper(col("holiday_flag")))

                .withColumn("start_date",
                        to_date(col("start_date"),
                                "yyyy-MM-dd"))

                .withColumn("end_date",
                        to_date(col("end_date"),
                                "yyyy-MM-dd"));
    }

    public static Dataset<Row> transformOrderItems(Dataset<Row> orderItems){

        return orderItems

                .dropDuplicates()

                .na().drop();

//                .withColumn(
//                        "total_price",
//                        col("price").multiply(col("quantity"))
//                );
    }

}