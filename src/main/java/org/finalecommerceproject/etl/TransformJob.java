package org.finalecommerceproject.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
                        upper(col("state")));
    }


    /*
        ORDERS TABLE
     */
    public static Dataset<Row> transformOrders(Dataset<Row> orders) {

        return orders

                .dropDuplicates()

                .na().drop()

                .withColumn(
                        "order_date",
                        to_date(
                                col("order_date"),
                                "yyyy-MM-dd"
                        )
                )

                .withColumn(
                        "delivery_state",
                        upper(col("delivery_state"))
                )

                .withColumn(
                        "month",
                        month(col("order_date"))
                )

                .withColumn(
                        "year",
                        year(col("order_date"))
                )

                .withColumn(
                        "quarter",
                        quarter(col("order_date"))
                );
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
    public static Dataset<Row> transformFestivals(Dataset<Row> holidayDF) {

        Dataset<Row> festivals = holidayDF
                .dropDuplicates("event_id", "event_name", "start_date", "end_date")
                .na().drop("all")
                .withColumn("festival_name", functions.initcap(functions.col("event_name")))
                .select(
                        functions.col("event_id"),
                        functions.col("event_name"),
                        functions.col("start_date"),
                        functions.col("end_date"),
                        functions.col("festival_name")
                );
//        return holidayDF
//                .dropDuplicates()
//                .filter(
//                        col("event_id").isNotNull()
//                                .and(col("event_name").isNotNull())
//                                .and(col("start_date").isNotNull())
//                                .and(col("end_date").isNotNull())
//                )
//                .withColumn("festival_name", initcap(col("event_name")));
        return festivals;
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