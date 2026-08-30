package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class RegionAnalytics {

    /*
     * Region-Wise Sales According to States
     *
     * Orders:
     * order_id, customer_id, order_date, payment_method,
     * delivery_city, delivery_state, total_amount
     *
     * State_Region:
     * state, region
     */

    public static void regionWiseSalesByState(
            Dataset<Row> orders,
            Dataset<Row> stateRegion) {

        System.out.println("\n==============================================");
        System.out.println("        REGION-WISE SALES BY STATE");
        System.out.println("==============================================");

        // Join Orders with State_Region
        Dataset<Row> salesWithRegion = orders
                .join(
                        stateRegion,
                        orders.col("delivery_state")
                                .equalTo(stateRegion.col("state")),
                        "left"
                );

        // Calculate sales according to region and state
        Dataset<Row> regionWiseSales = salesWithRegion
                .groupBy(
                        stateRegion.col("region"),
                        orders.col("delivery_state")
                )
                .agg(
                        countDistinct(
                                orders.col("order_id")
                        ).alias("total_orders"),

                        round(
                                sum(orders.col("total_amount")),
                                2
                        ).alias("total_sales")
                )
                .withColumnRenamed(
                        "delivery_state",
                        "state"
                )
                .orderBy(
                        col("region"),
                        col("total_sales").desc()
                );

        // Display result on console
        regionWiseSales.show(false);
    }
}