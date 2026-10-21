
package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class CustomerLifetimeValueAnalytics {

    public static Dataset<Row> analyze(
            Dataset<Row> orders,
            Dataset<Row> customers) {

        // Step 1: Calculate spending from recorded order totals.
        Dataset<Row> customerSpending = orders
                .groupBy(col("customer_id"))
                .agg(
                        countDistinct(col("order_id")).alias("total_orders"),
                        sum(col("total_amount")).alias("customer_lifetime_value")
                );

        // Step 2: Start with all customers so customers without
        // orders are also included in the output.
        Dataset<Row> result = customers
                .select(
                        col("customer_id"),
                        col("customer_name")
                )
                .join(customerSpending, new String[]{"customer_id"}, "left");

        // Step 3: Give customers without orders zero values.
        result = result
                .withColumn(
                        "total_orders",
                        coalesce(col("total_orders"), lit(0))
                )
                .withColumn(
                        "customer_lifetime_value",
                        coalesce(col("customer_lifetime_value"), lit(0.0))
                );

        // Step 4: Calculate average order value safely.
        result = result.withColumn(
                "average_order_value",
                when(
                        col("total_orders").gt(0),
                        col("customer_lifetime_value")
                                .divide(col("total_orders"))
                ).otherwise(lit(0.0))
        );

        // Step 5: Assign a loyalty tier using CLV thresholds.

        result = result.withColumn(
                "loyalty_tier",
                when(col("customer_lifetime_value").geq(700000), lit("PLATINUM"))
                        .when(col("customer_lifetime_value").geq(300000), lit("GOLD"))
                        .when(col("customer_lifetime_value").geq(100000), lit("SILVER"))
                        .otherwise(lit("STANDARD"))
        );


        // Step 6: Assign the proposed discount for each tier.

        result = result.withColumn(
                "discount_percentage",
                when(col("customer_lifetime_value").geq(700000), lit(15.0))
                        .when(col("customer_lifetime_value").geq(300000), lit(10.0))
                        .when(col("customer_lifetime_value").geq(100000), lit(5.0))
                        .otherwise(lit(0.0))
        );


        // Step 7: Select and format the final output columns.
        return result.select(
                col("customer_id"),
                col("customer_name"),
                col("total_orders"),
                round(col("customer_lifetime_value"), 2)
                        .alias("customer_lifetime_value"),
                round(col("average_order_value"), 2)
                        .alias("average_order_value"),
                col("loyalty_tier"),
                col("discount_percentage")
        );
    }

    public static void displayCustomer(
            Dataset<Row> clvResults,
            String customerId) {

        // Find the requested customer ID.
        Dataset<Row> customer = clvResults.filter(
                col("customer_id").cast("string").equalTo(customerId)
        );

        // Show a message if the customer ID doesn't exist.
        if (customer.limit(1).count() == 0) {
            System.out.println(
                    "Customer ID not found: " + customerId
            );
            return;
        }

        System.out.println("\n===== CUSTOMER LIFETIME VALUE =====");
        customer.show(false);
    }
}
