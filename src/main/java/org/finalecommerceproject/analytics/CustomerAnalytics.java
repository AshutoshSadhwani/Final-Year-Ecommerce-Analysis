package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class CustomerAnalytics {

    /*
        Total Customers
     */
    public static void totalCustomers(Dataset<Row> customers){

        System.out.println("\n========== TOTAL CUSTOMERS ==========");

        customers.select(count("customer_id")
                        .alias("Total Customers"))
                .show(false);
    }

    /*
        Gender Distribution
     */
    public static void genderDistribution(Dataset<Row> customers){

        System.out.println("\n========== GENDER DISTRIBUTION ==========");

        customers.groupBy("gender")
                .count()
                .orderBy(desc("count"))
                .show(false);
    }

    /*
        State Wise Customers
     */
    public static void stateWiseCustomers(Dataset<Row> customers){

        System.out.println("\n========== STATE WISE CUSTOMERS ==========");

        customers.groupBy("state")
                .count()
                .orderBy(desc("count"))
                .show(false);
    }

    /*
        Top Customers
     */
    public static void topCustomers(Dataset<Row> orders){

        System.out.println("\n========== TOP CUSTOMERS ==========");

        orders.groupBy("customer_id")
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Total Spending")
                )
                .orderBy(desc("Total Spending"))
                .limit(10)
                .show(false);
    }

    /*
        Repeat Customers
     */
    public static void repeatCustomers(Dataset<Row> orders){

        System.out.println("\n========== REPEAT CUSTOMERS ==========");

        orders.groupBy("customer_id")
                .count()
                .filter(col("count").gt(1))
                .orderBy(desc("count"))
                .show(false);
    }

}