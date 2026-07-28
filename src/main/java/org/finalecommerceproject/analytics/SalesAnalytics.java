package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class SalesAnalytics {

    /*
     * Total Revenue
     */
    public static void totalRevenue(Dataset<Row> orders){

        System.out.println("\n========== TOTAL REVENUE ==========");

        orders.select(
                        sum("total_amount")
                                .alias("Total Revenue"))
                .show(false);
    }

    /*
     * Total Orders
     */
    public static void totalOrders(Dataset<Row> orders){

        System.out.println("\n========== TOTAL ORDERS ==========");

        orders.select(
                        count("order_id")
                                .alias("Total Orders"))
                .show(false);
    }

    /*
     * Average Order Value
     */
    public static void averageOrderValue(Dataset<Row> orders){

        System.out.println("\n========== AVERAGE ORDER VALUE ==========");

        orders.select(
                        round(avg("total_amount"),2)
                                .alias("Average Order Value"))
                .show(false);
    }

    /*
     * Monthly Revenue
     */
    public static void monthlySales(Dataset<Row> orders){

        System.out.println("\n========== MONTHLY SALES ==========");

        orders.groupBy("month")
                .agg(round(sum("total_amount"),2)
                        .alias("Revenue"))
                .orderBy("month")
                .show(false);
    }

    /*
     * Yearly Revenue
     */
    public static void yearlySales(Dataset<Row> orders){

        System.out.println("\n========== YEARLY SALES ==========");

        orders.groupBy("year")
                .agg(round(sum("total_amount"),2)
                        .alias("Revenue"))
                .orderBy("year")
                .show(false);
    }

    /*
     * Quarter Wise Revenue
     */
    public static void quarterSales(Dataset<Row> orders){

        System.out.println("\n========== QUARTER SALES ==========");

        orders.groupBy("quarter")
                .agg(round(sum("total_amount"),2)
                        .alias("Revenue"))
                .orderBy("quarter")
                .show(false);
    }

}