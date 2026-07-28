package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class ProductAnalytics {

    /*
     * Department Wise Revenue
     */
    public static void departmentRevenue(Dataset<Row> orders){

        System.out.println("\n========== DEPARTMENT REVENUE ==========");

        orders.groupBy("department")
                .agg(round(sum("total_amount"),2)
                        .alias("Revenue"))
                .orderBy(desc("Revenue"))
                .show(false);
    }

    /*
     * Category Wise Revenue
     */
    public static void categoryRevenue(Dataset<Row> orders){

        System.out.println("\n========== CATEGORY REVENUE ==========");

        orders.groupBy("category")
                .agg(round(sum("total_amount"),2)
                        .alias("Revenue"))
                .orderBy(desc("Revenue"))
                .show(false);
    }

    /*
     * Brand Wise Revenue
     */
    public static void brandRevenue(Dataset<Row> orders){

        System.out.println("\n========== BRAND REVENUE ==========");

        orders.groupBy("brand")
                .agg(round(sum("total_amount"),2)
                        .alias("Revenue"))
                .orderBy(desc("Revenue"))
                .show(false);
    }

    /*
     * Top 10 Selling Products
     */
    public static void topSellingProducts(Dataset<Row> orders){

        System.out.println("\n========== TOP SELLING PRODUCTS ==========");

        orders.groupBy("product_name")
                .agg(sum("quantity")
                        .alias("Quantity Sold"))
                .orderBy(desc("Quantity Sold"))
                .limit(10)
                .show(false);
    }

    /*
     * Lowest Selling Products
     */
    public static void leastSellingProducts(Dataset<Row> orders){

        System.out.println("\n========== LOWEST SELLING PRODUCTS ==========");

        orders.groupBy("product_name")
                .agg(sum("quantity")
                        .alias("Quantity Sold"))
                .orderBy("Quantity Sold")
                .limit(10)
                .show(false);
    }

    /*
     * Average Product Rating
     */
    public static void productRatings(Dataset<Row> orders){

        System.out.println("\n========== PRODUCT RATINGS ==========");

        orders.groupBy("product_name")
                .agg(round(avg("rating"),2)
                        .alias("Average Rating"))
                .orderBy(desc("Average Rating"))
                .show(false);
    }

}