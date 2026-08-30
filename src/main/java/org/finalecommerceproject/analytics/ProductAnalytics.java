package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class ProductAnalytics {

    /*
     * Best-Selling Product According to Category
     *
     * Products:
     * product_id, product_name, brand, department, category, price, rating
     *
     * Order_Items:
     * order_item_id, order_id, product_id, quantity,
     * selling_price, discount, final_price
     */

    public static void bestSellingProductByCategory(
            Dataset<Row> products,
            Dataset<Row> orderItems) {

        System.out.println("\n==============================================");
        System.out.println("     BEST-SELLING PRODUCT BY CATEGORY");
        System.out.println("==============================================");

        // Join Products and Order_Items using product_id
        Dataset<Row> productSales = products
                .join(
                        orderItems,
                        products.col("product_id")
                                .equalTo(orderItems.col("product_id")),
                        "inner"
                );

//        System.out.println("\n========== PRODUCT JOIN DEBUG ==========");
//
//        System.out.println("Products count: " + products.count());
//        System.out.println("Order Items count: " + orderItems.count());
//        System.out.println("Joined count: " + productSales.count());
//
//        System.out.println("\nProducts product_id:");
//        products.select("product_id").show(10, false);
//
//        System.out.println("\nOrder Items product_id:");
//        orderItems.select("product_id").show(10, false);
//
//        System.out.println("\nJoined sample:");
//        productSales.select(
//                products.col("product_id"),
//                products.col("product_name"),
//                products.col("category"),
//                orderItems.col("quantity")
//        ).show(10, false);

        // Calculate total quantity sold for each product
        Dataset<Row> productQuantity = productSales
                .groupBy(
                        products.col("category"),
                        products.col("product_id"),
                        products.col("product_name")
                )
                .agg(
                        sum(orderItems.col("quantity"))
                                .alias("total_quantity_sold")
                );

        // Rank products within each category
        WindowSpec categoryWindow = Window
                .partitionBy("category")
                .orderBy(
                        col("total_quantity_sold").desc()
                );

        // Select the best-selling product from each category
        Dataset<Row> bestSellingProducts = productQuantity
                .withColumn(
                        "rank",
                        row_number().over(categoryWindow)
                )
                .filter(
                        col("rank").equalTo(1)
                )
                .drop("rank")
                .orderBy("category");

        // Display result on console
        bestSellingProducts.show(false);
    }
}