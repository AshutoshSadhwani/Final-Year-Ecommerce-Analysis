package org.finalecommerceproject.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriteData {

    private static final String URL =
            "jdbc:mysql://localhost:3307/ecommerce_analysis";

    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    // Common method to load data into MySQL
    private static void writeData(
            Dataset<Row> df,
            String tableName) {

        try {
            df.write()
                    .format("jdbc")
                    .option("url", URL)
                    .option("driver", DRIVER)
                    .option("dbtable", tableName)
                    .option("user", USER)
                    .option("password", PASSWORD)
                    .mode("overwrite")
                    .save();

            System.out.println(
                    tableName + " Loaded Successfully into MySQL."
            );

        } catch (Exception e) {

            System.err.println(
                    "Error loading " + tableName + " into MySQL."
            );

            e.printStackTrace();

            throw new RuntimeException(
                    "Failed to load table: " + tableName, e
            );
        }
    }


    // Load Products
    public static void loadProducts(Dataset<Row> df) {
        writeData(df, "PRODUCTS");
    }


    // Load Customers
    public static void loadCustomers(Dataset<Row> df) {
        writeData(df, "CUSTOMERS");
    }


    // Load Orders
    public static void loadOrders(Dataset<Row> df) {
        writeData(df, "ORDERS");
    }


    // Load Order Items
    public static void loadOrderItems(Dataset<Row> df) {
        writeData(df, "ORDER_ITEMS");
    }


    // Load State and Region data
    public static void loadRegions(Dataset<Row> df) {
        writeData(df, "REGIONS");
    }


    // Load Festival and Holiday data
    public static void loadFestivals(Dataset<Row> df) {
        writeData(df, "FESTIVALS");
    }




}