
package org.finalecommerceproject.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriteData {

    // Read database configuration from environment variables
    private static final String URL =
            System.getenv("DB_URL");

    private static final String USER =
            System.getenv("DB_USER");

    private static final String PASSWORD =
            System.getenv("DB_PASSWORD");

    private static final String DRIVER =
            "com.mysql.cj.jdbc.Driver";

    // Common method to load data into MySQL
    private static void writeData(
            Dataset<Row> df,
            String tableName) {

        // Validate environment variables
        if (URL == null || USER == null || PASSWORD == null) {
            throw new IllegalStateException(
                    "Missing database environment variables. "
                            + "Please set DB_URL, DB_USER and DB_PASSWORD."
            );
        }

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
