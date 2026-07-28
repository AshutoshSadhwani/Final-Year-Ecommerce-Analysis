package org.finalecommerceproject.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WriteData {

    private static final String URL =
            "jdbc:oracle:thin:@localhost:1521:xe";

    private static final String USER = "system";
    private static final String PASSWORD = "{Db_Password}";
    private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";

    public static void loadProducts(Dataset<Row> df){

        df.write()
                .format("jdbc")
                .option("url",URL)
                .option("driver",DRIVER)
                .option("dbtable","PRODUCTS")
                .option("user",USER)
                .option("password",PASSWORD)
                .mode("overwrite")
                .save();

        System.out.println("Products Loaded.");
    }

    public static void loadCustomers(Dataset<Row> df){

        df.write()
                .format("jdbc")
                .option("url",URL)
                .option("driver",DRIVER)
                .option("dbtable","CUSTOMERS")
                .option("user",USER)
                .option("password",PASSWORD)
                .mode("overwrite")
                .save();

        System.out.println("Customers Loaded.");
    }

    public static void loadOrders(Dataset<Row> df){

        df.write()
                .format("jdbc")
                .option("url",URL)
                .option("driver",DRIVER)
                .option("dbtable","ORDERS")
                .option("user",USER)
                .option("password",PASSWORD)
                .mode("overwrite")
                .save();

        System.out.println("Orders Loaded.");
    }

    public static void loadRegions(Dataset<Row> df){

        df.write()
                .format("jdbc")
                .option("url",URL)
                .option("driver",DRIVER)
                .option("dbtable","REGIONS")
                .option("user",USER)
                .option("password",PASSWORD)
                .mode("overwrite")
                .save();

        System.out.println("Regions Loaded.");
    }

    public static void loadFestivals(Dataset<Row> df){

        df.write()
                .format("jdbc")
                .option("url",URL)
                .option("driver",DRIVER)
                .option("dbtable","FESTIVALS")
                .option("user",USER)
                .option("password",PASSWORD)
                .mode("overwrite")
                .save();

        System.out.println("Festivals Loaded.");
    }
    public static void loadOrderItems(Dataset<Row> df){

        df.write()

                .format("jdbc")

                .option("url",URL)

                .option("driver",DRIVER)

                .option("dbtable","ORDER_ITEMS")

                .option("user",USER)

                .option("password",PASSWORD)

                .mode("overwrite")

                .save();

        System.out.println("Order Items Loaded.");
    }

}