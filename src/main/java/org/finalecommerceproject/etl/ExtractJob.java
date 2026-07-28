package org.finalecommerceproject.etl;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.finalecommerceproject.spark.SparkSessionFactory;
import org.finalecommerceproject.util.CSVReader;


public class ExtractJob {

    public static Dataset<Row> products;
    public static Dataset<Row> customers;
    public static Dataset<Row> orders;
    public static Dataset<Row> orderItems;
    public static Dataset<Row> holidayCalendar;
    public static Dataset<Row> stateRegion;

    public static void execute() {

        SparkSession spark = SparkSessionFactory.getSession();

        System.out.println("\n========== EXTRACT PHASE ==========\n");

        products = CSVReader.readCSV(spark, "Products.csv");
        customers = CSVReader.readCSV(spark, "Customers.csv");
        orders = CSVReader.readCSV(spark, "Orders.csv");
        orderItems = CSVReader.readCSV(spark, "Order_Items.csv");
        holidayCalendar = CSVReader.readCSV(spark, "Holiday_Calendar.csv");
        stateRegion = CSVReader.readCSV(spark, "State_Region.csv");

        System.out.println("Products Records : " + products.count());
        System.out.println("Customers Records : " + customers.count());
        System.out.println("Orders Records : " + orders.count());
        System.out.println("Order Items Records : " + orderItems.count());
        System.out.println("Holiday Records : " + holidayCalendar.count());
        System.out.println("State Records : " + stateRegion.count());

        System.out.println("\nExtraction Completed Successfully.\n");
    }

}