
package org.finalecommerceproject;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.finalecommerceproject.analytics.CustomerLifetimeValueAnalytics;
import org.finalecommerceproject.analytics.ProductAnalytics;
import org.finalecommerceproject.analytics.RegionAnalytics;
import org.finalecommerceproject.analytics.SeasonalityAnalytics;
import org.finalecommerceproject.etl.WriteData;

import java.util.Scanner;

public class AnalyticsMenu {

    public static void show(
            Dataset<Row> products,
            Dataset<Row> customers,
            Dataset<Row> orders,
            Dataset<Row> orderItems,
            Dataset<Row> stateRegion,
            Dataset<Row> holidayCalendar) {

        Scanner scanner = new Scanner(System.in);

        // Calculate and save CLV results once before customer lookups.
        Dataset<Row> clvResults =
                CustomerLifetimeValueAnalytics.analyze(orders, customers);

        WriteData.loadCustomerLifetimeValue(clvResults);

        boolean running = true;

        while (running) {
            System.out.println("\n===== E-COMMERCE ANALYTICS MENU =====");
            System.out.println("1. Customer Lifetime Value");
            System.out.println("2. Best-Selling Product by Category");
            System.out.println("3. Region-wise Sales by State");
            System.out.println("4. Monthly Sales Trend");
            System.out.println("5. Quarterly Sales Trend");
            System.out.println("6. Holiday Sales Impact");
            System.out.println("0. Exit");
            System.out.print("Enter your choice: ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    System.out.print("Enter Customer ID: ");
                    String customerId = scanner.nextLine().trim();

                    CustomerLifetimeValueAnalytics.displayCustomer(
                            clvResults, customerId);
                    break;

                case "2":
                    ProductAnalytics.bestSellingProductByCategory(
                            products, orderItems);
                    break;

                case "3":
                    RegionAnalytics.regionWiseSalesByState(
                            orders, stateRegion);
                    break;

                case "4": {
                    SeasonalityAnalytics seasonality =
                            new SeasonalityAnalytics(orders, holidayCalendar);

                    System.out.println("\n===== MONTHLY SALES TREND =====");
                    seasonality.monthlySalesTrend().show(12, false);
                    break;
                }

                case "5": {
                    SeasonalityAnalytics seasonality =
                            new SeasonalityAnalytics(orders, holidayCalendar);

                    System.out.println("\n===== QUARTERLY SALES TREND =====");
                    seasonality.quarterlySalesTrend().show(false);
                    break;
                }

                case "6": {
                    SeasonalityAnalytics seasonality =
                            new SeasonalityAnalytics(orders, holidayCalendar);

                    System.out.println("\n===== HOLIDAY SALES IMPACT =====");
                    seasonality.holidaySalesImpact().show(20, false);
                    break;
                }

                case "0":
                    running = false;
                    System.out.println("Exiting analytics menu. Goodbye!");
                    break;

                default:
                    System.out.println(
                            "Invalid choice. Please select an option from 0 to 6."
                    );
            }
        }
    }
}
