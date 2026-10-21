
package org.finalecommerceproject;

import org.finalecommerceproject.etl.ExtractJob;
import org.finalecommerceproject.etl.TransformJob;
import org.finalecommerceproject.etl.WriteData;
import org.finalecommerceproject.analytics.CustomerLifetimeValueAnalytics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FinalEcommerceAnalysisApplication {

    public static void main(String[] args) {

        // ==============================
        // EXTRACT
        // ==============================
        ExtractJob.execute();

        // ==============================
        // TRANSFORM
        // ==============================
        ExtractJob.products =
                TransformJob.transformProducts(ExtractJob.products);

        ExtractJob.customers =
                TransformJob.transformCustomers(ExtractJob.customers);

        ExtractJob.orders =
                TransformJob.transformOrders(ExtractJob.orders);

        ExtractJob.orderItems =
                TransformJob.transformOrderItems(ExtractJob.orderItems);

        ExtractJob.stateRegion =
                TransformJob.transformRegions(ExtractJob.stateRegion);

        ExtractJob.holidayCalendar =
                TransformJob.transformFestivals(ExtractJob.holidayCalendar);


        // ==============================
        // LOAD TRANSFORMED TABLES
        // ==============================
        WriteData.loadProducts(ExtractJob.products);
        WriteData.loadCustomers(ExtractJob.customers);
        WriteData.loadOrders(ExtractJob.orders);
        WriteData.loadOrderItems(ExtractJob.orderItems);
        WriteData.loadRegions(ExtractJob.stateRegion);
        WriteData.loadFestivals(ExtractJob.holidayCalendar);

        // ==============================
        // CUSTOMER LIFETIME VALUE
        // ==============================
        Dataset<Row> clvResults =
                CustomerLifetimeValueAnalytics.analyze(
                        ExtractJob.orders,
                        ExtractJob.customers
                );

        WriteData.loadCustomerLifetimeValue(clvResults);

        // ==============================
        // INTERACTIVE ANALYTICS MENU
        // ==============================
        AnalyticsMenu.show(
                ExtractJob.products,
                ExtractJob.customers,
                ExtractJob.orders,
                ExtractJob.orderItems,
                ExtractJob.stateRegion,
                ExtractJob.holidayCalendar
        );

        System.out.println("\n==============================================");
        System.out.println("       PROJECT FINISHED SUCCESSFULLY");
        System.out.println("==============================================");
    }
}
