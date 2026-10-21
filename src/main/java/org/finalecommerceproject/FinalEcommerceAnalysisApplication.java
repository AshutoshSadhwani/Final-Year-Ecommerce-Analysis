package org.finalecommerceproject;

import org.finalecommerceproject.analytics.ProductAnalytics;
import org.finalecommerceproject.analytics.RegionAnalytics;
import org.finalecommerceproject.etl.ExtractJob;
import org.finalecommerceproject.etl.TransformJob;
import org.finalecommerceproject.etl.WriteData;
import org.finalecommerceproject.analytics.SeasonalityAnalytics;

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
                TransformJob.transformFestivals(
                        ExtractJob.holidayCalendar
                );


        // ==============================
        // ANALYSIS
        // ==============================

        ProductAnalytics.bestSellingProductByCategory(
                ExtractJob.products,
                ExtractJob.orderItems
        );

        RegionAnalytics.regionWiseSalesByState(
                ExtractJob.orders,
                ExtractJob.stateRegion
        );

        // Analyze monthly, quarterly, and holiday sales patterns.
        SeasonalityAnalytics seasonality =
                new SeasonalityAnalytics(
                        ExtractJob.orders,
                        ExtractJob.holidayCalendar
                );

        System.out.println("\n===== MONTHLY SALES TREND =====");
        seasonality.monthlySalesTrend().show(12, false);

        System.out.println("\n===== QUARTERLY SALES TREND =====");
        seasonality.quarterlySalesTrend().show(false);

        System.out.println("\n===== HOLIDAY SALES IMPACT =====");
        seasonality.holidaySalesImpact().show(20, false);

        // ==============================
        // LOAD
        // ==============================

        WriteData.loadProducts(ExtractJob.products);
        WriteData.loadCustomers(ExtractJob.customers);
        WriteData.loadOrders(ExtractJob.orders);
        WriteData.loadOrderItems(ExtractJob.orderItems);
        WriteData.loadRegions(ExtractJob.stateRegion);
        WriteData.loadFestivals(ExtractJob.holidayCalendar);


        System.out.println("\n==============================================");
        System.out.println("       PROJECT FINISHED SUCCESSFULLY");
        System.out.println("==============================================");
    }
}