package org.finalecommerceproject;

import org.finalecommerceproject.etl.ExtractJob;
import org.finalecommerceproject.etl.TransformJob;
import org.finalecommerceproject.etl.WriteData;

public class FinalEcommerceAnalysisApplication {
    public static void main(String[] args) {

        // Extract
        ExtractJob.execute();

        // Transform
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

        // Load
        WriteData.loadProducts(ExtractJob.products);
        WriteData.loadCustomers(ExtractJob.customers);
        WriteData.loadOrders(ExtractJob.orders);
        WriteData.loadOrderItems(ExtractJob.orderItems);
        WriteData.loadRegions(ExtractJob.stateRegion);
        WriteData.loadFestivals(ExtractJob.holidayCalendar);

        System.out.println("Project Finished Successfully.");

    }
}
