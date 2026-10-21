
package org.finalecommerceproject.analytics;

// Dataset represents a distributed collection of rows in Apache Spark.
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// Import Spark SQL functions such as sum(), countDistinct(), year(), etc.
import static org.apache.spark.sql.functions.*;

public class SeasonalityAnalytics {

    /*
     * Stores the order data and holiday campaign data.
     *
     * orders:
     * Contains order_id, order_date, total_amount, and other order details.
     *
     * holidayCalendar:
     * Contains event_id, event_name, start_date, and end_date.
     *
     * These are instance variables so all three analytics methods
     * can access the same cleaned datasets.
     */
    private final Dataset<Row> orders;
    private final Dataset<Row> holidayCalendar;

    /*
     * Constructor:
     * Receives the DataFrames prepared by the ETL pipeline.
     *
     * Converts date columns into Spark DateType so that Spark can
     * correctly extract months/quarters and compare date ranges.
     *
     * The format "yyyy-MM-dd" matches dates such as 2025-01-01.
     */
    public SeasonalityAnalytics(
            Dataset<Row> orders,
            Dataset<Row> holidayCalendar) {

        this.orders = orders.withColumn(
                "order_date",
                to_date(col("order_date"), "yyyy-MM-dd")
        );

        this.holidayCalendar = holidayCalendar
                .withColumn(
                        "start_date",
                        to_date(col("start_date"), "yyyy-MM-dd")
                )
                .withColumn(
                        "end_date",
                        to_date(col("end_date"), "yyyy-MM-dd")
                );
    }

    /*
     * ANALYSIS 1: MONTHLY SALES TREND
     *
     * Objective:
     * Find how sales and order activity change from month to month.
     *
     * Output:
     * year, month_number, month_name, total_orders,
     * total_sales, average_order_value.
     */
    public Dataset<Row> monthlySalesTrend() {

        return orders

                // Extract the year from each order date.
                // Keeping the year allows January 2025 and January 2026
                // to appear as separate results.
                .withColumn("year", year(col("order_date")))

                // Extract month as a number from 1 to 12.
                // This allows months to be sorted chronologically.
                .withColumn("month_number", month(col("order_date")))

                // Extract a readable month name, such as January or February.
                .withColumn(
                        "month_name",
                        date_format(col("order_date"), "MMMM")
                )

                // Group all orders belonging to the same year and month.
                .groupBy(
                        col("year"),
                        col("month_number"),
                        col("month_name")
                )

                // Calculate the sales metrics for each month.
                .agg(

                        // Count unique orders to avoid counting the same
                        // order more than once if duplicate rows exist.
                        countDistinct(col("order_id"))
                                .alias("total_orders"),

                        // Add the total_amount values to calculate revenue.
                        sum(col("total_amount"))
                                .alias("total_sales"),

                        // Calculate the mean order amount for that month.
                        avg(col("total_amount"))
                                .alias("average_order_value")
                )

                // Sort by year and numeric month, not alphabetically
                // by month name.
                .orderBy(
                        col("year"),
                        col("month_number")
                );
    }

    /*
     * ANALYSIS 2: QUARTERLY SALES TREND
     *
     * Objective:
     * Compare revenue and order activity across financial/calendar
     * quarters using the standard calendar-year definition:
     *
     * Q1 = January to March
     * Q2 = April to June
     * Q3 = July to September
     * Q4 = October to December
     *
     * Note: These are calendar quarters, not necessarily Indian
     * financial-year quarters.
     */
    public Dataset<Row> quarterlySalesTrend() {

        return orders

                // Extract the year so different years are kept separate.
                .withColumn("year", year(col("order_date")))

                // Spark's quarter() function returns 1, 2, 3, or 4.
                .withColumn(
                        "quarter",
                        quarter(col("order_date"))
                )

                // Group orders by year and quarter.
                .groupBy(
                        col("year"),
                        col("quarter")
                )

                // Calculate the number of unique orders, total revenue,
                // and average order amount in each quarter.
                .agg(
                        countDistinct(col("order_id"))
                                .alias("total_orders"),

                        sum(col("total_amount"))
                                .alias("total_sales"),

                        avg(col("total_amount"))
                                .alias("average_order_value")
                )

                // Display quarters in chronological order.
                .orderBy(
                        col("year"),
                        col("quarter")
                );
    }

    /*
     * ANALYSIS 3: HOLIDAY / CAMPAIGN SALES IMPACT
     *
     * Objective:
     * Measure sales during campaigns such as:
     * New Year Sale, Diwali Sale, Black Friday Sale, and others.
     *
     * The holiday calendar has start_date and end_date columns.
     * Therefore, an order belongs to a campaign when:
     *
     * order_date >= start_date AND order_date <= end_date
     *
     * Both boundaries are inclusive.
     */
    public Dataset<Row> holidaySalesImpact() {

        /*
         * Give the two DataFrames short aliases:
         * h = holiday calendar
         * o = orders
         *
         * A LEFT JOIN preserves every campaign, even if no orders
         * fall within its date range.
         */
        Dataset<Row> eventOrders = holidayCalendar.alias("h")
                .join(
                        orders.alias("o"),

                        // Match orders whose dates fall inside
                        // each campaign's start and end dates.
                        expr(
                                "o.order_date >= h.start_date " +
                                        "AND o.order_date <= h.end_date"
                        ),

                        // Keep campaigns even when there are no matches.
                        "left"
                );

        return eventOrders

                // Group the joined rows by campaign.
                // Each campaign receives its own summary.
                .groupBy(
                        col("h.event_id"),
                        col("h.event_name"),
                        col("h.start_date"),
                        col("h.end_date")
                )

                // Calculate campaign sales metrics.
                .agg(

                        // Count distinct order IDs so an order is counted
                        // once per campaign.
                        countDistinct(col("o.order_id"))
                                .alias("total_orders"),

                        // Add order amounts.
                        // If a campaign has no matching orders, return 0
                        // rather than null for total_sales.
                        coalesce(
                                sum(col("o.total_amount")),
                                lit(0)
                        ).alias("total_sales"),

                        // Average order amount for orders during the
                        // campaign. It remains null if there are no orders.
                        avg(col("o.total_amount"))
                                .alias("average_order_value")
                )

                // Calculate the inclusive number of days in the campaign.
                // Example: Jan 1 through Jan 5 is 5 days, not 4.
                .withColumn(
                        "campaign_days",
                        datediff(
                                col("end_date"),
                                col("start_date")
                        ).plus(lit(1))
                )

                // Calculate average revenue per campaign day.
                // This makes campaigns of different lengths easier to compare.
                .withColumn(
                        "average_daily_sales",
                        col("total_sales")
                                .divide(col("campaign_days"))
                )

                // Sort campaign results by their starting dates.
                .orderBy(col("start_date"));
    }
}
