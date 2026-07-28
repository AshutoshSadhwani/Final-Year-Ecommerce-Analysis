package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class FestivalAnalytics {

    /*
        Festival Sales
     */

    public static void festivalRevenue(Dataset<Row> orders){

        System.out.println("\n========== FESTIVAL SALES ==========");

        orders.filter(col("holiday_flag").equalTo("F"))
                .groupBy("festival_name")
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Revenue")
                )
                .orderBy(desc("Revenue"))
                .show(false);

    }

    /*
        Holiday Revenue
     */

    public static void holidayRevenue(Dataset<Row> orders){

        System.out.println("\n========== HOLIDAY SALES ==========");

        orders.filter(col("holiday_flag").equalTo("H"))
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Holiday Revenue")
                )
                .show(false);

    }

    /*
        Month Wise Trend
     */

    public static void monthlyTrend(Dataset<Row> orders){

        System.out.println("\n========== MONTHLY SALES TREND ==========");

        orders.groupBy("month")
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Revenue")
                )
                .orderBy("month")
                .show(false);

    }

    /*
        Quarter Trend
     */

    public static void quarterTrend(Dataset<Row> orders){

        System.out.println("\n========== QUARTER TREND ==========");

        orders.groupBy("quarter")
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Revenue")
                )
                .orderBy("quarter")
                .show(false);

    }

}