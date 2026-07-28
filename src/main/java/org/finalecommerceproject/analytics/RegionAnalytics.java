package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class RegionAnalytics {

    /*
        State Wise Revenue
     */

    public static void stateRevenue(Dataset<Row> orders){

        System.out.println("\n========== STATE WISE REVENUE ==========");

        orders.groupBy("state")
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Revenue")
                )
                .orderBy(desc("Revenue"))
                .show(false);

    }

    /*
        Region Wise Revenue
     */

    public static void regionRevenue(Dataset<Row> orders){

        System.out.println("\n========== REGION WISE REVENUE ==========");

        orders.groupBy("region")
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Revenue")
                )
                .orderBy(desc("Revenue"))
                .show(false);

    }

    /*
        Top 5 States
     */

    public static void topStates(Dataset<Row> orders){

        System.out.println("\n========== TOP STATES ==========");

        orders.groupBy("state")
                .agg(sum("total_amount").alias("Revenue"))
                .orderBy(desc("Revenue"))
                .limit(5)
                .show(false);

    }

    /*
        Lowest 5 States
     */

    public static void bottomStates(Dataset<Row> orders){

        System.out.println("\n========== LOWEST STATES ==========");

        orders.groupBy("state")
                .agg(sum("total_amount").alias("Revenue"))
                .orderBy("Revenue")
                .limit(5)
                .show(false);

    }

}