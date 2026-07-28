package org.finalecommerceproject.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class LoyaltyAnalytics {

    /*
        Loyalty Members
     */

    public static void loyaltyMembers(Dataset<Row> customers){

        System.out.println("\n========== LOYALTY MEMBERS ==========");

        customers.filter(col("loyalty_flag").equalTo("Y"))
                .show(false);

    }

    /*
        Loyalty Count
     */

    public static void loyaltyCount(Dataset<Row> customers){

        System.out.println("\n========== LOYALTY COUNT ==========");

        customers.groupBy("loyalty_flag")
                .count()
                .show(false);

    }

    /*
        Revenue From Loyalty Customers
     */

    public static void loyaltyRevenue(Dataset<Row> orders){

        System.out.println("\n========== LOYALTY REVENUE ==========");

        orders.filter(col("loyalty_flag").equalTo("Y"))
                .agg(
                        round(sum("total_amount"),2)
                                .alias("Revenue")
                )
                .show(false);

    }

}