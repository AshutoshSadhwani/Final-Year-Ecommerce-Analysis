package org.finalecommerceproject.config;

import java.util.Properties;

public class OracleConnection {

    private static final String URL =
            "jdbc:oracle:thin:@localhost:1521:xe";

    private static final String USER = "system";

    private static final String PASSWORD = "ashutosh";

    private static final String DRIVER =
            "oracle.jdbc.driver.OracleDriver";

    public static String getUrl() {
        return URL;
    }

    public static Properties getProperties() {

        Properties properties = new Properties();

        properties.put("user", USER);

        properties.put("password", PASSWORD);

        properties.put("driver", DRIVER);

        return properties;

    }

}