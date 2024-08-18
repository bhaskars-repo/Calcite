/*
 * Name:   SimpleFileSql
 * Author: Bhaskar S
 * Date:   08/17/2024
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SimpleFileSql {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleFileSql.class);

    public static void main(String[] args) {
        CalciteConnection connection = getCalciteConnection();
        if (connection != null) {
            // The column names *MUST* be enclosed in double quotes
            String sql = "SELECT \"name\", \"email\", \"mobile\" FROM customers";
            queryAll(connection, sql);

            try {
                connection.close();
            }
            catch (SQLException ignored) {}
        }
    }

    static CalciteConnection getCalciteConnection() {
        CalciteConnection calciteConn = null;

        // The csv file has to be named in uppercase !!!

        try {
            Connection conn = DriverManager.getConnection("jdbc:calcite:model=target/classes/model.json");
            calciteConn = conn.unwrap(CalciteConnection.class);
        }
        catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }

        LOGGER.info("Calcite connection created !!!");

        return calciteConn;
    }

    static void queryAll(Connection connection, String sql) {
        try {
            LOGGER.info("Executing queryAll: {}", sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                LOGGER.info("Name: {}, Email: {}, Mobile: {}",
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3));
            }
            rs.close();
            stmt.close();
        }
        catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
