/*
 * Name:   SimpleMemDbSql
 * Author: Bhaskar S
 * Date:   08/17/2024
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.calcite;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class SimpleMemDbSql {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleMemDbSql.class);

    static ContactSchema contactSchema = new ContactSchema();

    public static void main(String[] args) {
        CalciteConnection connection = getCalciteConnection();
        if (connection != null) {
            dataSetup();

            schemaSetup(connection, contactSchema);

            String sql = "SELECT c.lastName, c.email, c.mobile FROM cs.contacts AS c";
            queryAll(connection, sql);

            String sql2 = "SELECT c.email, c.mobile FROM cs.contacts AS c WHERE c.lastName = 'Doctor'";
            queryWhere(connection, sql2);

            try {
                connection.close();
            }
            catch (SQLException ignored) {}
        }
    }

    static void dataSetup() {
        contactSchema.contacts = new Contact[] {
            new Contact("Alice", "Doctor", "alice.d@space.com", "123-321-1000"),
            new Contact("Bob", "Carpenter", "bob.c@space.com", "234-432-2000"),
            new Contact("Charlie", "Painter", "charlie_p@space.com", "345-543-3000"),
            new Contact("Donna", "Plumber", "donna.p@space.com", "456-654-4000"),
            new Contact("Eve", "Dentist", "eve_d@space.com", "567-765-5000")
        };
    }

    static CalciteConnection getCalciteConnection() {
        Properties props = new Properties();
        props.setProperty("lex", "JAVA");

        CalciteConnection calciteConn = null;

        try {
            Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
            calciteConn = conn.unwrap(CalciteConnection.class);
        }
        catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }

        LOGGER.info("Calcite connection created !!!");

        return calciteConn;
    }

    static void schemaSetup(CalciteConnection connection, ContactSchema contactSchema) {
        SchemaPlus rootSchema = connection.getRootSchema();
        Schema schema = new ReflectiveSchema(contactSchema);

        LOGGER.info("Tables names: {}", schema.getTableNames());

        rootSchema.add("cs", schema);

        LOGGER.info("Calcite schema setup !!!");
    }

    static void queryAll(CalciteConnection connection, String sql) {
        try {
            LOGGER.info("Executing queryAll: {}", sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                LOGGER.info("Last name: {}, Email: {}, Mobile: {}",
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

    static void queryWhere(CalciteConnection connection, String sql) {
        try {
            LOGGER.info("Executing queryWhere: {}", sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                LOGGER.info("Email: {}, Mobile: {}",
                        rs.getString(1),
                        rs.getString(2));
            }
            rs.close();
            stmt.close();
        }
        catch (SQLException e) {
            LOGGER.error(e.getMessage());
        }
    }

    // The ReflectiveSchema requires that all the fields are public

    static public class ContactSchema {
        public Contact[] contacts;
    }

    static public class Contact {
        public String firstName;
        public String lastName;
        public String email;
        public String mobile;

        public Contact(String firstName, String lastName, String email, String mobile) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
            this.mobile = mobile;
        }
    }
}
