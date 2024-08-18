/*
 * Name:   SimpleMemDbJoinSql
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

public class SimpleMemDbJoinSql {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleMemDbJoinSql.class);

    static PublicationSchema publicationSchema = new PublicationSchema();

    public static void main(String[] args) {
        CalciteConnection connection = getCalciteConnection();
        if (connection != null) {
            dataSetup();

            schemaSetup(connection, publicationSchema);

            String sql = "SELECT a.lastName, b.title, b.price FROM " +
                    "pub.authors AS a JOIN pub.books AS b ON a.id = b.authorId WHERE a.id = 2";
            queryJoin(connection, sql);

            try {
                connection.close();
            }
            catch (SQLException ignored) {}
        }
    }

    static void dataSetup() {
        publicationSchema.authors = new Author[] {
            new Author(1, "Alice", "Doctor"),
            new Author(2, "Bob", "Carpenter"),
            new Author(3, "Charlie", "Painter"),
        };

        publicationSchema.books = new Book[] {
            new Book("111", "Guide to Common Cold", 19.99f, 1),
            new Book("222", "How to Build Decks", 15.49f, 2),
            new Book("333", "Awesome Room Colors", 9.99f, 3),
            new Book("444", "How to Treat Cough", 17.49f, 1),
            new Book("555", "Fixing Wall Cracks", 11.99f, 2)
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

    static void schemaSetup(CalciteConnection connection, PublicationSchema publicationSchema) {
        SchemaPlus rootSchema = connection.getRootSchema();
        Schema schema = new ReflectiveSchema(publicationSchema);

        LOGGER.info("Tables names: {}", schema.getTableNames());

        rootSchema.add("pub", schema);

        LOGGER.info("Calcite schema setup !!!");
    }

    static void queryJoin(CalciteConnection connection, String sql) {
        try {
            LOGGER.info("Executing queryAll: {}", sql);

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                LOGGER.info("Author last name: {}, Book title: {}, Book price: {}",
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

    // The ReflectiveSchema requires that all the fields are public

    static public class PublicationSchema {
        public Author[] authors;
        public Book[] books;
    }

    static public class Book {
        public String isbn;
        public String title;
        public float price;
        public int authorId;

        public Book(String isbn, String title, float price, int authorId) {
            this.isbn = isbn;
            this.title = title;
            this.price = price;
            this.authorId = authorId;
        }
    }

    static public class Author {
        public int id;
        public String firstName;
        public String lastName;

        public Author(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }
}
