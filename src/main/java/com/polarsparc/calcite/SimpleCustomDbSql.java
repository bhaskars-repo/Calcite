/*
 * Name:   SimpleCustomDbSql
 * Author: Bhaskar S
 * Date:   08/17/2024
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

public class SimpleCustomDbSql {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleCustomDbSql.class);

    public static void main(String[] args) {
        CalciteConnection connection = getCalciteConnection();
        if (connection != null) {
            schemaSetup(connection);

            String sql = "SELECT d.item, d.store, d.discount FROM dl.deals AS d";
            queryAll(connection, sql);

            try {
                connection.close();
            }
            catch (SQLException ignored) {}
        }
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

    static void schemaSetup(CalciteConnection connection) {
        SchemaPlus rootSchema = connection.getRootSchema();
        Schema schema = new DealsCustomSchema();

        LOGGER.info("Tables names: {}", schema.getTableNames());

        rootSchema.add("dl", schema);

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

    // Represents the data table in-memory
    static class DealsCustomTable extends AbstractTable implements ScannableTable {
        private final List<String> fieldNames = new ArrayList<>();
        private final List<SqlTypeName> fieldTypes = new ArrayList<>();

        private final List<Deal> dealsData = new ArrayList<>();

        public DealsCustomTable() {
            fieldNames.add("item");
            fieldNames.add("store");
            fieldNames.add("price");
            fieldNames.add("discount");

            fieldTypes.add(SqlTypeName.VARCHAR);
            fieldTypes.add(SqlTypeName.VARCHAR);
            fieldTypes.add(SqlTypeName.FLOAT);
            fieldTypes.add(SqlTypeName.INTEGER);

            dataSetup();
        }

        private void dataSetup() {
            dealsData.add(new Deal("iPhone 15 Pro", "Costco", 639.99f, 20));
            dealsData.add(new Deal("iPad 10.9 10th Gen", "BestBuy", 349.99f, 5));
            dealsData.add(new Deal("iWatch Series 9", "Target", 329.99f, 15));
        }

        // Return an enumerator to the stream of data rows (as individual column objects)
        @Override
        public Enumerable<Object[]> scan(DataContext context) {
            Stream<Object[]> fieldsStream = dealsData.stream().map(Deal::toObjectArray);
            return Linq4j.asEnumerable(fieldsStream.toList());
        }

        // Return the fields names and data types for a row in the table
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            List<RelDataType> dataTypes = fieldTypes.stream()
                    .map(typeFactory::createSqlType)
                    .toList();
            return typeFactory.createStructType(dataTypes, fieldNames);
        }
    }

    // Represents the schema that holds table(s) in-memory
    static class DealsCustomSchema extends AbstractSchema {
        @Override
        protected Map<String, Table> getTableMap() {
            return Collections.singletonMap("deals", new DealsCustomTable());
        }
    }

    // Represents a row of the table in-memory
    record Deal(String item, String store, float price, int discount) {
        public Object[] toObjectArray() {
            Object[] fields = new Object[4];
            fields[0] = this.item;
            fields[1] = this.store;
            fields[2] = this.price;
            fields[3] = this.discount;
            return fields;
        }
    }
}
