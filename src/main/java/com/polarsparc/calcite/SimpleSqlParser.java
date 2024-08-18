/*
 * Name:   SimpleSqlParser
 * Author: Bhaskar S
 * Date:   08/17/2024
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleSqlParser {
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleSqlParser.class);

    public static void main(String[] args) {
        String goodQuery = "SELECT f2, f3, f5 FROM tbl WHERE f1 = 1 AND f4 = 'N'";
        String badQuery = "SELECT f2, f3, f5 FROM tbl WHERE f1 = ";

        parseSelectStatement(goodQuery);
        parseSelectStatement(badQuery);
    }

    static void parseSelectStatement(String sql) {
        LOGGER.info("------------------------- [ Start ] -------------------------");

        try {
            SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
            SqlNode node = parser.parseStmt();
            SqlSelect select = (SqlSelect) node;

            LOGGER.info("Query statement:");
            LOGGER.info(select.toString());

            assert select.getFrom() != null;

            LOGGER.info("Select from Table: " + select.getFrom().toString());

            LOGGER.info("List of selected fields:");
            select.getSelectList().forEach(sn -> LOGGER.info("-> {}", sn.toString()));
        }
        catch (SqlParseException e) {
            LOGGER.error(e.getMessage());
        }
        finally {
            LOGGER.info("------------------------- [  End  ] -------------------------");
        }
    }
}
