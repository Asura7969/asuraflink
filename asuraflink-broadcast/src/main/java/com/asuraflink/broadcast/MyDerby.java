package com.asuraflink.broadcast;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MyDerby {
    public static final String DB_URL = "jdbc:derby:memory:lookup";
    public static final String LOOKUP_TABLE = "lookup_table";

    public static void before() throws Exception {
        System.out.println("before =======");
//        System.setProperty(
//                "derby.stream.error.field", JdbcTestFixture.class.getCanonicalName() + ".DEV_NULL");

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

        try (Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
             Statement stat = conn.createStatement()) {
            stat.executeUpdate(
                    "CREATE TABLE "
                            + LOOKUP_TABLE
                            + " ("
                            + "id INT NOT NULL DEFAULT 0,"
                            + "name VARCHAR(1000) NOT NULL)");

            Object[][] data =
                    new Object[][] {
                            new Object[] {1, "手机"},
                            new Object[] {2, "牙刷"},
                            new Object[] {3, "电脑"},
                            new Object[] {4, "鼠标"},
                            new Object[] {5, "衣服"}
                    };

            boolean[] surroundedByQuotes = new boolean[] {false, true};
            StringBuilder sqlQueryBuilder =
                    new StringBuilder(
                            "INSERT INTO "
                                    + LOOKUP_TABLE
                                    + " (id, name) VALUES ");

            for (int i = 0; i < data.length; i++) {
                sqlQueryBuilder.append("(");
                for (int j = 0; j < data[i].length; j++) {
                    if (data[i][j] == null) {
                        sqlQueryBuilder.append("null");
                    } else {
                        if (surroundedByQuotes[j]) {
                            sqlQueryBuilder.append("'");
                        }
                        sqlQueryBuilder.append(data[i][j]);
                        if (surroundedByQuotes[j]) {
                            sqlQueryBuilder.append("'");
                        }
                    }
                    if (j < data[i].length - 1) {
                        sqlQueryBuilder.append(", ");
                    }
                }
                sqlQueryBuilder.append(")");
                if (i < data.length - 1) {
                    sqlQueryBuilder.append(", ");
                }
            }
            stat.execute(sqlQueryBuilder.toString());
        }
    }

    public static void after() throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stat = conn.createStatement()) {
            stat.execute("DROP TABLE " + LOOKUP_TABLE);
        }
    }
}
