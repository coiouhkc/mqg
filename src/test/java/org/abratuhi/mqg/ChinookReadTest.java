package org.abratuhi.mqg;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by abratuhi on 19.06.14.
 */
public class ChinookReadTest {

    private static final String QUERY_SELECT_1_FROM_TRACK = "select count(1) from Track";

    private Connection connection = null;


    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
    }


    @Before
    public void setUp() {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:src/test/resources/chinook.sqlite");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSelectAllFromTrack() throws SQLException {
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery(QUERY_SELECT_1_FROM_TRACK);) {
            int count = rs.getInt(1);
            assertEquals(3503, count);
        }
    }
}
