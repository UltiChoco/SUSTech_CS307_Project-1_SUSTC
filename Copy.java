package org.example;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

public class Copy {
    private Connection connection;
    private CopyManager copyManager;

    public Copy(Connection connection) throws SQLException {
        this.connection = connection;
        this.copyManager = new CopyManager((BaseConnection) connection);
    }

    public void copyTo(String table, String value_list, StringBuilder csvBuilder,String dev) throws SQLException, IOException {
        String sql = "COPY " + table + " (" + value_list + ") FROM STDIN WITH DELIMITER '"+dev+"'";
        InputStream inputStream = new ByteArrayInputStream(
                csvBuilder.toString().getBytes(StandardCharsets.UTF_8)
        );

        try {
            long rows = copyManager.copyIn(sql, inputStream);
            connection.commit();
        } catch (SQLException | IOException e) {
            connection.rollback();
            throw e;
        } finally {
            csvBuilder.setLength(0);
            inputStream.close();
        }
    }
}
