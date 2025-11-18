package org.example.Task3;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class InsertPerformanceTester {

    private final Connection connection;
    private final Copy copy;
    private final String schema;
    private final String reviewsFilepath;
    private static final String TABLE = "review";
    private static final String VALUE_LIST = "review_id,recipe_id,author_id,rating,review_text,date_submit,date_modify";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public InsertPerformanceTester(String url, String user, String password, String schema, String reviewsFilepath) {
        this.schema = schema;
        this.reviewsFilepath = reviewsFilepath;
        try {
            this.connection = DriverManager.getConnection(url, user, password);
            this.connection.setAutoCommit(false);
            this.copy = new Copy(connection);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize database connection", e);
        }
    }

    public void testCopyInsert() throws SQLException, IOException, CsvValidationException {
        String fullTable = schema + "." + TABLE;
        truncateTable(fullTable);
        
        CSVParser csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withEscapeChar('￥')
                .withQuoteChar('"')
                .build();
        CSVReader csvReader = new CSVReaderBuilder(new FileReader(reviewsFilepath))
                .withCSVParser(csvParser)
                .build();

        try {
            csvReader.readNext();
            StringBuilder csvBuilder = new StringBuilder();
            String[] nextLine;

            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = processReviewLine(nextLine);
                if (fields == null) continue;

                csvBuilder.append(String.join("|", fields)).append("\n");
            }

            long start = System.currentTimeMillis();
            copy.copyTo(fullTable, VALUE_LIST, csvBuilder, "|");
            long end = System.currentTimeMillis();
            System.out.printf("Copy插入耗时: %d ms%n", end - start);
        } finally {
            csvReader.close();
        }
    }

    public void testPreparedStatementInsertWithBatchSize(int batchSize) throws SQLException, IOException, CsvValidationException, ParseException {
        String fullTable = schema + "." + TABLE;
        truncateTable(fullTable);
        
        CSVParser csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withEscapeChar('￥')
                .withQuoteChar('"')
                .build();
        CSVReader csvReader = new CSVReaderBuilder(new FileReader(reviewsFilepath))
                .withCSVParser(csvParser)
                .build();

        String sql = "INSERT INTO " + fullTable + " (" + VALUE_LIST + ") VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            csvReader.readNext();
            String[] nextLine;
            int count = 0;

            long start = System.currentTimeMillis();
            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = processReviewLine(nextLine);
                if (fields == null) continue;

                pstmt.setInt(1, Integer.parseInt(fields[0]));
                pstmt.setInt(2, Integer.parseInt(fields[1]));
                pstmt.setInt(3, Integer.parseInt(fields[2]));
                pstmt.setInt(4, Integer.parseInt(fields[3]));
                pstmt.setString(5, fields[4]);
                pstmt.setDate(6, new Date(DATE_FORMAT.parse(fields[5]).getTime()));
                pstmt.setDate(7, new Date(DATE_FORMAT.parse(fields[6]).getTime()));
                pstmt.addBatch();

                if (++count % batchSize == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
            connection.commit();
            long end = System.currentTimeMillis();
            System.out.printf("PreparedStatement(batchSize=%d)插入耗时: %d ms%n", batchSize, end - start);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            csvReader.close();
        }
    }

    public void testStatementInsertWithBatchSize(int batchSize) throws SQLException, IOException, CsvValidationException {
        String fullTable = schema + "." + TABLE;
        truncateTable(fullTable);
        
        CSVParser csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withEscapeChar('￥')
                .withQuoteChar('"')
                .build();
        CSVReader csvReader = new CSVReaderBuilder(new FileReader(reviewsFilepath))
                .withCSVParser(csvParser)
                .build();

        try {
            csvReader.readNext();
            String[] nextLine;
            List<String> sqlBatch = new ArrayList<>();

            long start = System.currentTimeMillis();
            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = processReviewLine(nextLine);
                if (fields == null) continue;

                String sql = String.format(
                        "INSERT INTO %s (%s) VALUES (%s, %s, %s, %s, '%s', '%s'::date, '%s'::date)",
                        fullTable,
                        VALUE_LIST,
                        fields[0],
                        fields[1],
                        fields[2],
                        fields[3],
                        escapeSql(fields[4]),
                        escapeSql(fields[5]),
                        escapeSql(fields[6])
                );
                sqlBatch.add(sql);

                if (sqlBatch.size() >= batchSize) {
                    executeSqlBatch(sqlBatch);
                    sqlBatch.clear();
                }
            }

            if (!sqlBatch.isEmpty()) {
                executeSqlBatch(sqlBatch);
            }
            connection.commit();
            long end = System.currentTimeMillis();
            System.out.printf("普通Statement(batchSize=%d)插入耗时: %d ms%n", batchSize, end - start);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            csvReader.close();
        }
    }

    private String[] processReviewLine(String[] nextLine) {
        String[] fields = new String[9];
        for (int i = 0; i < Math.min(9, nextLine.length); i++) {
            fields[i] = nextLine[i] == null ? "" : nextLine[i].replace("|", "/").replace("\\", "/");
        }
        if (fields[1].isEmpty()) {
            return null;
        }
        try {
            return new String[]{
                    fields[0],
                    String.valueOf(Math.round(Double.parseDouble(fields[1]))),
                    fields[2],
                    fields[4],
                    fields[5],
                    fields[6],
                    fields[7]
            };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private void truncateTable(String table) throws SQLException {
        connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
    }

    private void executeSqlBatch(List<String> sqlList) throws SQLException {
        var stmt = connection.createStatement();
        for (String sql : sqlList) {
            stmt.addBatch(sql);
        }
        stmt.executeBatch();
        stmt.close();
    }

    private String escapeSql(String value) {
        return value.replace("'", "''");
    }

    public static void main(String[] args) {
        try {
            JsonParamReader jsonParamReader = new JsonParamReader("param.json");
            String url = jsonParamReader.getString("url").orElse("jdbc:postgresql://localhost:5432/database_project");
            String user = jsonParamReader.getString("user").orElse("postgres");
            String password = jsonParamReader.getString("password").orElse("Dr141592");
            String schema = jsonParamReader.getString("schema").orElse("project_unlogged");
            String reviewsFilepath = jsonParamReader.getString("review_filepath").orElse("src/main/resources/reviews.csv");

            InsertPerformanceTester tester = new InsertPerformanceTester(url, user, password, schema, reviewsFilepath);
            
            System.out.println("开始测试Copy插入...");
            //tester.testCopyInsert();
            
            int[] batchSizes = {500,1000, 2000, 5000, 10000};
            System.out.println("开始测试不同batchSize的PreparedStatement插入...");
            for (int batchSize : batchSizes) {
                tester.testPreparedStatementInsertWithBatchSize(batchSize);
            }
            
            System.out.println("开始测试不同batchSize的普通Statement插入...");
            for (int batchSize : batchSizes) {
                tester.testStatementInsertWithBatchSize(batchSize);
            }

            tester.connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
