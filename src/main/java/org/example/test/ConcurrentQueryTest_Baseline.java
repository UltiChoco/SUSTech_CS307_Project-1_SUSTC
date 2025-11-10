//todo: baseline --> Connection Pooling (HikariCP) --> PreparedStatement Cache + Warm-up
package org.example.test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.logging.*;

public class ConcurrentQueryTest_Baseline {

    private static final String URL = "jdbc:postgresql://localhost:5432/sustc_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "676767";
    private static final String SCHEMA = "public";

    private static final int THREAD_COUNT = 10;
    private static final int TOTAL_QUERIES = 100;
    private static final Random random = new Random(); //生成随机recipe_id

    // 日志对象
    private static final Logger logger = Logger.getLogger(ConcurrentQueryTest_Baseline.class.getName());

    static {
        try {
            // 自动检测有没有logs文件夹，没有的话会生成。注意，.gitignore已经忽略logs文件夹
            File logDir = new File("logs");
            if (!logDir.exists()) {
                boolean created = logDir.mkdirs();
                if (created) {
                    System.out.println("[INFO] Created logs directory: " + logDir.getAbsolutePath());
                } else {
                    System.err.println("[WARNING] Failed to create logs directory, fallback to root directory.");
                }
            }
            //配置日志输出到文件
            String logFilePath = new File(logDir, "baseline_test.log").getPath();
            FileHandler fileHandler = new FileHandler(logFilePath, true); // 追加模式,不覆盖历史
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setLevel(Level.INFO);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        logger.info("=== Baseline Test Started ====");
        logger.info("Threads = " + THREAD_COUNT + ", Total Queries = " + TOTAL_QUERIES + ", Schema = " + SCHEMA);

        //创建线程池
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        try {
            CountDownLatch latch = new CountDownLatch(TOTAL_QUERIES);
            long start = System.currentTimeMillis();

            for (int i = 0; i < TOTAL_QUERIES; i++) {
                pool.submit(() -> {
                    try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
                         Statement schemaStmt = conn.createStatement()) {

                        schemaStmt.execute("SET search_path TO " + SCHEMA);

                        try (PreparedStatement ps = conn.prepareStatement(
                                "SELECT * FROM recipe WHERE recipe_id = ?")) {
                            ps.setInt(1, random.nextInt(50000) + 1);
                            ps.executeQuery();
                        }

                    } catch (SQLException e) {
                        logger.warning("SQL Error: " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();

            long end = System.currentTimeMillis();
            double totalTime = (end - start) / 1000.0;
            double avgPerQuery = (end - start) * 1.0 / TOTAL_QUERIES;
            double qps = TOTAL_QUERIES / totalTime;

            logger.info(String.format("Total time: %.2f s", totalTime));
            logger.info(String.format("Average per query: %.3f ms", avgPerQuery));
            logger.info(String.format("Throughput (QPS): %.2f queries/s", qps));
            logger.info("==== Baseline Test Finished ====");
        } finally {
            pool.shutdown();
        }
    }
}

