package org.example.concurrent_test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.example.JsonParamReader;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.logging.*;

public class ConcurrentQueryTest_Pool {

    // 数据库连接参数
    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private static String SCHEMA;

    // 并发参数
    private static final int THREAD_COUNT = 1000;       // 并发线程数
    private static final int TOTAL_QUERIES = 200000;  // 总查询数
    private static final Random random = new Random();

    // 日志对象
    private static final Logger logger = Logger.getLogger(ConcurrentQueryTest_Pool.class.getName());

    static {
        // 加载 json 配置文件
        try {
            JsonParamReader jsonParamReader = new JsonParamReader("param.json");
            URL = jsonParamReader.getString("url")
                    .orElse("jdbc:postgresql://localhost:5432/database_project");
            USER = jsonParamReader.getString("user").orElse("postgres");
            PASSWORD = jsonParamReader.getString("password").orElse("xxxx");
            SCHEMA = jsonParamReader.getString("schema").orElse("project_unlogged");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to load param.json: " + e.getMessage());
        }
        try {
            // 自动检测 logs 文件夹
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
            String logFileName = String.format("concurrent_pool_test_T%d_Q%d.log", THREAD_COUNT, TOTAL_QUERIES);
            String logFilePath = new File(logDir, logFileName).getPath();
            FileHandler fileHandler = new FileHandler(logFilePath, true);
            fileHandler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    return record.getMessage() + System.lineSeparator();
                }
            });
            logger.addHandler(fileHandler);
            logger.setUseParentHandlers(false);
            logger.setLevel(Level.INFO);

            // 控制台输出
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            logger.addHandler(consoleHandler);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String now = String.format("%1$tB %1$te, %1$tY %1$tI:%1$tM:%1$tS %1$Tp %2$s",
                new java.util.Date(),
                ConcurrentQueryTest_Pool.class.getName());
        logger.info(now);

        logger.info("=== Connection Pooling Test Started ====");
        logger.info("Threads = " + THREAD_COUNT + ", Total Queries = " + TOTAL_QUERIES + ", Schema = " + SCHEMA);

        //1. 初始化 HikariCP 连接池
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);
        config.setConnectionInitSql("SET search_path TO " + SCHEMA);

        // 池大小配置
        config.setMaximumPoolSize(THREAD_COUNT);
        config.setMinimumIdle(THREAD_COUNT / 2);


        // 启用 PreparedStatement 缓存,存储常用查询的已编译版本
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        // 创建数据源（连接池对象）
        HikariDataSource ds = new HikariDataSource(config);

        //2. 开始并发测试
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(TOTAL_QUERIES);
        long start = System.currentTimeMillis();

        try {
            for (int i = 0; i < TOTAL_QUERIES; i++) {
                pool.submit(() -> {
                    try (Connection conn = ds.getConnection();   // 从连接池获取连接
                         PreparedStatement ps = conn.prepareStatement(
                                 "SELECT * FROM recipe WHERE recipe_id = ?")) {

                        ps.setInt(1, random.nextInt(522517) + 1);
                        ps.executeQuery();

                    } catch (SQLException e) {
                        logger.warning("SQL Error: " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // 等待所有线程完成
            latch.await();

            //3. 统计性能结果
            long end = System.currentTimeMillis();
            double totalTime = (end - start) / 1000.0;
            double avgPerQuery = (end - start) * 1.0 / TOTAL_QUERIES;
            double qps = TOTAL_QUERIES / totalTime;

            logger.info(String.format("Total time: %.2f s", totalTime));
            logger.info(String.format("Average per query: %.3f ms", avgPerQuery));
            logger.info(String.format("Throughput (QPS): %.2f queries/s", qps));
            logger.info("==== Connection Pooling Test Finished ====");
        } finally {
            //4. 关闭资源
            pool.shutdown();  // 关闭线程池
            ds.close();       // 关闭连接池（释放所有连接）
        }
    }
}
