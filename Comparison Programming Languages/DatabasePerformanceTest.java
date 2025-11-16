package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class DatabasePerformanceTest {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/sustc_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "676767";

    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE_NAME = SCHEMA_NAME + ".test_perf";

    private static final int TOTAL_ROWS = 500000;
    private static final int BATCH_SIZE = 10000;
    private static final int SAMPLE_SIZE = 1000;

    private static final int CONCURRENT_WORKERS = 10;
    private static final int TOTAL_TASKS = 1000;

    private static final Random random = new Random();
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    // 全局 Hikari 连接池
    private static HikariDataSource dataSource;

    public static void main(String[] args) {
        try {
            initConnectionPool();

            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(true);

                // =========================================
                // 1. 初始化 Schema、表
                // =========================================
                System.out.println("\n===== 1. 初始化Schema和表 =====");
                long t1 = System.nanoTime();
                initSchemaAndTable(conn);
                System.out.printf("初始化耗时: %.6f秒%n", (System.nanoTime() - t1) / 1e9);

                // =========================================
                // 2. 插入大量数据
                // =========================================
                System.out.println("\n===== 2. 插入大量数据 =====");
                long t2 = System.nanoTime();
                insertLargeData(conn, TOTAL_ROWS, BATCH_SIZE);
                System.out.printf("插入耗时: %.6f秒%n", (System.nanoTime() - t2) / 1e9);

                // =========================================
                // 3. 查询性能
                // =========================================
                System.out.println("\n===== 3. 测试查询性能 =====");
                long t3 = System.nanoTime();
                testSelectPerformance(conn);
                System.out.printf("查询测试总耗时: %.6f秒%n", (System.nanoTime() - t3) / 1e9);

                // =========================================
                // 4. 更新性能
                // =========================================
                System.out.println("\n===== 4. 测试更新性能 =====");
                long t4 = System.nanoTime();
                testUpdatePerformance(conn);
                System.out.printf("更新测试总耗时: %.6f秒%n", (System.nanoTime() - t4) / 1e9);

                // =========================================
                // 5. 删除性能
                // =========================================
                System.out.println("\n===== 5. 测试删除性能 =====");
                long t5 = System.nanoTime();
                testDeletePerformance(conn);
                System.out.printf("删除测试总耗时: %.6f秒%n", (System.nanoTime() - t5) / 1e9);

                // =========================================
                // 6. 并发性能（已启用连接池）
                // =========================================
                System.out.println("\n===== 6. 测试并发性能 =====");
                testConcurrentPerformance();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
            System.out.println("\n所有测试完成，资源已释放");
        }
    }

    // ==========================================================
    //  初始化 HikariCP 连接池
    // ==========================================================
    private static void initConnectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(USER);
        config.setPassword(PASSWORD);

        config.setMaximumPoolSize(50);
        config.setMinimumIdle(10);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        dataSource = new HikariDataSource(config);
        System.out.println("HikariCP 连接池初始化完成");
    }

    private static String generateRandomStr(int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++)
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        return sb.toString();
    }

    private static int getMaxId(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT MAX(id) FROM " + TABLE_NAME)) {
            if (rs.next()) return rs.getInt(1);
        }
        return 0;
    }

    private static void initSchemaAndTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);
            st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            st.execute("""
                CREATE TABLE %s(
                    id SERIAL PRIMARY KEY,
                    uid VARCHAR(32) NOT NULL UNIQUE,
                    content TEXT NOT NULL,
                    value INT NOT NULL,
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """.formatted(TABLE_NAME));
            st.execute("CREATE INDEX idx_val ON %s(value)".formatted(TABLE_NAME));
        }
    }

    private static void insertLargeData(Connection conn, int total, int batchSize) throws SQLException {
        String sql = "INSERT INTO " + TABLE_NAME + " (uid, content, value) VALUES (?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int batches = (total + batchSize - 1) / batchSize;

            for (int b = 0; b < batches; b++) {
                ps.clearBatch();
                int cur = Math.min(batchSize, total - b * batchSize);

                for (int i = 0; i < cur; i++) {
                    ps.setString(1, generateRandomStr(32));
                    ps.setString(2, generateRandomStr(100));
                    ps.setInt(3, random.nextInt(1000) + 1);
                    ps.addBatch();
                }
                ps.executeBatch();

                if ((b + 1) % 10 == 0)
                    System.out.printf("插入进度 %.1f%%\n", (b + 1) * 100.0 / batches);
            }
        }
    }

    private static void testSelectPerformance(Connection conn) throws SQLException {
        int maxId = getMaxId(conn);
        if (maxId == 0) return;

        long totalSingle = 0;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM " + TABLE_NAME + " WHERE id = ?")) {

            for (int i = 0; i < SAMPLE_SIZE; i++) {
                ps.setInt(1, random.nextInt(maxId) + 1);
                long s = System.nanoTime();
                ps.executeQuery();
                totalSingle += System.nanoTime() - s;
            }
        }
        System.out.printf("单条主键查询平均: %.6f秒%n",
                totalSingle / 1e9 / SAMPLE_SIZE);

        long totalCond = 0;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM " + TABLE_NAME + " WHERE value = ? LIMIT 10")) {
            for (int i = 0; i < SAMPLE_SIZE; i++) {
                ps.setInt(1, random.nextInt(1000) + 1);
                long s = System.nanoTime();
                ps.executeQuery();
                totalCond += System.nanoTime() - s;
            }
        }
        System.out.printf("条件查询平均: %.6f秒%n",
                totalCond / 1e9 / SAMPLE_SIZE);

        long s = System.nanoTime();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM " + TABLE_NAME + " WHERE value BETWEEN 400 AND 600 ORDER BY create_time LIMIT 1000"
        )) {
            ps.executeQuery();
        }
        System.out.printf("范围查询: %.6f秒%n", (System.nanoTime() - s) / 1e9);
    }

    private static void testUpdatePerformance(Connection conn) throws SQLException {
        int maxId = getMaxId(conn);
        long total = 0;

        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {

            for (int i = 0; i < SAMPLE_SIZE; i++) {
                ps.setInt(1, random.nextInt(1000) + 1);
                ps.setInt(2, random.nextInt(maxId) + 1);

                long s = System.nanoTime();
                ps.executeUpdate();
                total += System.nanoTime() - s;
            }
        }
        System.out.printf("更新平均: %.6f秒%n", total / 1e9 / SAMPLE_SIZE);
    }

    private static void testDeletePerformance(Connection conn) throws SQLException {
        List<Object[]> backup = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT id,uid,content,value FROM " + TABLE_NAME + " ORDER BY random() LIMIT " + SAMPLE_SIZE)) {
            while (rs.next())
                backup.add(new Object[]{rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4)});
        }

        long total = 0;
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM " + TABLE_NAME + " WHERE id = ?")) {
            for (Object[] row : backup) {
                ps.setInt(1, (int) row[0]);
                long s = System.nanoTime();
                ps.executeUpdate();
                total += System.nanoTime() - s;
            }
        }
        System.out.printf("删除平均: %.6f秒%n", total / 1e9 / SAMPLE_SIZE);

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + TABLE_NAME + " (id,uid,content,value) VALUES (?,?,?,?)")) {
            for (Object[] row : backup) {
                ps.setInt(1, (int) row[0]);
                ps.setString(2, (String) row[1]);
                ps.setString(3, (String) row[2]);
                ps.setInt(4, (int) row[3]);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // ==========================================================
    // ⭐ 单个并发任务（确保使用连接池）
    // ==========================================================
    private static class ConcurrentTask implements Callable<Map<String, Object>> {

        private final int id;

        public ConcurrentTask(int id) {
            this.id = id;
        }

        @Override
        public Map<String, Object> call() {

            Map<String, Object> res = new HashMap<>();
            res.put("id", id);

            try (Connection conn = dataSource.getConnection()) {  // ⭐ 重点：从连接池取连接
                conn.setAutoCommit(true);

                double p = random.nextDouble();
                String action;

                if (p < 0.3) action = "select";
                else if (p < 0.6) action = "update";
                else action = "delete_restore";

                long start = System.nanoTime();

                switch (action) {
                    case "select" -> doSelect(conn);
                    case "update" -> doUpdate(conn);
                    case "delete_restore" -> doDeleteRestore(conn);
                }

                long end = System.nanoTime();

                res.put("action", action);
                res.put("time", (end - start) / 1e9);

            } catch (Exception e) {
                res.put("action", "error");
                res.put("error", e.getMessage());
            }

            return res;
        }

        private void doSelect(Connection conn) throws Exception {
            int maxId = getMaxId(conn);
            if (maxId <= 0) return;

            int rid = random.nextInt(maxId) + 1;

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM " + TABLE_NAME + " WHERE id = ?")) {
                ps.setInt(1, rid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {}
                }
            }
        }

        private void doUpdate(Connection conn) throws Exception {
            int maxId = getMaxId(conn);
            if (maxId <= 0) return;

            int rid = random.nextInt(maxId) + 1;
            int val = random.nextInt(1000) + 1;

            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                ps.setInt(1, val);
                ps.setInt(2, rid);
                ps.executeUpdate();
            }
        }

        private void doDeleteRestore(Connection conn) throws Exception {
            int id;
            String uid, content;
            int value;

            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id,uid,content,value FROM " +
                                 TABLE_NAME + " ORDER BY random() LIMIT 1")) {

                if (!rs.next()) return;

                id = rs.getInt(1);
                uid = rs.getString(2);
                content = rs.getString(3);
                value = rs.getInt(4);
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM " + TABLE_NAME + " WHERE id = ?")) {
                ps.setInt(1, id);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + TABLE_NAME + " (id,uid,content,value) VALUES (?,?,?,?)")) {
                ps.setInt(1, id);
                ps.setString(2, uid);
                ps.setString(3, content);
                ps.setInt(4, value);
                ps.executeUpdate();
            }
        }
    }

    // ==========================================================
    // ⭐ 并发测试（已使用连接池）
    // ==========================================================
    private static void testConcurrentPerformance() {
        System.out.printf("开始并发测试（%d线程，共%d次操作）...\n",
                CONCURRENT_WORKERS, TOTAL_TASKS);

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_WORKERS);
        List<Future<Map<String, Object>>> futures = new ArrayList<>();

        long start = System.nanoTime();

        for (int i = 0; i < TOTAL_TASKS; i++) {
            futures.add(pool.submit(new ConcurrentTask(i)));
        }

        int success = 0;
        int fail = 0;

        Map<String, List<Double>> times = new HashMap<>();
        times.put("select", new ArrayList<>());
        times.put("update", new ArrayList<>());
        times.put("delete_restore", new ArrayList<>());

        for (Future<Map<String, Object>> f : futures) {
            try {
                Map<String, Object> r = f.get();
                String action = (String) r.get("action");

                if ("error".equals(action)) {
                    fail++;
                } else {
                    success++;
                    times.get(action).add((Double) r.get("time"));
                }

            } catch (Exception e) {
                fail++;
            }
        }

        pool.shutdown();
        long end = System.nanoTime();

        System.out.printf("并发总耗时: %.6f秒%n", (end - start) / 1e9);
        System.out.printf("成功: %d, 失败: %d%n", success, fail);

        times.forEach((k, v) -> {
            double avg = v.stream().mapToDouble(a -> a).average().orElse(0);
            System.out.printf("%s 平均时间: %.6f秒%n", k, avg);
        });
    }
}
