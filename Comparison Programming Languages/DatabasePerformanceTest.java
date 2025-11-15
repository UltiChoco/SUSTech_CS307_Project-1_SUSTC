package org.example.compare_test;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DatabasePerformanceTest {
    // 数据库配置
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/project";
    private static final String USER = "postgres";
    private static final String PASSWORD = "Dr141592";
    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE_NAME = SCHEMA_NAME + ".test_perf";
    private static final int TOTAL_ROWS = 500000;
    private static final int BATCH_SIZE = 10000;
    private static final int CONCURRENT_WORKERS = 10;
    private static final int SAMPLE_SIZE = 1000;

    // 随机字符串生成器
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random random = new Random();

    public static void main(String[] args) {
        Connection conn = null;
        try {
            // 初始化连接
            conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            conn.setAutoCommit(true);

            // 1. 初始化Schema和表
            System.out.println("\n===== 1. 初始化Schema和表 =====");
            long initStart = System.nanoTime();
            initSchemaAndTable(conn);
            long initEnd = System.nanoTime();
            double initTime = (initEnd - initStart) / 1e9;
            System.out.printf("初始化耗时: %.6f秒%n", initTime);

            // 2. 插入大量数据
            System.out.println("\n===== 2. 插入大量数据 =====");
            long insertStart = System.nanoTime();
            insertLargeData(conn, TOTAL_ROWS, BATCH_SIZE);
            long insertEnd = System.nanoTime();
            double insertTime = (insertEnd - insertStart) / 1e9;
            System.out.printf("插入%d万条数据总耗时: %.6f秒，平均每条: %.8f秒%n",
                    TOTAL_ROWS / 10000, insertTime, insertTime / TOTAL_ROWS);

            // 3. 测试查询性能
            System.out.println("\n===== 3. 测试查询性能 =====");
            long selectStart = System.nanoTime();
            testSelectPerformance(conn);
            long selectEnd = System.nanoTime();
            double selectTime = (selectEnd - selectStart) / 1e9;
            System.out.printf("查询测试总耗时: %.6f秒%n", selectTime);

            // 4. 测试更新性能
            System.out.println("\n===== 4. 测试更新性能 =====");
            long updateStart = System.nanoTime();
            testUpdatePerformance(conn);
            long updateEnd = System.nanoTime();
            double updateTime = (updateEnd - updateStart) / 1e9;
            System.out.printf("更新测试总耗时: %.6f秒%n", updateTime);

            // 5. 测试删除性能
            System.out.println("\n===== 5. 测试删除性能 =====");
            long deleteStart = System.nanoTime();
            testDeletePerformance(conn);
            long deleteEnd = System.nanoTime();
            double deleteTime = (deleteEnd - deleteStart) / 1e9;
            System.out.printf("删除测试总耗时: %.6f秒%n", deleteTime);

            // 6. 测试并发性能
            System.out.println("\n===== 6. 测试并发性能 =====");
            testConcurrentPerformance();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("\n所有测试完成，资源已释放");
        }
    }

    // 生成随机字符串
    private static String generateRandomStr(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }

    // 初始化Schema和表
    private static void initSchemaAndTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);
            stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.execute("""
                CREATE TABLE %s (
                    id SERIAL PRIMARY KEY,
                    uid VARCHAR(32) NOT NULL UNIQUE,
                    content TEXT NOT NULL,
                    value INT NOT NULL,
                    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """.formatted(TABLE_NAME));
            stmt.execute("CREATE INDEX idx_%s_test_perf_value ON %s(value)"
                    .formatted(SCHEMA_NAME, TABLE_NAME));
            System.out.printf("Schema '%s'和表 '%s' 创建完成%n", SCHEMA_NAME, TABLE_NAME);
        }
    }

    // 批量插入数据
    private static void insertLargeData(Connection conn, int totalRows, int batchSize) throws SQLException {
        System.out.printf("开始插入 %d 条数据...%n", totalRows);
        int totalBatches = (totalRows + batchSize - 1) / batchSize;

        String sql = "INSERT INTO " + TABLE_NAME + " (uid, content, value) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int batch = 0; batch < totalBatches; batch++) {
                int currentBatchSize = Math.min(batchSize, totalRows - batch * batchSize);
                pstmt.clearBatch();

                for (int i = 0; i < currentBatchSize; i++) {
                    pstmt.setString(1, generateRandomStr(32));
                    pstmt.setString(2, generateRandomStr(100));
                    pstmt.setInt(3, random.nextInt(1000) + 1);
                    pstmt.addBatch();
                }

                pstmt.executeBatch();

                if ((batch + 1) % 10 == 0) {
                    double progress = (batch + 1.0) / totalBatches * 100;
                    System.out.printf("插入进度: %.1f%% (%d/%d批次)%n", progress, batch + 1, totalBatches);
                }
            }
        }
    }

    // 测试查询性能
    private static void testSelectPerformance(Connection conn) throws SQLException {
        // 获取最大ID
        int maxId;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM " + TABLE_NAME)) {
            rs.next();
            maxId = rs.getInt(1);
            if (maxId == 0) return;
        }

        // 单条主键查询
        String singleSql = "SELECT * FROM " + TABLE_NAME + " WHERE id = ?";
        long totalSingle = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(singleSql)) {
            for (int i = 0; i < SAMPLE_SIZE; i++) {
                int id = random.nextInt(maxId) + 1;
                pstmt.setInt(1, id);

                long start = System.nanoTime();
                pstmt.executeQuery();
                long end = System.nanoTime();
                totalSingle += (end - start);
            }
        }
        double avgSingle = totalSingle / 1e9 / SAMPLE_SIZE;

        // 条件查询
        String condSql = "SELECT * FROM " + TABLE_NAME + " WHERE value = ? LIMIT 10";
        long totalCond = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(condSql)) {
            for (int i = 0; i < SAMPLE_SIZE; i++) {
                int value = random.nextInt(1000) + 1;
                pstmt.setInt(1, value);

                long start = System.nanoTime();
                pstmt.executeQuery();
                long end = System.nanoTime();
                totalCond += (end - start);
            }
        }
        double avgCond = totalCond / 1e9 / SAMPLE_SIZE;

        // 范围查询
        String rangeSql = "SELECT * FROM " + TABLE_NAME +
                " WHERE value BETWEEN ? AND ? ORDER BY create_time LIMIT 1000";
        long rangeTime;
        try (PreparedStatement pstmt = conn.prepareStatement(rangeSql)) {
            pstmt.setInt(1, 400);
            pstmt.setInt(2, 600);

            long start = System.nanoTime();
            pstmt.executeQuery();
            long end = System.nanoTime();
            rangeTime = end - start;
        }
        double rangeSec = rangeTime / 1e9;

        System.out.println("查询性能:");
        System.out.printf("  单条主键查询（%d次平均）: %.6f秒/次%n", SAMPLE_SIZE, avgSingle);
        System.out.printf("  条件查询（%d次平均）: %.6f秒/次%n", SAMPLE_SIZE, avgCond);
        System.out.printf("  范围查询（1000条结果）: %.6f秒%n", rangeSec);
    }

    // 测试更新性能
    private static void testUpdatePerformance(Connection conn) throws SQLException {
        // 获取最大ID
        int maxId;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM " + TABLE_NAME)) {
            rs.next();
            maxId = rs.getInt(1);
            if (maxId == 0) return;
        }

        String sql = "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?";
        long totalTime = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < SAMPLE_SIZE; i++) {
                int targetId = random.nextInt(maxId) + 1;
                int newValue = random.nextInt(1000) + 1;

                pstmt.setInt(1, newValue);
                pstmt.setInt(2, targetId);

                long start = System.nanoTime();
                pstmt.executeUpdate();
                long end = System.nanoTime();
                totalTime += (end - start);
            }
        }

        double avgUpdate = totalTime / 1e9 / SAMPLE_SIZE;
        System.out.printf("更新性能（%d次平均）: %.6f秒/次%n", SAMPLE_SIZE, avgUpdate);
    }

    // 测试删除性能
    private static void testDeletePerformance(Connection conn) throws SQLException {
        // 备份数据
        List<Object[]> backup = new ArrayList<>();
        String selectSql = "SELECT id, uid, content, value FROM " + TABLE_NAME +
                " ORDER BY random() LIMIT " + SAMPLE_SIZE;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSql)) {
            while (rs.next()) {
                backup.add(new Object[]{
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getInt(4)
                });
            }
        }

        if (backup.isEmpty()) return;

        // 执行删除
        String deleteSql = "DELETE FROM " + TABLE_NAME + " WHERE id = ?";
        long totalTime = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
            for (Object[] row : backup) {
                int id = (int) row[0];
                pstmt.setInt(1, id);

                long start = System.nanoTime();
                pstmt.executeUpdate();
                long end = System.nanoTime();
                totalTime += (end - start);
            }
        }

        // 恢复数据
        String insertSql = "INSERT INTO " + TABLE_NAME + " (id, uid, content, value) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            for (Object[] row : backup) {
                pstmt.setInt(1, (int) row[0]);
                pstmt.setString(2, (String) row[1]);
                pstmt.setString(3, (String) row[2]);
                pstmt.setInt(4, (int) row[3]);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }

        double avgDelete = totalTime / 1e9 / SAMPLE_SIZE;
        System.out.printf("删除性能（%d次平均）: %.6f秒/次%n", SAMPLE_SIZE, avgDelete);
    }

    // 并发操作任务
    private static class ConcurrentTask implements Callable<Map<String, Object>> {
        private final int operationId;

        public ConcurrentTask(int operationId) {
            this.operationId = operationId;
        }

        @Override
        public Map<String, Object> call() {
            Map<String, Object> result = new HashMap<>();
            result.put("id", operationId);

            Connection conn = null;
            try {
                conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
                conn.setAutoCommit(true);

                String action = random.nextBoolean() ?
                        (random.nextBoolean() ? "select" : "update") : "delete_restore";
                long start = System.nanoTime();

                switch (action) {
                    case "select":
                        handleSelect(conn);
                        break;
                    case "update":
                        handleUpdate(conn);
                        break;
                    case "delete_restore":
                        handleDeleteRestore(conn);
                        break;
                }

                long end = System.nanoTime();
                result.put("action", action);
                result.put("time", (end - start) / 1e9);

            } catch (SQLException e) {
                result.put("action", "error");
                result.put("error", e.getMessage());
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        // 忽略关闭异常
                    }
                }
            }
            return result;
        }

        private void handleSelect(Connection conn) throws SQLException {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM " + TABLE_NAME)) {
                if (rs.next() && rs.getInt(1) > 0) {
                    int maxId = rs.getInt(1);
                    int id = random.nextInt(maxId) + 1;
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "SELECT * FROM " + TABLE_NAME + " WHERE id = ?")) {
                        pstmt.setInt(1, id);
                        pstmt.executeQuery();
                    }
                }
            }
        }

        private void handleUpdate(Connection conn) throws SQLException {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM " + TABLE_NAME)) {
                if (rs.next() && rs.getInt(1) > 0) {
                    int maxId = rs.getInt(1);
                    int targetId = random.nextInt(maxId) + 1;
                    int newValue = random.nextInt(1000) + 1;

                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                        pstmt.setInt(1, newValue);
                        pstmt.setInt(2, targetId);
                        pstmt.executeUpdate();
                    }
                }
            }
        }

        private void handleDeleteRestore(Connection conn) throws SQLException {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT id, uid, content, value FROM " + TABLE_NAME + " ORDER BY random() LIMIT 1")) {
                if (rs.next()) {
                    int id = rs.getInt(1);
                    String uid = rs.getString(2);
                    String content = rs.getString(3);
                    int value = rs.getInt(4);

                    // 删除
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "DELETE FROM " + TABLE_NAME + " WHERE id = ?")) {
                        pstmt.setInt(1, id);
                        pstmt.executeUpdate();
                    }

                    // 恢复
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "INSERT INTO " + TABLE_NAME + " (id, uid, content, value) VALUES (?, ?, ?, ?)")) {
                        pstmt.setInt(1, id);
                        pstmt.setString(2, uid);
                        pstmt.setString(3, content);
                        pstmt.setInt(4, value);
                        pstmt.executeUpdate();
                    }
                }
            }
        }
    }

    // 测试并发性能
    private static void testConcurrentPerformance() {
        int totalOps = 1000;
        System.out.printf("开始并发测试（%d线程，共%d次操作）...%n", CONCURRENT_WORKERS, totalOps);

        long start = System.nanoTime();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_WORKERS);
        List<Future<Map<String, Object>>> futures = new ArrayList<>();

        for (int i = 0; i < totalOps; i++) {
            futures.add(executor.submit(new ConcurrentTask(i)));
        }

        // 收集结果
        int success = 0;
        List<String> errors = new ArrayList<>();
        Map<String, List<Double>> actionTimes = new HashMap<>();
        actionTimes.put("select", new ArrayList<>());
        actionTimes.put("update", new ArrayList<>());
        actionTimes.put("delete_restore", new ArrayList<>());

        for (Future<Map<String, Object>> future : futures) {
            try {
                Map<String, Object> result = future.get();
                String action = (String) result.get("action");

                if ("error".equals(action)) {
                    errors.add((String) result.get("error"));
                } else {
                    success++;
                    actionTimes.get(action).add((Double) result.get("time"));
                }
            } catch (InterruptedException | ExecutionException e) {
                errors.add(e.getMessage());
            }
        }

        executor.shutdown();
        long end = System.nanoTime();
        double totalTime = (end - start) / 1e9;

        // 计算平均时间
        Map<String, Double> avgTimes = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : actionTimes.entrySet()) {
            List<Double> times = entry.getValue();
            if (times.isEmpty()) {
                avgTimes.put(entry.getKey(), 0.0);
            } else {
                double avg = times.stream().mapToDouble(Double::doubleValue).average().orElse(0);
                avgTimes.put(entry.getKey(), avg);
            }
        }

        System.out.println("并发测试完成:");
        System.out.printf("  总耗时: %.6f秒%n", totalTime);
        System.out.printf("  总操作数: %d，成功: %d，失败: %d%n", totalOps, success, errors.size());
        System.out.println("  平均耗时（按操作类型）:");
        System.out.printf("    查询: %.6f秒/次%n", avgTimes.get("select"));
        System.out.printf("    更新: %.6f秒/次%n", avgTimes.get("update"));
        System.out.printf("    删除恢复: %.6f秒/次%n", avgTimes.get("delete_restore"));

        if (!errors.isEmpty()) {
            System.out.printf("  错误示例: %s%n", errors.get(0));
        }
    }
}