package org.example.DBMSvsFileIO_test;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;
import java.nio.file.*;
import java.util.stream.Collectors;

import org.example.JsonParamReader;

public class DBMSvsFileIO {

    // 数据库连接参数
    private static String URL;
    private static String USER;
    private static String PASSWORD;
    private static String SCHEMA;
    private static String CSV_PATH;
    // 每种操作执行次数
    private static final int N = 20;

    // 日志对象
    private static final Logger logger = Logger.getLogger(DBMSvsFileIO.class.getName());

    static  {
        // 加载 json 配置文件
        try {
            JsonParamReader jsonParamReader = new JsonParamReader("param.json");
            URL = jsonParamReader.getString("url")
                    .orElse("jdbc:postgresql://localhost:5432/database_project");
            USER = jsonParamReader.getString("user").orElse("postgres");
            PASSWORD = jsonParamReader.getString("password").orElse("xxxx");
            SCHEMA = jsonParamReader.getString("schema").orElse("project");
            CSV_PATH = jsonParamReader.getString("recipe_filepath").orElse("recipe.csv");
            System.out.println("[INFO] 配置加载成功：");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to load param.json: " + e.getMessage());
        }
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
            String logFilePath = new File(logDir, "DBMS_vs_File_test.log").getPath();
            FileHandler fileHandler = new FileHandler(logFilePath, true);
            fileHandler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    return record.getMessage() + System.lineSeparator();
                }
            });
            logger.addHandler(fileHandler);
            logger.setLevel(Level.INFO);

            // 控制台输出
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(Level.INFO);
            logger.addHandler(consoleHandler);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String now = String.format("%1$tB %1$te, %1$tY %1$tI:%1$tM:%1$tS %1$Tp %2$s",
                new java.util.Date(),
                DBMSvsFileIO.class.getName());
        logger.info(now);

        logger.info("=== DBMS vs File I/O Test Started ====");
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            conn.setAutoCommit(false);
            System.out.println("已连接 PostgreSQL 数据库\n");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET search_path TO " + SCHEMA);
                conn.commit();
                System.out.println("已经完成路径切换");
            }

            Map<String, Double> dbmsResults = testDBMS(conn);
            Map<String, Double> fileResults = testFileIO();
            Map<String, Double> streamResults = testFileIOStream();  // 新增Stream测试

            printComparison(dbmsResults, fileResults, streamResults);  // 更新打印方法

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Double> testDBMS(Connection conn) throws Exception {
        Map<String, Double> avgTimes = new LinkedHashMap<>();
        Random rand = new Random();

        // SELECT
        avgTimes.put("SELECT", avgTime(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM recipe WHERE recipe_id = ?")) {
                ps.setInt(1, rand.nextInt(1000) + 1);
                ps.executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));

        // INSERT
        avgTimes.put("INSERT", avgTime(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO recipe (author_id, dish_name, date_published) VALUES (1, ?, CURRENT_DATE)")) {
                ps.setString(1, "TempDish_" + rand.nextInt(100000));
                ps.executeUpdate();
                conn.rollback(); // 避免污染数据
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));

        // UPDATE
        avgTimes.put("UPDATE", avgTime(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE recipe SET dish_name = dish_name WHERE recipe_id = ?")) {
                ps.setInt(1, rand.nextInt(1000) + 1);
                ps.executeUpdate();
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));

        // DELETE
        avgTimes.put("DELETE", avgTime(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE FROM recipe WHERE recipe_id = ?")) {
                //删除一个不存在的id，避免外键约束问题
                ps.setInt(1, Integer.MAX_VALUE);
                ps.executeUpdate();
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));

        return avgTimes;
    }

    private static Map<String, Double> testFileIO() throws Exception {
        Map<String, Double> avgTimes = new LinkedHashMap<>();
        Random rand = new Random();

        // SELECT：逐行扫描
        avgTimes.put("SELECT", avgTime(() -> {
            int target = rand.nextInt(1000) + 1;
            try (BufferedReader br = new BufferedReader(new FileReader(CSV_PATH))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith(target + ",")) break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        // INSERT ：追加到文件末尾
        avgTimes.put("INSERT", avgTime(() -> {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(CSV_PATH, true))) {
                bw.write("999999,TempDish_" + rand.nextInt(100000) + ",1,2025-11-12\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        // UPDATE ：读取全部行到内存, 修改后再写回
        avgTimes.put("UPDATE", avgTime(() -> {
            int target = rand.nextInt(1000) + 1;
            List<String> lines = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(CSV_PATH))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith(target + ",")) {
                        line = line.replaceFirst(",", "_updated,");
                    }
                    lines.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 写回临时文件再覆盖,防止破坏原文件
            File temp = new File("recipe_tmp.csv");
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
                for (String l : lines) bw.write(l + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
            temp.delete(); // 不保留结果
        }));

        // DELETE ：重写文件去除指定行
        avgTimes.put("DELETE", avgTime(() -> {
            int target = rand.nextInt(1000) + 1;
            File temp = new File("recipe_tmp.csv");
            try (BufferedReader br = new BufferedReader(new FileReader(CSV_PATH));
                 BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.startsWith(target + ",")) bw.write(line + "\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            temp.delete();
        }));

        return avgTimes;
    }

    private static Map<String, Double> testFileIOStream() throws Exception {
        Map<String, Double> avgTimes = new LinkedHashMap<>();
        Random rand = new Random();
        Path csvPath = Paths.get(CSV_PATH);

        // SELECT：使用Stream过滤查找
        avgTimes.put("SELECT", avgTime(() -> {
            int target = rand.nextInt(1000) + 1;
            String targetPrefix = target + ",";
            try {
                Files.lines(csvPath)
                        .filter(line -> line.startsWith(targetPrefix))
                        .findFirst(); // 找到第一个匹配项就停止
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        // INSERT：使用Stream追加（实际还是用传统方式更高效，这里仅作对比）
        avgTimes.put("INSERT", avgTime(() -> {
            String newLine = "999999,TempDish_Stream_" + rand.nextInt(100000) + ",1,2025-11-12";
            try {
                Files.write(csvPath,
                        Collections.singletonList(newLine),
                        StandardOpenOption.APPEND,
                        StandardOpenOption.CREATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        // UPDATE：使用Stream处理并写入临时文件
        avgTimes.put("UPDATE", avgTime(() -> {
            int target = rand.nextInt(1000) + 1;
            String targetPrefix = target + ",";
            Path tempPath = Paths.get("recipe_stream_tmp.csv");
            try {
                List<String> updatedLines = Files.lines(csvPath)
                        .map(line -> line.startsWith(targetPrefix) ?
                                line.replaceFirst(",", "_updated,") : line)
                        .collect(Collectors.toList());

                Files.write(tempPath, updatedLines);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        avgTimes.put("DELETE", avgTime(() -> {
            int target = rand.nextInt(1000) + 1;
            String targetPrefix = target + ",";
            Path tempPath = Paths.get("recipe_stream_tmp.csv");
            try {
                List<String> remainingLines = Files.lines(csvPath)
                        .filter(line -> !line.startsWith(targetPrefix))
                        .collect(Collectors.toList());

                Files.write(tempPath, remainingLines);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        return avgTimes;
    }

    private static double avgTime(Runnable action) {
        long total = 0;
        for (int i = 0; i < N; i++) {
            long t1 = System.nanoTime();
            action.run();
            long t2 = System.nanoTime();
            total += (t2 - t1);
        }
        return total / 1e6 / N; // 返回毫秒平均值
    }

    private static void printComparison(Map<String, Double> dbms, Map<String, Double> file, Map<String, Double> stream) {

        logger.info("性能对比结果（平均耗时，单位：ms）：");

        String header = String.format("%-10s %-20s %-20s %-20s %-15s %-15s",
                "操作类型", "PostgreSQL", "File I/O", "File Stream",
                "File/DB倍率", "Stream/DB倍率");
        logger.info(header);

        for (String key : dbms.keySet()) {
            double dbTime = dbms.get(key);
            double fileTime = file.get(key);
            double streamTime = stream.get(key);
            String line = String.format("%-10s %-20.3f %-20.3f %-20.3f x%.2f        x%.2f",
                    key, dbTime, fileTime, streamTime,
                    fileTime / dbTime, streamTime / dbTime);
            logger.info(line);
        }
        logger.info("==== DBMS vs File I/O Test Finished ====");
    }

}