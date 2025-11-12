package org.example.DBMSvsFileIO_test;

import java.io.*;
import java.sql.*;
import java.util.*;

public class DBMSvsFileIO {

//---------------------- 配置参数 ----------------------//
    // 数据库连接参数
    private static final String URL = "jdbc:postgresql://localhost:5432/sustc_db";
    private static final String USER = "postgres";
    private static final String PASSWORD = "676767";
    private static final String SCHEMA = "public";
    // .csv测试文件路径
    private static final String CSV_PATH = "recipe.csv";
    // 测试次数
    private static final int N = 100;  // 每种操作执行次数
//-----------------------------------------------------//

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD)) {
            conn.setAutoCommit(false);
            System.out.println("已连接 PostgreSQL 数据库\n");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET search_path TO " + SCHEMA);
            }

            Map<String, Double> dbmsResults = testDBMS(conn);
            Map<String, Double> fileResults = testFileIO();

            printComparison(dbmsResults, fileResults);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // DBMS 部分
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
                ps.setInt(1, rand.nextInt(1000) + 1);
                ps.executeUpdate();
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }));

        return avgTimes;
    }

    // File I/O 部分
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

    private static void printComparison(Map<String, Double> dbms, Map<String, Double> file) {
        System.out.println("==== 性能对比结果（平均耗时，单位：ms） ====");
        System.out.printf("%-10s %-20s %-20s %-10s%n", "操作类型", "PostgreSQL", "File I/O", "倍率差距");
        for (String key : dbms.keySet()) {
            double t1 = dbms.get(key);
            double t2 = file.get(key);
            System.out.printf("%-10s %-20.3f %-20.3f x%.2f%n", key, t1, t2, t2 / t1);
        }
    }
}

