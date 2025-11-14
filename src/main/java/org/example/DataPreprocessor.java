package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 数据预处理工具类，用于处理CSV文件的预处理操作
 */
public class DataPreprocessor {
    private final String recipeFilepath;
    private final String reviewsFilepath;
    private final String userFilepath;

    public DataPreprocessor(String recipeFilepath, String reviewsFilepath, String userFilepath) {
        this.recipeFilepath = recipeFilepath;
        this.reviewsFilepath = reviewsFilepath;
        this.userFilepath = userFilepath;
    }

    /**
     * 主方法，执行数据预处理流程
     * @param args 命令行参数
     * @throws InterruptedException 线程中断异常
     */
    public static void main(String[] args) throws InterruptedException {
        JsonParamReader jsonParamReader = new JsonParamReader("param.json");
        // 从配置文件读取路径（此处使用默认路径，实际使用时可替换为从JsonParamReader读取）
        String recipe_filepath = jsonParamReader.getString("recipe_filepath").orElse("src/main/resources/recipes.csv");
        String reviews_filepath = jsonParamReader.getString("review_filepath").orElse("src/main/resources/reviews.csv");
        String user_filepath = jsonParamReader.getString("user_filepath").orElse("src/main/resources/user.csv");

        DataPreprocessor preprocessor = new DataPreprocessor(recipe_filepath, reviews_filepath, user_filepath);
        long start = System.currentTimeMillis();
        System.out.println("开始数据预处理...");

        preprocessor.processAllCSVs();

        long end = System.currentTimeMillis();
        System.out.println("数据预处理完成");
        System.out.println("总耗时: " + (end - start) / 1000.0 + "s");
    }

    /**
     * 并行处理所有CSV文件
     * @throws InterruptedException 线程中断异常
     */
    public void processAllCSVs() throws InterruptedException {
        ExecutorService csvExecutor = Executors.newFixedThreadPool(3);
        processCSVLines(recipeFilepath);
        processCSVLines(reviewsFilepath);

        csvExecutor.submit(() -> {
            System.out.println("处理食谱CSV文件...");
            processCSV(recipeFilepath, true);
            System.out.println("食谱CSV文件处理完成");
        });

        csvExecutor.submit(() -> {
            System.out.println("处理评论CSV文件...");
            processCSV(reviewsFilepath, true);
            System.out.println("评论CSV文件处理完成");
        });

        csvExecutor.submit(() -> {
            System.out.println("处理用户CSV文件...");
            processCSV(userFilepath, false);
            System.out.println("用户CSV文件处理完成");
        });

        csvExecutor.shutdown();
        csvExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * 处理单个CSV文件，替换无效字符
     * @param path 文件路径
     * @param replaceInvalidChars 是否替换无效字符
     */
    public void processCSV(String path, boolean replaceInvalidChars) {
        List<String> processedLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (replaceInvalidChars) {
                    // 替换竖线和反斜杠为斜杠，避免与分隔符冲突
                    line = line.replace("|", "/").replace("\\", "/");
                }
                processedLines.add(line);
            }
        } catch (IOException e) {
            System.out.println("读取文件错误: " + path + " - " + e.getMessage());
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            for (String line : processedLines) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("写入文件错误: " + path + " - " + e.getMessage());
        }
    }

    /**
     * 处理CSV文件的行合并（用于修复被拆分的行）
     * @param path 文件路径
     */
    public void processCSVLines(String path) {
        List<String> processLine = new ArrayList<>();
        String previous = "";
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            int lineCnt = 1;
            processLine.add(reader.readLine()); // 添加表头
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                // 假设第一列是自增ID，用于判断行是否被拆分
                String id = currentLine.split(",")[0];
                if (id.equals("" + lineCnt)) {
                    if (!previous.isEmpty()) {
                        processLine.add(previous);
                    }
                    previous = currentLine;
                    lineCnt++;
                } else {
                    previous += currentLine;
                }
            }
            if (previous != null) {
                processLine.add(previous);
            }
        } catch (IOException e) {
            System.out.println("处理文件行错误: " + e.getMessage());
        }

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            for (String line : processLine) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("写入处理后的行错误: " + e.getMessage());
        }
    }
}