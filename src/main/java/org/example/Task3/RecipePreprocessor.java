//不完善，目前只能实现打印不符合要求的数据，仅供参考
package org.example.Task3;

import com.opencsv.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RecipePreprocessor {

    public static void main(String[] args) {
        String inputPath = "src/main/resources/recipes.csv";
        String cleanPath = "src/main/resources/recipes_clean.csv";
        String errorPath = "src/main/resources/recipes_error.log";

        preprocessRecipes(inputPath, cleanPath, errorPath);
    }

    public static void preprocessRecipes(String inputPath, String cleanPath, String errorPath) {
        int total = 0, success = 0, fail = 0;
        List<String> errors = new ArrayList<>();

        try (BufferedWriter cleanWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cleanPath), StandardCharsets.UTF_8));
             BufferedWriter errorWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(errorPath), StandardCharsets.UTF_8))) {

            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();

            try (CSVReader csvReader = new CSVReaderBuilder(new FileReader(inputPath))
                    .withCSVParser(csvParser)
                    .build()) {

                String[] header = csvReader.readNext(); // 表头
                if (header != null) {
                    cleanWriter.write(String.join(",", header));
                    cleanWriter.newLine();
                }

                String[] nextLine;
                while ((nextLine = csvReader.readNext()) != null) {
                    total++;

                    String[] fields = preprocessFields(nextLine);

                    ValidationResult result = validate(fields);
                    if (result.valid) {
                        cleanWriter.write(String.join(",", fields));
                        cleanWriter.newLine();
                        success++;
                    } else {
                        fail++;
                        errors.add("行 " + total + " 异常 [" + result.reason + "] → " + Arrays.toString(fields));
                    }

                    if (total % 5000 == 0)
                        System.out.printf("已处理 %d 行... 成功 %d, 失败 %d%n", total, success, fail);
                }
            }

            for (String err : errors) {
                errorWriter.write(err);
                errorWriter.newLine();
            }

            System.out.printf("清洗完成: 成功 %d 行, 失败 %d 行%n", success, fail);
            System.out.println("干净数据: " + cleanPath);
            System.out.println("错误日志: " + errorPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** 字段预处理（去转义、补空值） */
    private static String[] preprocessFields(String[] line) {
        String[] f = new String[25];
        for (int i = 0; i < Math.min(25, line.length); i++) {
            f[i] = line[i] == null ? "" : line[i].replace("|", "/").replace("\\", "/");
        }
        if (f[4].isEmpty()) f[4] = "PT0S";
        if (f[5].isEmpty()) f[5] = "PT0S";
        return f;
    }

    /** 校验结果结构体 */
    static class ValidationResult {
        boolean valid;
        String reason;
        ValidationResult(boolean v, String r) { valid = v; reason = r; }
    }

    /** 主校验逻辑 */
    private static ValidationResult validate(String[] f) {
        try {
            if (f == null || f.length < 25)
                return new ValidationResult(false, "字段数量不足");

            if (f[0] == null || f[0].trim().isEmpty())
                return new ValidationResult(false, "recipe_id 缺失");
            Integer.parseInt(f[0].trim());

            if (f[1] == null || f[1].trim().isEmpty())
                return new ValidationResult(false, "dish_name 为空");
            if (f[1].contains(","))
                return new ValidationResult(false, "dish_name 含逗号（疑似列错位）");

            if (f[9] != null && !f[9].trim().isEmpty()) {
                String cat = f[9].trim();
                if (!(cat.startsWith("c(") && cat.endsWith(")"))) {
                    if (!cat.matches("^[A-Za-z /&-]+$"))
                        return new ValidationResult(false, "category 含非法字符");
                }
            }

            if (f[23] != null && !f[23].trim().isEmpty()) {
                String val = f[23].trim();
                if (val.contains(".")) {
                    String[] parts = val.split("\\.");
                    // 有小数点后内容，且不是全为0，则报错
                    if (parts.length > 1 && !parts[1].matches("0+")) {
                        return new ValidationResult(false, "servings 小数部分不为0");
                    }
                }
                Double.parseDouble(val);
            }


            return new ValidationResult(true, "OK");

        } catch (NumberFormatException e) {
            return new ValidationResult(false, "数字解析错误: " + e.getMessage());
        } catch (Exception e) {
            return new ValidationResult(false, "未知错误: " + e.getMessage());
        }
    }
}
