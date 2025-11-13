package org.example;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.Double.parseDouble;

public class Importer_pro {
    public static void main(String[] args) throws SQLException, InterruptedException {
        JsonParamReader jsonParamReader = new JsonParamReader("param.json");
        String url = jsonParamReader.getString("url").orElse("jdbc:postgresql://localhost:5432/database_project");
        String user = jsonParamReader.getString("user").orElse("postgres");
        String password = jsonParamReader.getString("password").orElse("xxxx");
        String schema = jsonParamReader.getString("schema").orElse("project_unlogged");
        String recipe_filepath = jsonParamReader.getString("recipe_filepath").orElse("src/main/resources/recipes.csv");
        String reviews_filepath = jsonParamReader.getString("review_filepath").orElse("src/main/resources/reviews.csv");
        String user_filepath = jsonParamReader.getString("user_filepath").orElse("src/main/resources/user.csv");
        //请先完善以上信息


        TableCreator tableCreator = new TableCreator(url,user,password,schema);
        tableCreator.createTable();
        boolean del = true;
        Importer_pro importer = new Importer_pro(url,user,password,schema,del,recipe_filepath,reviews_filepath,user_filepath);
        long start = System.currentTimeMillis();
        System.out.println("processing csv_file...");

        // Parallelize CSV processing
        ExecutorService csvExecutor = Executors.newFixedThreadPool(3);
        csvExecutor.submit(() -> {
            System.out.println("Processing recipe CSV...");
            importer.processCSV(recipe_filepath, true); // Preprocess recipe file
            System.out.println("Finished processing recipe CSV.");
        });
        csvExecutor.submit(() -> {
            System.out.println("Processing reviews CSV...");
            importer.processCSV(reviews_filepath, true); // Preprocess reviews file
            System.out.println("Finished processing reviews CSV.");
        });
        csvExecutor.submit(() -> {
            System.out.println("Processing user CSV...");
            importer.processCSV(user_filepath, false); // Preprocess user file
            System.out.println("Finished processing user CSV.");
        });
        csvExecutor.shutdown();
        csvExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        System.out.println("finish");

        importer.disableForeignKeyConstraints();

        // Truncate tables *before* creating the ExecutorService
        if (del) {
            importer.truncateTables();
        }

        // Use ExecutorService for parallel execution
        ExecutorService executor = Executors.newFixedThreadPool(2); // Adjust thread pool size as needed

        executor.submit(importer::copyToUsers);
        executor.submit(importer::copyToRecipe);

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // Wait for all tasks to complete


        ExecutorService executor0 = Executors.newFixedThreadPool(2);
        executor0.submit(importer::copyToReview);
        executor0.submit(importer::copyToHas);
        executor0.shutdown();
        executor0.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        try {
            importer.connection.commit();
        } catch (SQLException e) {
            importer.connection.rollback();
            throw e;
        }
        importer.enableForeignKeyConstraints();

        long end = System.currentTimeMillis();
        System.out.println((end-start)/1000.0);

    }

    // Add a method to truncate tables
    private void truncateTables() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + schema + ".users CASCADE");
            statement.execute("TRUNCATE TABLE " + schema + ".follows CASCADE");
            statement.execute("TRUNCATE TABLE " + schema + ".recipe CASCADE");
            statement.execute("TRUNCATE TABLE " + schema + ".favors_recipe CASCADE");
            statement.execute("TRUNCATE TABLE " + schema + ".keyword CASCADE");
            String sequenceName2 = schema + ".keyword_keyword_id_seq";
            statement.execute("ALTER SEQUENCE " + sequenceName2 + " RESTART WITH 1");
            statement.execute("TRUNCATE TABLE " + schema + ".category CASCADE");
            String sequenceName3 = schema + ".category_category_id_seq";
            statement.execute("ALTER SEQUENCE " + sequenceName3 + " RESTART WITH 1");
            statement.execute("TRUNCATE TABLE " + schema + ".ingredient CASCADE");
            String sequenceName4 = schema + ".ingredient_ingredient_id_seq";
            statement.execute("ALTER SEQUENCE " + sequenceName4 + " RESTART WITH 1");
            statement.execute("TRUNCATE TABLE " + schema + ".instruction CASCADE");
            statement.execute("TRUNCATE TABLE " + schema + ".review CASCADE");
            statement.execute("TRUNCATE TABLE " + schema + ".likes_review CASCADE");
            connection.commit();
            System.out.println("Tables truncated.");
        } catch (SQLException e) {
            System.err.println("Error truncating tables: " + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw e; // Re-throw the exception to be handled in main
        }
    }

    private final Connection connection;
    private final Copy copy;
    private final String schema;
    private final boolean del;
    private final String recipe_filepath;
    private final String reviews_filepath;
    private final String user_filepath;
    public Importer_pro(String url,String user,String password,String schema,boolean del,String recipe_filepath,String reviews_filepath,String user_filepath){
        this.del = del;
        this.schema = schema;
        this.recipe_filepath = recipe_filepath;
        this.reviews_filepath = reviews_filepath;
        this.user_filepath = user_filepath;
        try {
            this.connection = DriverManager.getConnection(url, user, password);
            this.copy = new Copy(connection);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void disableForeignKeyConstraints() {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET session_replication_role = replica;");
            connection.commit();
            System.out.println("Foreign key constraints disabled.");
        } catch (SQLException e) {
            System.err.println("Error disabling foreign key constraints: " + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            e.printStackTrace();
        }
    }

    private void enableForeignKeyConstraints() {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET session_replication_role = origin;");
            connection.commit();
            System.out.println("Foreign key constraints enabled.");
        } catch (SQLException e) {
            System.err.println("Error enabling foreign key constraints: " + e.getMessage());
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            e.printStackTrace();
        }
    }

    public void copyToUsers(){
        String table = schema+".users";
        String filepath = user_filepath;
        String value_list = "author_id,author_name,gender,age,following_cnt,follower_cnt";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();

        String table1 = schema + ".follows";
        String value_list1 = "blogger_id,follower_id";
        StringBuilder csvBuilder1 = new StringBuilder();
        try {
            // REMOVE TRUNCATE STATEMENTS HERE
            csvReader = new CSVReader(new FileReader(filepath));
            String[] nextLine;
            csvReader.readNext();
            int cnt = 0;
            while ((nextLine = csvReader.readNext()) != null) {

                String bloggerId = nextLine[0];
                String followersStr = nextLine[6];
                if (followersStr != null && followersStr.startsWith("\"\"\"") && followersStr.endsWith("\"\"\"")) {
                    followersStr = followersStr.substring(3, followersStr.length() - 3);
                }else if(followersStr != null && followersStr.startsWith("\"") && followersStr.endsWith("\"")){
                    followersStr = followersStr.substring(1, followersStr.length() - 1);
                }

                if (followersStr != null && !followersStr.isEmpty()) {
                    String[] followerIds = followersStr.split(",");
                    for (String followerId : followerIds) {
                        followerId = followerId.trim();
                        csvBuilder1.append(bloggerId).append("|").append(followerId).append("\n");
                    }
                }


                for (int i = 0; i < 6; i++) {
                    String value = nextLine[i];
                    csvBuilder.append(value);
                    if (i < 5) {
                        csvBuilder.append("|");
                    }
                }
                csvBuilder.append("\n");


                cnt++;

            }
            copy.copyTo(table, value_list, csvBuilder,"|");
            copy.copyTo(table1, value_list1, csvBuilder1, "|");
            // 同步序列，让自增ID与最大author_id对齐
            String seqUpdate = "SELECT setval(pg_get_serial_sequence('" + table + "', 'author_id'), "
                    + "(SELECT MAX(author_id) FROM " + table + "));";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(seqUpdate);
            } catch (SQLException e) {
                System.err.println("Failed to update sequence for " + table);
                e.printStackTrace();
            }

        } catch (SQLException | IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void copyToRecipe() {
        String table = schema + ".recipe";
        String filepath = recipe_filepath;

        String value_list = "recipe_id,author_id,dish_name,date_published,cook_time,prep_time," +
                "description,aggr_rating,review_cnt,yield,servings,calories,fat,saturated_fat," +
                "cholesterol,sodium,carbohydrate,fiber,sugar,protein";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();

        String table1 = schema + ".favors_recipe";
        String value_list1 = "author_id,recipe_id";
        StringBuilder csvBuilder1 = new StringBuilder();

        String table2 = schema + ".keyword";
        String value_list2 = "keyword_name";
        StringBuilder csvBuilder2 = new StringBuilder();
        List<String> keylist = new ArrayList<>();

        String table3 = schema + ".category";
        String value_list3 = "category_name";
        StringBuilder csvBuilder3 = new StringBuilder();
        List<String> categorylist = new ArrayList<>();

        String table4 = schema + ".ingredient";
        String value_list4 = "ingredient_name";
        StringBuilder csvBuilder4 = new StringBuilder();
        List<String> ingredientList = new ArrayList<>();

        String table5 = schema + ".instruction";
        String value_list5 = "recipe_id,step_no,instruction_text";
        StringBuilder csvBuilder5 = new StringBuilder();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            // REMOVE TRUNCATE STATEMENTS HERE
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;

            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = new String[25];
                for (int i = 0; i < Math.min(25, nextLine.length); i++) {
                    fields[i] = nextLine[i] == null ? "" : nextLine[i];
                }
                if(!fields[9].isEmpty()){
                    categorylist.add(fields[9]);
                }

                csvBuilder.append(fields[0])     // recipe_id
                        .append("|").append(fields[2])  // author_id
                        .append("|").append(fields[1])  // dish_name
                        .append("|").append(fields[7])  // date_published
                        .append("|").append(fields[4].isEmpty() ? "PT0S" : fields[4])  // cook_time
                        .append("|").append(fields[5].isEmpty() ? "PT0S" : fields[5])  // prep_time
                        .append("|").append(fields[8])  // description
                        .append("|").append(fields[12].isEmpty() ? 0.0 : fields[12])  // aggr_rating
                        .append("|").append(fields[13].isEmpty() ? 0 : Math.round(parseDouble(fields[13])))  // review_cnt
                        .append("|").append(fields[24]) // yield
                        .append("|").append(fields[23]) // servings
                        .append("|").append(fields[14]) // calories
                        .append("|").append(fields[15]) // fat
                        .append("|").append(fields[16]) // saturated_fat
                        .append("|").append(fields[17]) // cholesterol
                        .append("|").append(fields[18]) // sodium
                        .append("|").append(fields[19]) // carbohydrate
                        .append("|").append(fields[20]) // fiber
                        .append("|").append(fields[21]) // sugar
                        .append("|").append(fields[22]) // protein
                        .append("\n");

                String[] author_idList;
                if(!nextLine[26].isEmpty()){
                    author_idList = cutString(nextLine[26]).split(",");
                    for (int i = 0; i < author_idList.length; i++) {
                        csvBuilder1.append(author_idList[i])
                                .append('|').append(nextLine[0])
                                .append("\n");
                    }

                    if(!nextLine[10].isEmpty()){
                        String[] keys;
                        if(nextLine[10].startsWith("c(") && nextLine[10].endsWith(")")){
                            keys = nextLine[10].substring(2,nextLine[10].length()-1).split(", \"");
                        }else{
                            keys = nextLine[10].split(", \"");
                        }

                        for (int i = 0; i < keys.length; i++) {
                            keylist.add(cutString(keys[i].trim()));
                        }
                    }

                    if(!nextLine[11].isEmpty()){
                        String[] keys;
                        if(nextLine[11].startsWith("c(") && nextLine[11].endsWith(")")){
                            keys = nextLine[11].substring(2,nextLine[11].length()-1).split(", \"");
                        }else{
                            keys = nextLine[11].split(", \"");
                        }
                        for (int i = 0; i < keys.length; i++) {
                            ingredientList.add(cutString(keys[i].trim()));
                        }
                    }

                    nextLine[25] = cutString(nextLine[25]);
                    if(!nextLine[25].isEmpty()){
                        String[] steps;
                        if(nextLine[25].startsWith("c(") && nextLine[25].endsWith(")")){
                            steps = nextLine[25].substring(2,nextLine[25].length()-1).split("\", \"");
                        }else {
                            steps = nextLine[25].split("\", \"");
                        }

                        for (int i = 0; i < steps.length; i++) {
                            csvBuilder5.append(nextLine[0]).append("|")
                                    .append(i+1).append("|")
                                    .append(cutString(steps[i].trim())).append("\n");
                        }
                    }
                }
            }

            keylist.stream()
                    .distinct()
                    .forEach(key->{
                        csvBuilder2.append(key).append("\n");
                    });

            categorylist.stream()
                    .distinct()
                    .forEach(category->{
                        csvBuilder3.append(category).append("\n");
                    });

            ingredientList.stream()
                    .distinct()
                    .forEach(ingredient->{
                        csvBuilder4.append(ingredient).append("\n");
                    });

            copy.copyTo(table, value_list, csvBuilder, "|");
            copy.copyTo(table1, value_list1, csvBuilder1, "|");
            copy.copyTo(table2, value_list2, csvBuilder2, "|");
            copy.copyTo(table3, value_list3, csvBuilder3, "|");
            copy.copyTo(table4, value_list4, csvBuilder4, "|");
            copy.copyTo(table5, value_list5, csvBuilder5, "|");
            //同步序列，确保 recipe_id 自增与最大 id 对齐
            String seqUpdate = "SELECT setval(pg_get_serial_sequence('" + table + "', 'recipe_id'), "
                    + "(SELECT MAX(recipe_id) FROM " + table + "));";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(seqUpdate);
            } catch (SQLException e) {
                System.err.println("Failed to update sequence for " + table);
                e.printStackTrace();
            }

        } catch (SQLException | IOException | CsvValidationException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    public void copyToReview(){
        String table = schema + ".review";
        String filepath = reviews_filepath;
        String value_list = "review_id,recipe_id,author_id,rating,review_text,date_submit,date_modify";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();


        String table1 = schema + ".likes_review";
        String value_list1 = "author_id,review_id";
        StringBuilder csvBuilder1 = new StringBuilder();

        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            // REMOVE TRUNCATE STATEMENTS HERE
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;

            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = new String[9];
                for (int i = 0; i < Math.min(9, nextLine.length); i++) {
                    fields[i] = nextLine[i] == null ? "" : nextLine[i];
                }
                if (fields[1].isEmpty()){
                    continue;
                }
                csvBuilder.append(fields[0])
                        .append('|').append(Math.round(parseDouble(fields[1])))
                        .append('|').append(fields[2])
                        .append('|').append(fields[4])
                        .append('|').append(fields[5])
                        .append('|').append(fields[6])
                        .append('|').append(fields[7])
                        .append("\n");


                String[] author_idList;
                if(nextLine[8].isEmpty()){
                    continue;
                }else {
                    author_idList = cutString(nextLine[8]).split(",");
                }
                for (int i = 0; i < author_idList.length; i++) {
                    csvBuilder1.append(author_idList[i])
                            .append('|').append(nextLine[0])
                            .append("\n");
                }
            }
            copy.copyTo(table, value_list, csvBuilder, "|");
            copy.copyTo(table1, value_list1, csvBuilder1, "|");
            // 同步序列，确保 review_id 自增与最大 id 对齐
            String seqUpdate = "SELECT setval(pg_get_serial_sequence('" + table + "', 'review_id'), "
                    + "(SELECT MAX(review_id) FROM " + table + "));";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(seqUpdate);
            } catch (SQLException e) {
                System.err.println("Failed to update sequence for " + table);
                e.printStackTrace();
            }

        } catch (SQLException | IOException | CsvValidationException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void copyToHas(){
        String table = schema + ".has_keyword";
        String filepath = recipe_filepath;

        String value_list = "recipe_id,keyword_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);

        Map<String,Integer> keyword = getAllKeywords();


        String table1 = schema + ".has_ingredient";

        String value_list1 = "recipe_id,ingredient_id";
        StringBuilder csvBuilder1 = new StringBuilder(1024*1024);
        Map<String,Integer> integerMap = getAllIngredient();

        String table2 = schema + ".has_category";

        String value_list2 = "recipe_id,category_id";
        StringBuilder csvBuilder2 = new StringBuilder(1024*1024);
        Map<String,Integer> allCategory = getAllCategory();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            // REMOVE TRUNCATE STATEMENTS HERE
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;

            while ((nextLine = csvReader.readNext()) != null) {
                if(!nextLine[10].isEmpty()){
                    String[] keys;
                    if (nextLine[10].startsWith("c(") && nextLine[10].endsWith(")")){
                        keys = nextLine[10].substring(2,nextLine[10].length()-1).split(", \"");
                    }else {
                        keys = nextLine[10].split(", \"");
                    }
                    for (int i = 0; i < keys.length; i++) {
                        int key = keyword.getOrDefault(cutString(keys[i].trim()),-1);

                        if (key != -1) {
                            csvBuilder.append(nextLine[0]).append("|").append(keyword.get(cutString(keys[i].trim()))).append("\n");
                        }
                    }
                }


                if(!nextLine[11].isEmpty()){
                    String[] keys;
                    if(nextLine[11].startsWith("c(") && nextLine[11].endsWith(")")){
                        keys = nextLine[11].substring(2,nextLine[11].length()-1).split(", \"");
                    }else{
                        keys = nextLine[11].split(", \"");
                    }
                    List<String> keylist = Arrays.stream(keys)
                            .map(key->cutString(key.trim()))
                            .distinct()
                            .collect(Collectors.toList());
                    for (int i = 0; i < keylist.size(); i++) {
                        int key = integerMap.getOrDefault(keylist.get(i),-1);
                        if(key != -1){
                            csvBuilder1.append(nextLine[0]).append("|")
                                    .append(integerMap.get(keylist.get(i))).append("\n");
                        }
                    }
                }

                if(!nextLine[9].isEmpty()){
                    int category_id = allCategory.getOrDefault(nextLine[9],-1);
                    if(category_id != -1){
                        csvBuilder2.append(nextLine[0]).append("|")
                                .append(category_id).append("\n");
                    }

                }
            }

            copy.copyTo(table, value_list, csvBuilder, "|");
            copy.copyTo(table1, value_list1, csvBuilder1, "|");
            copy.copyTo(table2, value_list2, csvBuilder2, "|");

        } catch (SQLException | IOException | CsvValidationException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public Map<String, Integer> getAllKeywords() {
        String sql = "SELECT keyword_id, keyword_name FROM "+schema+".keyword";
        Map<String, Integer> result = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String keywordName = rs.getString("keyword_name");
                Integer keywordId = rs.getInt("keyword_id");

                if (keywordName != null) {
                    result.put(keywordName.trim(), keywordId);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Map<String, Integer> getAllIngredient() {
        String sql = "SELECT ingredient_id, ingredient_name FROM "+schema+".ingredient";
        Map<String, Integer> result = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String ingredient_name = rs.getString("ingredient_name");
                Integer ingredient_id = rs.getInt("ingredient_id");

                if (ingredient_name != null) {
                    result.put(ingredient_name.trim(), ingredient_id);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Map<String, Integer> getAllCategory() {
        String sql = "SELECT category_id, category_name FROM "+schema+".category";
        Map<String, Integer> result = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String category_name = rs.getString("category_name");
                Integer category_id = rs.getInt("category_id");

                if (category_name != null) {
                    result.put(category_name.trim(), category_id);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void processCSV(String path, boolean replaceInvalidChars) {
        List<String> processedLines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (replaceInvalidChars) {
                    line = line.replace("|", "/").replace("\\", "/");
                }
                processedLines.add(line);
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + path + " - " + e.getMessage());
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            for (String line : processedLines) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("Error writing to file: " + path + " - " + e.getMessage());
        }
    }


    public String cutString(String s) {
        if (s == null || s.length() < 2) {
            return s;
        }

        int start = 0;
        int end = s.length() - 1;

        while (start <= end && s.charAt(start) == '"') {
            start++;
        }
        while (end >= start && s.charAt(end) == '"') {
            end--;
        }
        return start > end ? "" : s.substring(start, end + 1);
    }
}