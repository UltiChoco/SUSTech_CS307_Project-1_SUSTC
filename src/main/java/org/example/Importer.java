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
import java.util.stream.Collectors;

import static java.lang.Double.parseDouble;

public class Importer {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/sustc_db";
        String user = "postgres";
        String password = "676767";
        String schema = "public";
        String recipe_filepath = "src/main/resources/recipes.csv";
        String reviews_filepath = "src/main/resources/reviews.csv";
        String user_filepath = "src/main/resources/user.csv";
        //请先完善以上信息

        TableCreator tableCreator = new TableCreator(url,user,password,schema);
        tableCreator.createTable();
        boolean del = true;
        Importer importer = new Importer(url,user,password,schema,del,recipe_filepath,reviews_filepath,user_filepath);
        long start = System.currentTimeMillis();
        System.out.println("processing csv_file...");
        importer.processCSV(recipe_filepath);
        importer.processCSV(reviews_filepath);
        System.out.println("finish");
        importer.copyToUsers();
        importer.copyToFollowers();
        importer.copyToRecipe();
        importer.copyToFavors_recipe();
        importer.copyToReview();
        importer.copyToLikes_review();
        importer.copyToKeyword();
        importer.copyToIngredient();
        importer.copyToInstruction();
        importer.copyToCategory();
        importer.copyToHas_keyword();
        importer.copyToHas_Ingredient();
        importer.copyToHas_Category();

        long end = System.currentTimeMillis();
        System.out.println((end-start)/1000.0);
    }

    private final Connection connection;
    private final Copy copy;
    private final String schema;
    private final boolean del;
    private final String recipe_filepath;
    private final String reviews_filepath;
    private final String user_filepath;
    public Importer(String url,String user,String password,String schema,boolean del,String recipe_filepath,String reviews_filepath,String user_filepath){
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

    public void copyToUsers(){
        String table = schema+".users";
        String filepath = user_filepath;
        String value_list = "author_id,author_name,gender,age,following_cnt,follower_cnt";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();
        try {
            if(del){connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");}
            csvReader = new CSVReader(new FileReader(filepath));
            String[] nextLine;
            csvReader.readNext();
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL USERS");
            int cnt = 0;
            while ((nextLine = csvReader.readNext()) != null) {
                for (int i = 0; i < 6; i++) {
                    String value = nextLine[i];
                    csvBuilder.append(value);
                    if (i < 5) {
                        csvBuilder.append("|");
                    }
                }
                csvBuilder.append("\n");
                cnt++;
                bar.update(cnt*100/299892);
            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder,"|");
            System.out.println("finish");

            // 同步序列，让自增ID与最大author_id对齐
            String seqUpdate = "SELECT setval(pg_get_serial_sequence('" + table + "', 'author_id'), "
                    + "(SELECT MAX(author_id) FROM " + table + "));";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(seqUpdate);
                connection.commit();
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

    public void copyToFollowers() {
        String table = schema + ".follows";
        String filepath = user_filepath;
        String value_list = "blogger_id,follower_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();
        try {
            if(del){connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");}
            csvReader = new CSVReader(new FileReader(filepath));
            String[] nextLine;
            csvReader.readNext();
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL FOLLOWERS");
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
                        csvBuilder.append(bloggerId).append("|").append(followerId).append("\n");
                    }
                }
                cnt++;
                bar.update(cnt*100/299892);
            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");}
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL RECIPE");

            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = new String[25];
                for (int i = 0; i < Math.min(25, nextLine.length); i++) {
                    fields[i] = nextLine[i] == null ? "" : nextLine[i].replace("|", "/").replace("\\","/"); // 转义分隔符
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
                bar.update((int)parseDouble(fields[0])*100/522517);
                }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");

            //同步序列，确保 recipe_id 自增与最大 id 对齐
            String seqUpdate = "SELECT setval(pg_get_serial_sequence('" + table + "', 'recipe_id'), "
                    + "(SELECT MAX(recipe_id) FROM " + table + "));";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(seqUpdate);
                connection.commit();
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

    public void copyToFavors_recipe(){
        String table = schema + ".favors_recipe";
        String filepath = recipe_filepath;

        String value_list = "author_id,recipe_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");}
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL FAVORS RECIPE");

            while ((nextLine = csvReader.readNext()) != null) {
                String recipe_id = nextLine[0];
                String[] author_idList;
                if(nextLine[26].isEmpty()){
                    continue;
                }else {
                    author_idList = cutString(nextLine[26]).split(",");
                }
                for (int i = 0; i < author_idList.length; i++) {
                    csvBuilder.append(author_idList[i])
                            .append('|').append(nextLine[0])
                            .append("\n");
                }
                bar.update((int)parseDouble(nextLine[0])*100/522517);
            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");}
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL REVIEW");

            while ((nextLine = csvReader.readNext()) != null) {
                String[] fields = new String[9];
                for (int i = 0; i < Math.min(9, nextLine.length); i++) {
                    fields[i] = nextLine[i] == null ? "" : nextLine[i].replace("|", "/").replace("\\","/");
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
                bar.update((int)parseDouble(nextLine[0])*100/1401982);
            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");

            // 同步序列，确保 review_id 自增与最大 id 对齐
            String seqUpdate = "SELECT setval(pg_get_serial_sequence('" + table + "', 'review_id'), "
                    + "(SELECT MAX(review_id) FROM " + table + "));";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(seqUpdate);
                connection.commit();
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

    public void copyToLikes_review(){
        String table = schema + ".likes_review";
        String filepath = reviews_filepath;

        String value_list = "author_id,review_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");}
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL LIKES REVIEW");

            while ((nextLine = csvReader.readNext()) != null) {

                String[] fields = new String[9];

                for (int i = 0; i < Math.min(9, nextLine.length); i++) {
                    fields[i] = nextLine[i] == null ? "" : nextLine[i].replace("|", "/").replace("\\","/");
                }
                if (fields[1].isEmpty()){
                    continue;
                }
                String[] author_idList;
                if(nextLine[8].isEmpty()){
                    continue;
                }else {
                    author_idList = cutString(nextLine[8]).split(",");
                }
                for (int i = 0; i < author_idList.length; i++) {
                    csvBuilder.append(author_idList[i])
                            .append('|').append(nextLine[0])
                            .append("\n");
                }

                bar.update((int)parseDouble(nextLine[0])*100/1401982);

            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToKeyword(){
        String table = schema + ".keyword";
        String filepath = recipe_filepath;

        String value_list = "keyword_name";

        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);

        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
                String sequenceName = table + "_keyword_id_seq";
                connection.createStatement().execute("ALTER SEQUENCE " + sequenceName + " RESTART WITH 1");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            List<String> keylist = new ArrayList<>();
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL KEYWORD");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                if(nextLine[10].isEmpty()){
                    continue;
                }
                String[] keys;
                if(nextLine[10].startsWith("c(") && nextLine[10].endsWith(")")){
                    keys = nextLine[10].substring(2,nextLine[10].length()-1).split(", \"");
                }else{
                    keys = nextLine[10].split(", \"");
                }

                for (int i = 0; i < keys.length; i++) {
                    keylist.add(cutString(keys[i].trim()).replace("|","/").replace("\\","/"));
                }
            }
            keylist.stream()
                    .distinct()
                    .forEach(key->{
                        csvBuilder.append(key).append("\n");
                    });
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToIngredient(){
        String table = schema + ".ingredient";
        String filepath = recipe_filepath;

        String value_list = "ingredient_name";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
                String sequenceName = table + "_ingredient_id_seq";
                connection.createStatement().execute("ALTER SEQUENCE " + sequenceName + " RESTART WITH 1");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            List<String> keylist = new ArrayList<>();
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL INGREDIENT");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                if(nextLine[11].isEmpty()){
                    continue;
                }

                String[] keys;
                if(nextLine[11].startsWith("c(") && nextLine[11].endsWith(")")){
                    keys = nextLine[11].substring(2,nextLine[11].length()-1).split(", \"");
                }else{
                    keys = nextLine[11].split(", \"");
                }
                for (int i = 0; i < keys.length; i++) {
                    keylist.add(cutString(keys[i].trim()));
                }
            }
            keylist.stream()
                    .distinct()
                    .forEach(key->{
                        String unikey = key.replace("|","/").replace("\\","/");
                        csvBuilder.append(unikey).append("\n");
                    });
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToInstruction(){
        String table = schema + ".instruction";
        String filepath = recipe_filepath;

        String value_list = "recipe_id,step_no,instruction_text";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL INSTRUCTION");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                nextLine[25] = cutString(nextLine[25]);
                if(nextLine[25].isEmpty()){
                    continue;
                }
                String[] steps;
                if(nextLine[25].startsWith("c(") && nextLine[25].endsWith(")")){
                    steps = nextLine[25].substring(2,nextLine[25].length()-1).split("\", \"");
                }else {
                    steps = nextLine[25].split("\", \"");
                }

                for (int i = 0; i < steps.length; i++) {
                    csvBuilder.append(nextLine[0]).append("|")
                            .append(i+1).append("|")
                            .append(cutString(steps[i].trim()).replace("\\","/").replace("|","/")).append("\n");
                }
            }

            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToCategory(){
        String table = schema + ".category";
        String filepath = recipe_filepath;

        String value_list = "category_name";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
                String sequenceName = table + "_category_id_seq";
                connection.createStatement().execute("ALTER SEQUENCE " + sequenceName + " RESTART WITH 1");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            List<String> categorylist = new ArrayList<>();
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL CATEGORY");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                if(nextLine[9].isEmpty()){
                    continue;
                }
                categorylist.add(nextLine[9]);
            }
            categorylist.stream()
                    .distinct()
                    .forEach(category->{
                        String unikey = category.replace("|","/").replace("\\","/");
                        csvBuilder.append(category).append("\n");
                    });
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToHas_keyword(){
        String table = schema + ".has_keyword";
        String filepath = recipe_filepath;

        String value_list = "recipe_id,keyword_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);

        Map<String,Integer> keyword = getAllKeywords();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL HAS_KEYWORD");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                if(nextLine[10].isEmpty()){
                    continue;
                }
                String[] keys;
                if (nextLine[10].startsWith("c(") && nextLine[10].endsWith(")")){
                    keys = nextLine[10].substring(2,nextLine[10].length()-1).split(", \"");
                }else {
                    keys = nextLine[10].split(", \"");
                }
                for (int i = 0; i < keys.length; i++) {
                    csvBuilder.append(nextLine[0]).append("|")
                            .append(keyword.get(cutString(keys[i].trim()).replace("|","/").replace("\\","/"))).append("\n");
                }
            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToHas_Ingredient(){
        String table = schema + ".has_ingredient";
        String filepath = recipe_filepath;

        String value_list = "recipe_id,ingredient_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);
        Map<String,Integer> integerMap = getAllIngredient();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL HAS_INGREDIENT");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                if(nextLine[11].isEmpty()){
                    continue;
                }

                String[] keys;
                if(nextLine[11].startsWith("c(") && nextLine[11].endsWith(")")){
                    keys = nextLine[11].substring(2,nextLine[11].length()-1).split(", \"");
                }else{
                    keys = nextLine[11].split(", \"");
                }
                List<String> keylist = Arrays.stream(keys)
                        .map(key->cutString(key.trim()).replace("|","/").replace("\\","/"))
                        .distinct()
                        .collect(Collectors.toList());
                for (int i = 0; i < keylist.size(); i++) {
                    csvBuilder.append(nextLine[0]).append("|")
                            .append(integerMap.get(keylist.get(i))).append("\n");
                }
            }

            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void copyToHas_Category(){
        String table = schema + ".has_category";
        String filepath = recipe_filepath;

        String value_list = "recipe_id,category_id";
        CSVReader csvReader = null;
        StringBuilder csvBuilder = new StringBuilder(1024*1024);
        Map<String,Integer> allCategory = getAllCategory();
        try {
            CSVParser csvParser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withEscapeChar('￥')
                    .withQuoteChar('"')
                    .build();
            if(del){
                connection.createStatement().execute("TRUNCATE TABLE " + table + " CASCADE");
            }
            csvReader = new CSVReaderBuilder(new FileReader(filepath))
                    .withCSVParser(csvParser)
                    .build();

            String[] header = csvReader.readNext();
            String[] nextLine;
            ConsoleProgressBar bar = new ConsoleProgressBar(100);
            bar.setPrefix("FILL HAS_CATEGORY");

            while ((nextLine = csvReader.readNext()) != null) {
                bar.update((int)parseDouble(nextLine[0])*100/522517);
                if(nextLine[9].isEmpty()){
                    continue;
                }
                int category_id = allCategory.get(nextLine[9].replace("|","/").replace("\\","/"));
                csvBuilder.append(nextLine[0]).append("|")
                        .append(category_id).append("\n");
            }
            System.out.println("committing...");
            copy.copyTo(table, value_list, csvBuilder, "|");
            System.out.println("finish");
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

    public void processCSV(String path){
        List<String> processLine = new ArrayList<>();
        String previous = "";
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))){
            int line_cnt = 1;
            processLine.add(reader.readLine());
            String currentLine;
            while ((currentLine = reader.readLine())!= null){
                String id = currentLine.split(",")[0];
                if (id.equals(""+line_cnt)){
                    if (!previous.isEmpty()){processLine.add(previous);}
                    previous = currentLine;
                    line_cnt++;
                }else{
                    previous += currentLine;
                }
            }
            if (previous!=null){
                processLine.add(previous);
            }
        }catch (IOException e){
            System.out.println(e);
        }
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8))) {
            for (String line : processLine) {
                writer.write(line);
                writer.newLine();
            }
        }catch (IOException e){
            System.out.println(e);
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
