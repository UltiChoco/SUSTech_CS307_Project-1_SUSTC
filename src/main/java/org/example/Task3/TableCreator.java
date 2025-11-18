package org.example.Task3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TableCreator {
    private Connection connection;
    private String schema;
    private String sql = "CREATE TABLE IF NOT EXISTS users (\n" +
            "                       author_id      SERIAL PRIMARY KEY,\n" +
            "                       author_name    VARCHAR(100) NOT NULL,\n" +
            "                       gender         VARCHAR(10)  CHECK (gender IN ('Male','Female')),\n" +
            "                       age            INT,\n" +
            "                       following_cnt  INT,\n" +
            "                       follower_cnt   INT\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS follows (\n" +
            "                         blogger_id  INT REFERENCES users(author_id),\n" +
            "                         follower_id INT REFERENCES users(author_id),\n" +
            "                         PRIMARY KEY (blogger_id, follower_id)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS recipe (\n" +
            "                        recipe_id       SERIAL PRIMARY KEY,\n" +
            "                        author_id       INT REFERENCES users(author_id),\n" +
            "                        dish_name       VARCHAR(150) NOT NULL,\n" +
            "                        date_published  DATE,\n" +
            "                        cook_time       INTERVAL,\n" +
            "                        prep_time       INTERVAL,\n" +
            "                        description     TEXT,\n" +
            "                        aggr_rating     DECIMAL(3,2),\n" +
            "                        review_cnt      INT,\n" +
            "                        yield           VARCHAR(100),\n" +
            "                        servings       VARCHAR(50),\n" +
            "                        calories        DECIMAL(8,2),\n" +
            "                        fat             DECIMAL(8,2),\n" +
            "                        saturated_fat   DECIMAL(8,2),\n" +
            "                        cholesterol     DECIMAL(8,2),\n" +
            "                        sodium          DECIMAL(10,2),\n" +
            "                        carbohydrate    DECIMAL(8,2),\n" +
            "                        fiber           DECIMAL(8,2),\n" +
            "                        sugar           DECIMAL(8,2),\n" +
            "                        protein         DECIMAL(8,2)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS favors_recipe (\n" +
            "                               author_id INT REFERENCES users(author_id),\n" +
            "                               recipe_id INT REFERENCES recipe(recipe_id),\n" +
            "                               PRIMARY KEY (author_id, recipe_id)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS review (\n" +
            "                        review_id     SERIAL PRIMARY KEY,\n" +
            "                        recipe_id     INT REFERENCES recipe(recipe_id),\n" +
            "                        author_id     INT REFERENCES users(author_id),\n" +
            "                        rating        DECIMAL(3,2),\n" +
            "                        review_text   TEXT,\n" +
            "                        date_submit   DATE,\n" +
            "                        date_modify   DATE\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS likes_review (\n" +
            "                              author_id INT REFERENCES users(author_id),\n" +
            "                              review_id INT REFERENCES review(review_id),\n" +
            "                              PRIMARY KEY (author_id, review_id)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS keyword (\n" +
            "                         keyword_id   SERIAL PRIMARY KEY,\n" +
            "                         keyword_name VARCHAR(100) UNIQUE NOT NULL\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS ingredient (\n" +
            "                            ingredient_id   SERIAL PRIMARY KEY,\n" +
            "                            ingredient_name VARCHAR(100) UNIQUE NOT NULL\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS instruction (\n" +
            "                             recipe_id        INT REFERENCES recipe(recipe_id),\n" +
            "                             step_no          INT,\n" +
            "                             instruction_text TEXT,\n" +
            "                             PRIMARY KEY (recipe_id, step_no)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS has_keyword (\n" +
            "                             recipe_id  INT REFERENCES recipe(recipe_id),\n" +
            "                             keyword_id INT REFERENCES keyword(keyword_id),\n" +
            "                             PRIMARY KEY (recipe_id, keyword_id)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS has_ingredient (\n" +
            "                                recipe_id    INT REFERENCES recipe(recipe_id),\n" +
            "                                ingredient_id INT REFERENCES ingredient(ingredient_id),\n" +
            "                                PRIMARY KEY (recipe_id, ingredient_id)\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS category (\n" +
            "                          category_id   SERIAL PRIMARY KEY,\n" +
            "                          category_name VARCHAR(100) UNIQUE NOT NULL\n" +
            ");\n" +
            "\n" +
            "CREATE TABLE IF NOT EXISTS has_category (\n" +
            "                              recipe_id   INT REFERENCES recipe(recipe_id),\n" +
            "                              category_id INT REFERENCES category(category_id),\n" +
            "                              PRIMARY KEY (recipe_id, category_id)\n" +
            ");\n";
    public TableCreator(String url, String user ,String password,String schema){
        this.schema = schema;
        try{
            this.connection = DriverManager.getConnection(url,user,password);
        }catch (SQLException e){
            System.out.println(e);
        }
    }

    public void createTable() {
        try {
            String createSchemaSql = "CREATE SCHEMA IF NOT EXISTS " + schema;
            try (var statement = connection.createStatement()) {
                statement.execute(createSchemaSql);
                System.out.println("Schema '" + schema + "' created or already exists.");
            }

            String setSchemaSql = "SET search_path TO " + schema;
            try (var statement = connection.createStatement()) {
                statement.execute(setSchemaSql);
                System.out.println("Switched to schema '" + schema + "'.");
            }

            try (var statement = connection.createStatement()) {
                statement.execute(sql);
                System.out.println("Tables created successfully in schema '" + schema + "'.");
            }
        } catch (SQLException e) {
            System.err.println("Error creating schema or tables: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
