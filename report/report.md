# SUSTech CS307 Project Part I — Report
> Team: <12410148> & <12410303>    
> Date: 2025-11-8  
> Course: CS307 Fall 2025

[TOC]

## 1. 群组信息
- **成员1**: 刘以煦 12410148
- **成员2**: 刘君昊 12410303


## 2. 项目背景
- #### 项目介绍： 
根据课程提供的SUSTC食谱数据集设计标准数据库管理方式。完成数据库设计、数据快速导入、比较DBMS和文件I/O的性能。
- **原始文件**：`recipes.csv`, `user.csv`, `reviews.csv`


## 3. 任务一： E-R图绘制

- #### 绘图工具
https://online.visual-paradigm.com
- #### E-R 图
![ER diagram](ER_diagram.png)
- #### 说明（详细建表说明将在任务二中指出）
  **strong entity set**: users, review, recipe, keyword, ingredient, category
  **weak entity set**: instruction
  **relationship**:
  - *follows*: followers follow bloggers (N to M)
  - *creates_recipe*: users create recipes (1 to N)
  - *favors_recipe*: users favor recipes (N to M)
  - *likes_review*: users like reviews (N to M)
  - *writes_review*: users write reviews (1 to N)
  - *has_review*: recipes have reviews (1 to N)
  - *has_keyword*: recipes have keywords (N to M)
  - *has_ingredient*: recipes have ingredients (N to M)
  - *has_instruction*: recpes have instructions (1 to N)
  - *has_catregory*: recipes have categorties (N to M)

## 4. 任务二： 数据库设计
- #### 数据库图
![diagram_visualization](diagram_visualization.png)
- #### 建表说明
  #### users表
  **主键**：`author_id`    
  **属性**：  
  - `author_name`：用户名  
  - `gender`：性别，只取 `'Male'`、`'Female'`  
  - `age`：年龄  
  - `following_cnt`：派生属性，由 `follows` 表计算（该用户关注的人数）  
  - `follower_cnt`：派生属性，由 `follows` 表计算（关注该用户的人数）  
  #### follows表
   **说明**：表示用户之间的关注关系，自连接实现。
  **主键**：`(blogger_id, follower_id)`  
  **外键**：  
  - `blogger_id` → `users(author_id)` （被关注者）  
  - `follower_id` → `users(author_id)` （粉丝） 


  #### recipe表
  **主键**：`recipe_id`  
  **外键**：`author_id` → `users(author_id)`  
  **属性**：  
  - `dish_name`：菜品名  
  - `date_published`：发布日期  
  - `cook_time`：烹饪时间（INTERVAL）  
  - `prep_time`：准备时间（INTERVAL）  
  - `description`：食谱描述  
  - `aggr_rating`：派生属性，由`review`表计算（平均评分）  
  - `review_cnt`：派生属性，由`review`表计算（评论数） 
  - `yield`：产出量（带单位）  
  - `servings`：可供人数  
  - `calories`：卡路里  
  - `fat` / `saturated_fat` / `cholesterol` / `sodium` / `carbohydrate` / `fiber` / `sugar` / `protein`：营养信息  

  #### favors_recipe表
  **说明**：用户收藏食谱的关系表（多对多）
  **主键**：`(author_id, recipe_id)`  
  **外键**：  
  - `author_id` → `users(author_id)`  
  - `recipe_id` → `recipe(recipe_id)`  
   
  #### review表
  **主键**：`review_id`  
  **外键**：  
  - `recipe_id` → `recipe(recipe_id)`  
  - `author_id` → `users(author_id)`  
  **属性**：  
  - `rating`：评分  
  - `review_text`：评论内容  
  - `date_submit`：提交日期  
  - `date_modify`：修改日期  

  #### likes_review表
  **说明**：用户点赞评论的关系表（多对多）
  **主键**：`(author_id, review_id)`  
  **外键**：  
  - `author_id` → `users(author_id)`  
  - `review_id` → `review(review_id)`  


  #### keyword表
  **主键**：`keyword_id`  
  **属性**：  
  - `keyword_name`：关键词内容（唯一）  


  #### ingredient表
  **主键**：`ingredient_id`   
  **属性**：  
  - `ingredient_name`：食材名（唯一）  

  #### instruction表
  **说明**：弱实体，依附于 `recipe` 实体
  **主键**：`(recipe_id, step_no)`  
  **外键**：  
  - `recipe_id` → `recipe(recipe_id)`  
  **属性**：  
  - `step_no`：步骤序号  
  - `instruction_text`：操作说明  

  #### has_keyword表
  **说明**：`recipe` 与 `keyword` 的多对多关系表
  **主键**：`(recipe_id, keyword_id)`  
  **外键**：  
  - `recipe_id` → `recipe(recipe_id)`  
  - `keyword_id` → `keyword(keyword_id)`    

  #### has_ingredient表
  **说明**：`recipe` 与 `ingredient` 的多对多关系表
  **主键**：`(recipe_id, ingredient_id)`  
  **外键**：  
  - `recipe_id` → `recipe(recipe_id)`  
  - `ingredient_id` → `ingredient(ingredient_id)`  


  #### category表
  **主键**：`category_id`  
  **属性**：  
  - `category_name`：分类名（唯一）  

  #### has_category表
  **说明**：`recipe` 与 `category` 的多对多关系表
  **主键**：`(recipe_id, category_id)`  
  **外键**：  
  - `recipe_id` → `recipe(recipe_id)`  
  - `category_id` → `category(category_id)`  
  
## 5. 任务三： 数据导入
## 6. 任务四： 比较DBMS与文件I/O
## 7. 任务五： 高并发查询处理
- #### 测试环境
  - **硬件配置**
    - *CPU型号*： 12th Gen Intel(R) Core(TM) i9-12900H @ 2.50 GHz (14 cores/ 20 threads)
    - *内存大小*：64 GB DDR5 @ 4800 MT/s
  - **软件环境**
    - *DBMS*: PostgreSQL 17.4 on x86_64-windows 
    - *JDK*: JDK 11 （pom.xml指定）
    - *构建工具*： Apache Maven 3.9.9
    - *主要依赖*：
      PostgreSQL JDBC 驱动：org.postgresql:postgresql:42.7.3
      CSV 文件解析库：com.opencsv:opencsv:5.10
    - *项目编码*： UTF-8

- #### 性能分析方法
  - 任务五选择平均每条查询耗时作为性能指标。人为指定线程数、查询总数（模拟高并发），生成随机`recipe_id`。`recipe`表格作为我们数据库设计中数据量最大的单个表格，适用于高并发性能探索
  - 在本实验的高并发性能分析中，我们选择使用 `SELECT` 语句而非 `INSERT`、`DELETE` 或 `UPDATE`
  其原因在于：
    - `SELECT` 是只读操作，不会修改数据库状态，能够安全地在多线程环境下并发执行
    - 写操作（如 `INSERT`/`DELETE`）会引发行锁或页锁竞争，掩盖数据库真实的查询性能


- #### 预查询优化
  为提高查询效率，我们针对高频查询字段建立了多组索引。
例如，在 `review` 表中对 `recipe_id` 建立 B-Tree 索引后，通过 EXPLAIN ANALYZE 观察到执行计划由 Seq Scan 变为 Index Scan，execution time 由 187.05 ms 降至 0.13 ms。因此，在接下来的高并发查询处理中，我们使用建立索引的方式优化基本的查询性能。以下三种探索均用到索引。

>todo: baseline --> Connection Pooling (HikariCP) --> PreparedStatement 
Cache + Warm-up

- #### Baseline（未能实现高并发，但有启发意义）
  - **基线搭建**：
    使用 Java 多线程（50 线程、共 10,000 次查询）执行：
    ```sql
    SELECT * FROM recipe WHERE recipe_id = ?
    ```
    每次查询独立创建并关闭 JDBC 连接，日志记录至 `logs/baseline_test.log`

  - **问题**：
    实验运行后程序停滞在启动阶段，CPU 占用升高但无输出结果。
    在 PostgreSQL 中执行：
    ```sql
    SELECT count(*) FROM pg_stat_activity WHERE datname = 'sustc_db';
    ```
    仅返回 **1**，表明数据库中只有一个活动连接，原因是每次查询都独立建立连接，造成瞬时大量连接请求。  
    而PostgreSQL 默认 `max_connections ≈ 100`，其余线程被阻塞或排队，导致系统无法真正并发执行。

  - **解决方案（Solution）**：
    下一阶段引入 **连接池（HikariCP）** 优化。连接池预先建立固定数量（如 50 个）可复用连接，避免频繁连接创建与销毁带来的高昂开销。该优化预期可显著提升并发吞吐量（QPS），并在数据库中维持稳定的 50 个活动连接。


