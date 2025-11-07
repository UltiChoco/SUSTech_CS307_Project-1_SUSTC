# 代码使用说明

## 重要提醒
recipe中的yield和sodium都存在数据范围过小的问题，在运行代码之前请先修改这两个数据的范围

## 功能概述
该代码库主要用于将CSV文件数据导入到PostgreSQL数据库中，包含CSV数据读取、进度显示、数据库批量插入等功能，适用于批量数据迁移场景。

## 核心类说明

### 1. `Copy` 类
- **功能**：基于PostgreSQL的`CopyManager`实现高效批量数据插入
- **主要方法**：
  - `copyTo(String table, String value_list, StringBuilder csvBuilder, String dev)`：将CSV格式数据批量插入指定表
    - 参数：
      - `table`：目标表名（含schema）
      - `value_list`：表字段列表
      - `csvBuilder`：包含CSV数据的字符串构建器
      - `dev`：CSV数据分隔符

### 2. `ConsoleProgressBar` 类
- **功能**：在控制台显示进度条，直观展示数据处理进度
- **主要方法**：
  - `update(int progress)`：更新进度条状态
  - 可通过`setPrefix()`、`setColor()`等方法自定义进度条样式

### 3. `Importer` 类
- **功能**：核心数据导入类，负责读取CSV文件并导入到对应的数据表
- **主要方法**：
  - 各类`copyToXXX()`方法：对应不同数据表的导入逻辑（如`copyToUsers()`、`copyToRecipe()`等）
  - `processCSV()`：预处理CSV文件（示例中未完全展示实现）

## 使用步骤

### 1. 环境准备
- 依赖：PostgreSQL JDBC驱动、OpenCSV（处理CSV文件）
- 数据库：PostgreSQL 数据库环境，需提前创建对应的数据表结构

### 2. 配置修改
在`Importer`类的`main`方法中修改数据库连接信息：
```java
String url = "jdbc:postgresql://localhost:5432/database_project"; // 数据库连接地址
String user = "postgres"; // 数据库用户名
String password = "xxx"; // 数据库密码
String schema = "test"; // 数据库schema
// CSV文件路径
String recipe_filepath = "src/main/resources/recipes.csv";
String reviews_filepath = "src/main/resources/reviews.csv";
String user_filepath = "src/main/resources/user.csv";
```

### 3. 运行方式
- 直接运行`Importer`类的`main`方法
- 程序会按顺序执行以下操作：
  1. 处理CSV文件
  2. 依次将数据导入到各个对应的数据表
  3. 输出总耗时

### 4.在导入数据时遇到的部分问题

在解析recipe.csv时，发现了许多由于解析规则不匹配，数据类型不相符而引发的错误。（review也有类似情况）

- 文件的字符串部分出现了转义符，错误的解开了引号的约束，使得字符串的解析错误
（已经通过修改对转义符的定义解决了该问题）
- 文件中评论数一栏为浮点数而非整数
- 部分行出现了引号未闭合换行的情况（可能原因）导致csv默认解析为完整字符串
（已通过行整理解决）
- 数据超限（yield VARCHAR(100)、sodium DECIMAL(10,2)）
- 数据导入（读取没有问题）时由转义符引起的编码格式报错
- review中部分recipeID为空
