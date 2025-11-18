# DBMS vs File I/O 性能对比测试工具

## 项目简介

本工具用于对比数据库管理系统(DBMS)与文件I/O（包括传统文件操作和Stream API操作）在常见数据操作（SELECT、INSERT、UPDATE、DELETE）上的性能差异。通过多次执行相同操作并计算平均耗时，帮助开发者理解不同数据存储方式在性能上的优劣。

## 功能说明

工具主要实现了以下三种数据操作方式的性能测试：

1. **PostgreSQL数据库操作**：通过JDBC连接数据库，执行SQL语句
2. **传统文件I/O操作**：使用BufferedReader/BufferedWriter操作CSV文件
3. **文件Stream操作**：使用Java NIO的Stream API操作CSV文件

每种操作方式都会测试四种基本数据操作：
- SELECT：查询指定ID的记录
- INSERT：插入新记录
- UPDATE：更新指定ID的记录
- DELETE：删除指定ID的记录

## 环境要求

- Java 8 或更高版本
- PostgreSQL 数据库
- Maven（可选，用于依赖管理）

## 依赖项

- PostgreSQL JDBC驱动
- 项目中引用的`JsonParamReader`工具类（用于读取配置文件）

## 配置说明

1. 在项目根目录创建`param.json`配置文件，内容示例：
```json
{
  "url": "jdbc:postgresql://localhost:5432/database_project",
  "user": "postgres",
  "password": "your_password",
  "schema": "project",
  "recipe_filepath": "recipe.csv"
}
```

2. 配置项说明：
   - `url`：PostgreSQL数据库连接URL
   - `user`：数据库用户名
   - `password`：数据库密码
   - `schema`：数据库模式名
   - `recipe_filepath`：CSV文件路径

## 使用方法

1. 确保PostgreSQL数据库已启动，且包含名为`recipe`的表
2. 准备好测试用的CSV文件（格式应与数据库表结构对应）
3. 配置`param.json`文件
4. 运行`DBMSvsFileIO`类的`main`方法

## 测试流程

1. 程序启动时加载配置文件并初始化日志系统
2. 建立数据库连接
3. 分别执行三种操作方式的性能测试（每种操作执行20次取平均值）
4. 输出并记录性能对比结果

## 结果说明

测试结果将以表格形式展示，包含以下信息：
- 操作类型（SELECT/INSERT/UPDATE/DELETE）
- 每种操作方式的平均耗时（毫秒）
- 文件操作与数据库操作的耗时倍率
- Stream操作与数据库操作的耗时倍率

结果会同时输出到控制台和`logs/DBMS_vs_File_test.log`文件中。

## 注意事项

1. 数据库测试中使用了事务回滚，避免测试数据污染实际数据
2. 文件操作测试中使用临时文件，测试完成后会自动删除，不会修改原文件
3. 测试结果受硬件性能、数据库配置、文件大小等多种因素影响，建议在相同环境下进行对比
4. 日志文件会保存在项目根目录的`logs`文件夹下
