# 数据库并发查询并发测试项目

## 项目概述

本项目包含两个Java类，用于测试数据库在并发查询场景下的性能表现。通过对比使用连接池（HikariCP）与传统连接方式的性能差异，评估数据库连接池对系统吞吐量的提升效果。

## 测试组件说明

### 1. ConcurrentQueryTest_Pool
使用HikariCP连接池进行并发查询测试，主要特点：
- 采用连接池管理数据库连接
- 启用PreparedStatement缓存
- 可配置并发线程数和总查询数
- 自动记录测试日志到文件

### 2. ConcurrentQueryTest_Baseline
传统方式的并发查询测试（基准测试），主要特点：
- 每次查询创建新的数据库连接
- 不使用连接池管理
- 作为性能对比基准
- 同样支持日志记录功能

## 配置说明

### 数据库配置
通过`param.json`文件配置数据库连接参数：
```json
{
  "url": "jdbc:postgresql://localhost:5432/database_project",
  "user": "postgres",
  "password": "your_password",
  "schema": "project_unlogged"
}
```

若配置文件不存在，将使用默认值连接本地PostgreSQL数据库。

### 测试参数
可在代码中调整以下参数：
- 并发线程数（THREAD_COUNT）
- 总查询数（TOTAL_QUERIES）
- HikariCP连接池配置（仅Pool测试类）

## 运行方法

1. 确保数据库服务已启动并创建相应表结构
2. 配置`param.json`文件（可选）
3. 分别运行两个测试类的main方法
4. 查看控制台输出和logs目录下的日志文件获取测试结果

## 测试结果说明

测试完成后会输出以下性能指标：
- 总耗时（Total time）：完成所有查询的总时间（秒）
- 平均查询时间（Average per query）：每个查询的平均耗时（毫秒）
- 吞吐量（Throughput, QPS）：每秒处理的查询数

## 注意事项

1. 测试前请确保数据库中存在`recipe`表且包含足够数据
2. 大量并发查询可能会对数据库服务器造成压力，请根据服务器性能调整参数
3. 日志文件会自动生成在项目根目录的`logs`文件夹下
4. 连接池测试的线程数不宜超过数据库能支持的最大连接数

## 依赖说明

- Java 8+
- PostgreSQL JDBC驱动
- HikariCP连接池
- 日志相关依赖（Java自带logging）