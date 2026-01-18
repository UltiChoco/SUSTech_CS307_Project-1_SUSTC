# SUSTC

## final score

![Grade](https://img.shields.io/badge/Grade-103/110-blue)

## 功能概述

该代码库主要用于将CSV文件数据导入到PostgreSQL数据库中。其主要功能包括：

1. **CSV数据预处理**：在导入数据之前，代码库首先会进行CSV文件的预处理，包括数据清洗、格式化以及转换
2. **数据库连接与操作**：通过提供配置文件，代码库能够与PostgreSQL数据库建立连接，并执行一系列操作，如创建表格、插入数据等
3. **性能测试**：代码库还包括了性能测试功能，用于测试数据插入操作的效率，帮助优化数据库操作
4. **多种编程语言支持**：代码库支持多种编程语言的实现，如Java、Python、C++、go
5. **并发性能测试**：对于数据库并发查询和插入的性能评估，代码库可检测在高并发下数据库的性能表现

## 项目结构

```
CS307_PROJECT_1
├── logs
├── report
├── src
│   └── main
│       └── java
│           └── org
│               └── example
│                   ├── Task2
│                   │   ├── create_tables.sql
│                   │   └── README.md
│                   ├── Task3
│                   │   ├── ConsoleProgressBar.java
│                   │   ├── Copy.java
│                   │   ├── DataPreprocessor.java
│                   │   ├── Importer_pro.java
│                   │   ├── Importer.java
│                   │   ├── InsertPerformanceTester.java
│                   │   ├── JsonParamReader.java
│                   │   ├── README.md
│                   │   ├── RecipePreprocessor.java
│                   │   └── TableCreator.java
│                   ├── Task4
│                   │   ├── Task4_Compare_programming_languages
│                   │   │   ├── DatabasePerformanceTest.cpp
│                   │   │   ├── DatabasePerformanceTest.go
│                   │   │   ├── DatabasePerformanceTest.java
│                   │   │   ├── DatabasePerformanceTest.py
│                   │   │   ├── README.md
│                   │   │   └── result.md
│                   │   ├── Task4_Concurrent_Test
│                   │   │   ├── ConcurrentQueryTest_Baseline.java
│                   │   │   ├── ConcurrentQueryTest_Pool.java
│                   │   │   ├── create_index.sql
│                   │   │   └── README.md
│                   │   └── Task4_DBMSvsFileIOvsFileStream_test
│                   │       └── DBMSvsFileIO.java
│                   │       └── README.md
├── resources
│   ├── param.json
│   ├── recipes.csv
│   ├── reviews.csv
│   └── user.csv
├── target
├── .gitignore

```
