# Task3数据导入与代码优化

## 项目结构

```
Task3/
├── TableCreator.java        # 数据库表创建类，负责创建数据库模式和表结构
├── JsonParamReader.java     # JSON配置文件读取类，用于读取数据库连接参数等配置
├── Importer_pro.java        # 并行数据导入类，采用多线程并行导入数据以提高效率
├── Importer.java            # 数据导入类，实现了各类数据的导入逻辑
├── RecipePreprocessor.java  # 食谱数据预处理类，用于清洗食谱CSV数据并记录错误
├── ConsoleProgressBar.java  # 控制台进度条类，用于显示数据处理进度
├── Copy.java                # 数据库复制类，封装了PostgreSQL的COPY命令用于高效数据导入
├── DataPreprocessor.java    # 数据预处理工具类，用于处理CSV文件中的无效字符和行合并
└── InsertPerformanceTaster.java  # 不同插入方法性能测试类，用于测试不同插入方法的性能
```

## 功能说明

### 1. 数据库表结构创建
`TableCreator`类负责创建数据库模式（schema）和所需的表结构，包括用户表（users）、关注表（follows）、食谱表（recipe）等12张表，并定义了表之间的关系和约束。

### 2. 配置文件读取
`JsonParamReader`类用于读取`param.json`配置文件中的参数，包括数据库连接信息（URL、用户名、密码）、数据库模式名称以及各CSV文件的路径等。

### 3. 数据导入
- `Importer`类：实现了将用户、关注、食谱等数据从CSV文件导入到对应数据库表中的功能，使用PostgreSQL的COPY命令进行高效导入，并提供了进度条显示。
- `Importer_pro`类：在`Importer`的基础上采用多线程并行导入数据，提高了数据导入的效率。

### 4. 数据预处理
- `RecipePreprocessor`类：专门用于清洗食谱CSV数据，检查数据的有效性（如字段数量、必填字段是否缺失、数字格式是否正确等），将干净的数据写入新文件，并记录错误数据。
- `DataPreprocessor`类：用于处理CSV文件中的无效字符（如替换竖线和反斜杠，避免与分隔符冲突）和修复被拆分的行。

### 5. 辅助功能
`ConsoleProgressBar`类用于在控制台显示数据处理的进度条，方便用户了解处理进度。

## 使用方法

### 1. 配置参数
在项目的资源目录下创建或修改`param.json`配置文件，设置以下参数：
```json
{
    "url": "jdbc:postgresql://localhost:5432/database_project",
    "user": "postgres",
    "password": "your_password",
    "schema": "project_unlogged",
    "recipe_filepath": "src/main/resources/recipes.csv",
    "review_filepath": "src/main/resources/reviews.csv",
    "user_filepath": "src/main/resources/user.csv"
}
```

### 2. 数据预处理
如果需要对数据进行预处理，可以运行`DataPreprocessor`类的`main`方法，它会对CSV文件进行处理，替换无效字符并修复被拆分的行。对于食谱数据，还可以运行`RecipePreprocessor`类的`main`方法进行专门的清洗。

### 3. 创建数据库表结构
运行`Importer`或`Importer_pro`类的`main`方法时，会先通过`TableCreator`类创建数据库模式和表结构。

### 4. 导入数据
- 运行`Importer`类的`main`方法，将按顺序导入各类数据。
- 运行`Importer_pro`类的`main`方法，将采用多线程并行导入数据，提高导入速度。

## 注意事项
- 确保PostgreSQL数据库服务已启动，并且配置文件中的数据库连接信息正确。
- 在进行数据导入前，建议先进行数据预处理，以确保数据的有效性。
- 对于大型CSV文件，推荐使用`Importer_pro`类进行并行导入，以提高效率。
- 导入过程中会禁用外键约束以提高性能，导入完成后会重新启用。