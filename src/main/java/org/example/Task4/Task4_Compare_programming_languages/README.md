# 数据库性能测试项目

该项目旨在对比不同编程语言（Go、C++、Java、Python）在数据库操作上的性能表现，通过执行相同的数据库任务，测量并分析各语言在数据插入、查询、更新、删除及并发操作等场景下的性能差异。

## 测试环境

- **数据库**：PostgreSQL
- **数据库配置**：host=localhost, port=5432, dbname=sustc_db, user=postgres, password=676767
- **测试表结构**：
  - 表名：test_schema.test_perf
  - 字段：id（SERIAL PRIMARY KEY）、uid（VARCHAR(32) NOT NULL UNIQUE）、content（TEXT NOT NULL）、value（INT NOT NULL）、create_time（TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP）
  - 索引：idx_test_perf_value（基于value字段）

## 测试内容

1. **初始化Schema和表**：创建测试所需的schema和表结构，并建立相关索引
2. **批量插入数据**：插入50万条测试数据，每条数据包含随机生成的uid、content和value
3. **查询性能测试**：
   - 单条主键查询（1000次平均）
   - 条件查询（基于value字段，1000次平均）
   - 范围查询（value在400-600之间，限制1000条结果）
4. **更新性能测试**：随机更新1000条记录的value值（1000次平均）
5. **删除性能测试**：随机删除1000条记录后恢复（1000次平均）
6. **并发性能测试**：使用10个并发线程执行1000次混合操作（查询、更新、删除恢复）

## 各语言实现说明

### Go
- 使用`database/sql`标准库结合`lib/pq`驱动
- 采用全局连接池管理数据库连接
- 批量插入使用事务+预编译语句提升性能
- 并发测试通过goroutine实现

### C++
- 使用`libpq`库直接操作PostgreSQL
- 为每个线程分配独立的随机数生成器避免竞争
- 预编译常用SQL语句减少解析开销
- 批量插入使用单个大事务包裹提升效率

### Java
- 使用HikariCP连接池管理数据库连接
- 采用JDBC规范进行数据库操作
- 批量插入使用`addBatch()`和`executeBatch()`方法
- 并发测试通过线程池实现

### Python
- 使用连接池管理数据库连接
- 采用psycopg2库进行PostgreSQL操作
- 通过装饰器测量各操作耗时
- 批量插入通过`executemany()`方法优化性能
- 并发测试使用`threading`模块实现多线程

## 测试结果概览

| 操作类型        | Go          | C++         | Java        | Python      |
|---------------|-------------|-------------|-------------|-------------|
| 初始化耗时      | 0.043679秒   | 0.0436163秒  | 0.117656秒   | 0.158234秒   |
| 插入50万条数据  | 28.013082秒  | 9.32522秒    | 9.982485秒   | 42.36712秒   |
| 单条主键查询平均 | 0.000057秒   | 4.38995e-05秒| 0.000125秒   | 0.000213秒   |
| 条件查询平均    | 0.000050秒   | 3.98984e-05秒| 0.000113秒   | 0.000189秒   |
| 范围查询        | 0.102362秒   | 0.0433786秒  | 0.077149秒   | 0.186421秒   |
| 更新平均        | 0.000193秒   | 0.000132715秒| 0.000340秒   | 0.000427秒   |
| 删除平均        | 0.000127秒   | 8.93919e-05秒| 0.000272秒   | 0.000356秒   |
| 并发测试总耗时   | 6.503238秒   | 5.48928秒    | 8.939368秒   | 15.78234秒   |

## 如何运行

1. 确保PostgreSQL数据库服务已启动并配置正确
2. 根据所测试的语言，安装相应的数据库驱动和依赖库：
   - Go：`go get github.com/lib/pq`
   - C++：安装`libpq-dev`（Ubuntu）或对应系统的PostgreSQL开发库
   - Java：添加HikariCP和PostgreSQL JDBC驱动依赖
   - Python：`pip install psycopg2-binary`
3. 运行对应语言的测试程序：
   - Go: `go run DatabasePerformanceTest.go`
   - C++: 编译命令（示例）`g++ -o db_perf_test DatabasePerformanceTest.cpp -lpq`，然后运行`./db_perf_test`
   - Java: 编译`javac DatabasePerformanceTest.java`，运行`java DatabasePerformanceTest`
   - Python: `python DatabasePerformanceTest.py`

## 注意事项

- 测试结果可能受硬件配置、数据库配置和系统负载影响，建议在相同环境下单独运行各语言测试
- 运行测试前请确保数据库用户具有创建schema、表、索引及增删改查的权限
- 大批量数据插入可能需要调整数据库连接超时设置（如PostgreSQL的`max_connections`、`statement_timeout`）
- 并发测试时，数据库连接池配置（如最大连接数）会影响测试结果，各语言已统一配置合理的连接池参数
- Python的GIL可能影响并发性能，测试中已通过多线程结合数据库操作IO阻塞特性优化
