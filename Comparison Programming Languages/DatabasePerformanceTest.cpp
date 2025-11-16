#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <algorithm>
#include <mutex>
#include <map>
#include <array>
#include <windows.h>

#include <libpq-fe.h>

// ========================== 数据库配置 ==========================
const std::string DB_CONNINFO =
    "host=localhost port=5432 dbname=sustc_db user=postgres password=676767";

const std::string SCHEMA_NAME = "test_schema";
const std::string TABLE_NAME  = "test_schema.test_perf";

const int TOTAL_ROWS = 500000;
const int BATCH_SIZE = 10000;
const int SAMPLE_SIZE = 1000;
const int CONCURRENT_WORKERS = 10;

// ========================== 工具：随机字符串生成 ==========================
std::mt19937 rng(std::random_device{}());
std::string generateRandomStr(int len) {
    static const std::string chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::uniform_int_distribution<> dist(0, chars.size() - 1);
    std::string s;
    s.reserve(len);
    for (int i = 0; i < len; i++) s.push_back(chars[dist(rng)]);
    return s;
}

// ========================== 工具：执行 SQL 并检查 ==========================
PGresult* execSQL(PGconn* conn, const std::string& sql) {
    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) == PGRES_FATAL_ERROR) {
        std::cerr << "SQL ERROR: " << PQerrorMessage(conn) << "\nSQL = " << sql << "\n";
        PQclear(res);
        return nullptr;
    }
    return res;
}

// ========================== 1. 初始化 schema 和表 ==========================
void initSchemaAndTable(PGconn* conn) {
    std::cout << "===== 1. 初始化Schema和表 =====\n";

    execSQL(conn, "CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);
    execSQL(conn, "DROP TABLE IF EXISTS " + TABLE_NAME);

    std::string createTableSQL =
        "CREATE TABLE " + TABLE_NAME + " ("
        " id SERIAL PRIMARY KEY,"
        " uid VARCHAR(32) NOT NULL UNIQUE,"
        " content TEXT NOT NULL,"
        " value INT NOT NULL,"
        " create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
        ")";

    execSQL(conn, createTableSQL);
    execSQL(conn, "CREATE INDEX idx_perf_value ON " + TABLE_NAME + "(value)");

    std::cout << "Schema '" << SCHEMA_NAME   
              << "'和表 '" << TABLE_NAME
              << "' 创建完成\n";
}

// ========================== 2. 批量插入 ==========================
double insertLargeData(PGconn* conn) {

    std::cout << "\n===== 2. 插入大量数据 =====\n";  
    std::cout << "开始插入 " << TOTAL_ROWS << " 条数据...\n"; 

    auto startTotal = std::chrono::high_resolution_clock::now();
    int totalBatches = (TOTAL_ROWS + BATCH_SIZE - 1) / BATCH_SIZE;

    for (int batch = 0; batch < totalBatches; batch++) {

        std::string sql = "INSERT INTO " + TABLE_NAME + "(uid, content, value) VALUES ";

        for (int i = 0; i < BATCH_SIZE && batch * BATCH_SIZE + i < TOTAL_ROWS; i++) {
            sql += "('" + generateRandomStr(32) + "', '"
                        + generateRandomStr(100) + "', "
                        + std::to_string(rng() % 1000 + 1) + ")";

            if (i != BATCH_SIZE - 1)
                sql += ",";
        }

        sql += ";";

        execSQL(conn, sql);

        if ((batch + 1) % 10 == 0) {
            double progress = 100.0 * (batch + 1) / totalBatches;

            std::cout << "插入进度: " << progress
                      << "% (" << batch + 1
                      << "/" << totalBatches << "批次)\n";
        }

    }

    auto endTotal = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(endTotal - startTotal).count();

    std::cout << "插入50万条数据总耗时: " << sec
              << "秒，平均每条: " << (sec / TOTAL_ROWS)
              << "秒\n";


    return sec;
}

// ========================== 3. 查询性能测试 ==========================
double testSelectPerformance(PGconn* conn) {

    std::cout << "\n===== 3. 测试查询性能 =====\n";   
    auto begin = std::chrono::high_resolution_clock::now();

    // 获取 max(id)
    PGresult* rs = execSQL(conn, "SELECT MAX(id) FROM " + TABLE_NAME);
    if (!rs) return 0;
    int maxId = std::stoi(PQgetvalue(rs, 0, 0));
    PQclear(rs);
    if (maxId == 0) return 0;

    // 主键查询
    long long totalSingleNs = 0;
    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int id = rng() % maxId + 1;

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = execSQL(conn,
            "SELECT * FROM " + TABLE_NAME + " WHERE id = " + std::to_string(id));
        auto end = std::chrono::high_resolution_clock::now();

        if (res) PQclear(res);

        totalSingleNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    // 条件查询
    long long totalCondNs = 0;
    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int value = rng() % 1000 + 1;

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = execSQL(conn,
            "SELECT * FROM " + TABLE_NAME + " WHERE value = " + std::to_string(value) + " LIMIT 10");
        auto end = std::chrono::high_resolution_clock::now();

        if (res) PQclear(res);
        totalCondNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    // 范围查询
    auto r1 = std::chrono::high_resolution_clock::now();
    PGresult* res = execSQL(conn,
        "SELECT * FROM " + TABLE_NAME +
        " WHERE value BETWEEN 400 AND 600 ORDER BY create_time LIMIT 1000");
    auto r2 = std::chrono::high_resolution_clock::now();
    if (res) PQclear(res);

    double avgSingle = totalSingleNs / 1e9 / SAMPLE_SIZE;
    double avgCond   = totalCondNs   / 1e9 / SAMPLE_SIZE;
    double rangeTime = std::chrono::duration<double>(r2 - r1).count();

    std::cout << "查询性能:\n";
    std::cout << "  单条主键查询（1000次平均）: " << avgSingle << "秒/次\n";
    std::cout << "  条件查询（1000次平均）: "     << avgCond   << "秒/次\n";
    std::cout << "  范围查询（1000条结果）: "     << rangeTime << "秒\n";

    auto end = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(end - begin).count();

    std::cout << "查询测试总耗时: " << total << "秒\n";   

    return total;
}


// ========================== 4. 更新性能 ==========================
double testUpdatePerformance(PGconn* conn) {


    std::cout << "\n===== 4. 测试更新性能 =====\n";   
    auto begin = std::chrono::high_resolution_clock::now();

    PGresult* rs = execSQL(conn, "SELECT MAX(id) FROM " + TABLE_NAME);
    if (!rs) return 0;
    int maxId = std::stoi(PQgetvalue(rs, 0, 0));
    PQclear(rs);

    long long totalNs = 0;

    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int id = rng() % maxId + 1;
        int newValue = rng() % 1000 + 1;

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = execSQL(conn,
            "UPDATE " + TABLE_NAME +
            " SET value = " + std::to_string(newValue) +
            " WHERE id = " + std::to_string(id));
        auto end = std::chrono::high_resolution_clock::now();

        if (res) PQclear(res);
        totalNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    double avg = totalNs / 1e9 / SAMPLE_SIZE;

    std::cout << "更新性能（1000次平均）: " << avg << "秒/次\n";  
    auto end = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(end - begin).count();
    std::cout << "更新测试总耗时: " << total << "秒\n";           
    return total;
}


// ========================== 5. 删除性能 ==========================
double testDeletePerformance(PGconn* conn) {


    std::cout << "\n===== 5. 测试删除性能 =====\n";   
    auto begin = std::chrono::high_resolution_clock::now();

    std::vector<std::array<std::string, 4>> backup;

    // 取 SAMPLE_SIZE 条待删数据
    PGresult* rs = execSQL(conn,
        "SELECT id, uid, content, value FROM " + TABLE_NAME +
        " ORDER BY random() LIMIT " + std::to_string(SAMPLE_SIZE));

    if (!rs) return 0;
    int rows = PQntuples(rs);

    for (int i = 0; i < rows; i++) {
        backup.push_back({
            PQgetvalue(rs, i, 0),
            PQgetvalue(rs, i, 1),
            PQgetvalue(rs, i, 2),
            PQgetvalue(rs, i, 3)
        });
    }
    PQclear(rs);

    long long totalNs = 0;

    for (auto& row : backup) {
        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = execSQL(conn,
            "DELETE FROM " + TABLE_NAME + " WHERE id = " + row[0]);
        auto end = std::chrono::high_resolution_clock::now();
        if (res) PQclear(res);
        totalNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    // 恢复数据
    for (auto& row : backup) {
        execSQL(conn,
            "INSERT INTO " + TABLE_NAME +
            "(id, uid, content, value) VALUES (" +
            row[0] + ", '" + row[1] + "', '" + row[2] + "', " + row[3] + ")");
    }

    double avg = totalNs / 1e9 / SAMPLE_SIZE;


    std::cout << "删除性能（1000次平均）: " << avg << "秒/次\n";  

    auto end = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(end - begin).count();

    std::cout << "删除测试总耗时: " << total << "秒\n";        
    return total;

}


// ========================== 6. 并发测试任务 ==========================
struct TaskResult {
    std::string action;
    double time;
    bool error = false;
    std::string message;
};

TaskResult runConcurrentTask(int opId) {
    PGconn* conn = PQconnectdb(DB_CONNINFO.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        return {"error", 0, true, PQerrorMessage(conn)};
    }

    int actionType = rng() % 3;
    std::string action;

    auto start = std::chrono::high_resolution_clock::now();

    if (actionType == 0) {
        action = "select";
        PGresult* res = execSQL(conn,
            "SELECT * FROM " + TABLE_NAME + " WHERE id = 1");
        if (res) PQclear(res);

    } else if (actionType == 1) {
        action = "update";
        execSQL(conn,
            "UPDATE " + TABLE_NAME + " SET value = 999 WHERE id = 1");

    } else {
        action = "delete_restore";

        PGresult* rs = execSQL(conn,
            "SELECT id, uid, content, value FROM " + TABLE_NAME +
            " ORDER BY random() LIMIT 1");

        if (rs && PQntuples(rs) > 0) {
            std::string id = PQgetvalue(rs, 0, 0);
            std::string uid = PQgetvalue(rs, 0, 1);
            std::string content = PQgetvalue(rs, 0, 2);
            std::string value = PQgetvalue(rs, 0, 3);

            PQclear(rs);
            execSQL(conn,
                "DELETE FROM " + TABLE_NAME + " WHERE id = " + id);
            execSQL(conn,
                "INSERT INTO " + TABLE_NAME +
                "(id, uid, content, value) VALUES (" +
                id + ", '" + uid + "', '" + content + "', " + value + ")");
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    PQfinish(conn);

    double sec = std::chrono::duration<double>(end - start).count();
    return {action, sec, false, ""};
}

// ========================== 7. 并发测试入口 ==========================
double testConcurrentPerformance() {

    std::cout << "\n===== 6. 测试并发性能 =====\n"; 
    std::cout << "开始并发测试（" << CONCURRENT_WORKERS << "线程，共1000次操作）...\n";
    auto begin = std::chrono::high_resolution_clock::now();

    const int totalOps = 1000;

    std::vector<std::thread> threads;
    std::vector<TaskResult> results(totalOps);

    int index = 0;
    std::mutex idxMutex;

    auto worker = [&]() {
        while (true) {
            int i;
            {
                std::lock_guard<std::mutex> lock(idxMutex);
                if (index >= totalOps) return;
                i = index++;
            }
            results[i] = runConcurrentTask(i);
        }
    };

    for (int i = 0; i < CONCURRENT_WORKERS; i++)
        threads.emplace_back(worker);
    for (auto& t : threads)
        t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double totalTime = std::chrono::duration<double>(end - begin).count();

    // 聚合结果
    int success = 0, fail = 0;
    std::map<std::string, std::vector<double>> times;

    for (auto& r : results) {
        if (r.error) {
            fail++;
        } else {
            success++;
            times[r.action].push_back(r.time);
        }
    }

    std::cout << "并发测试完成:\n";
    std::cout << "总耗时: " << totalTime << "秒\n";
    std::cout << "总操作数: 1000，成功: " << success 
              << "，失败: " << fail << "\n";
    std::cout << "平均耗时（按操作类型）:\n";

    // 输出平均值
    for (auto& p : times) {
        double avg = 0;
        for (double t : p.second) avg += t;
        avg /= p.second.size();

        if (p.first == "select")
            std::cout << "查询: " << avg << "秒/次\n";
        else if (p.first == "update")
            std::cout << "更新: " << avg << "秒/次\n";
        else if (p.first == "delete_restore")
            std::cout << "删除恢复: " << avg << "秒/次\n";
    }

    return totalTime;
}


// ========================== main ==========================
int main() {

    SetConsoleOutputCP(CP_UTF8);
    SetConsoleCP(CP_UTF8);

    PGconn* conn = PQconnectdb(DB_CONNINFO.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        std::cout << "数据库连接失败: " << PQerrorMessage(conn) << "\n";
        return 1;
    }

    //// ===== 1. 初始化 Schema 和表 =====
    auto t1 = std::chrono::high_resolution_clock::now();
    initSchemaAndTable(conn);
    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "初始化耗时: "
              << std::chrono::duration<double>(t2 - t1).count()
              << "秒\n\n";

    //// ===== 2. 插入大量数据 =====
    double insertTime = insertLargeData(conn);
 
    //// ===== 3. 查询性能测试 =====
    double selectTime = testSelectPerformance(conn);  

    //// ===== 4. 更新性能 =====
    double updateTime = testUpdatePerformance(conn);  

    //// ===== 5. 删除性能 =====
    double deleteTime = testDeletePerformance(conn);  

    //// ===== 6. 并发性能 =====
    double concurTime = testConcurrentPerformance();  

    PQfinish(conn);

    std::cout << "\n所有测试完成\n";

    return 0;
}
