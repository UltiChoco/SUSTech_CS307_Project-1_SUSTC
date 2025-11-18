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

// ========================== 工具：线程局部随机数 ==========================
// 使用 thread_local RNG，避免多线程共享一个全局 rng 产生竞争/UB
std::mt19937& thread_rng() {
    static thread_local std::mt19937 eng{ std::random_device{}() };
    return eng;
}



std::string generateRandomStr(int len) {
    static const std::string chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    std::uniform_int_distribution<> dist(0, (int)chars.size() - 1);
    std::string s;
    s.reserve(len);
    auto& rng = thread_rng();
    for (int i = 0; i < len; i++) s.push_back(chars[dist(rng)]);
    return s;
}

// ========================== 工具：执行 SQL 并检查 ==========================
PGresult* execSQL(PGconn* conn, const std::string& sql) {
    PGresult* res = PQexec(conn, sql.c_str());
    if (PQresultStatus(res) == PGRES_FATAL_ERROR ||
        PQresultStatus(res) == PGRES_BAD_RESPONSE) {
        std::cerr << "SQL ERROR: " << PQerrorMessage(conn) << "\nSQL = " << sql << "\n";
        PQclear(res);
        return nullptr;
    }
    return res;
}

// ========================== 预编译语句准备 ==========================
// 每个连接上 prepare 常用语句，避免反复 parse/plan
void prepareCommonStatements(PGconn* conn) {
    std::string base = TABLE_NAME;

    PGresult* res = nullptr;

    // 按 id 查询
    res = PQprepare(
        conn,
        "sel_by_id",
        ("SELECT * FROM " + base + " WHERE id = $1").c_str(),
        1,
        nullptr
    );
    if (res) PQclear(res);

    // 按 value 查询
    res = PQprepare(
        conn,
        "sel_by_value",
        ("SELECT * FROM " + base + " WHERE value = $1 LIMIT 10").c_str(),
        1,
        nullptr
    );
    if (res) PQclear(res);

    // 按 id 更新 value
    res = PQprepare(
        conn,
        "upd_by_id",
        ("UPDATE " + base + " SET value = $1 WHERE id = $2").c_str(),
        2,
        nullptr
    );
    if (res) PQclear(res);

    // 按 id 删除
    res = PQprepare(
        conn,
        "del_by_id",
        ("DELETE FROM " + base + " WHERE id = $1").c_str(),
        1,
        nullptr
    );
    if (res) PQclear(res);

    // 删除恢复时用：插入一行（带 id）
    res = PQprepare(
        conn,
        "ins_full_row",
        ("INSERT INTO " + base + " (id, uid, content, value) VALUES ($1,$2,$3,$4)").c_str(),
        4,
        nullptr
    );
    if (res) PQclear(res);
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

    // 使用单个大事务包裹所有插入，避免每条 INSERT 一个隐式事务
    execSQL(conn, "BEGIN");

    for (int batch = 0; batch < totalBatches; batch++) {
        std::string sql;
        sql.reserve(BATCH_SIZE * 150); // 预留空间减少 realloc
        sql = "INSERT INTO " + TABLE_NAME + "(uid, content, value) VALUES ";

        int thisBatchRows = 0;
        for (int i = 0; i < BATCH_SIZE && batch * BATCH_SIZE + i < TOTAL_ROWS; i++) {
            if (thisBatchRows > 0) sql += ",";

            auto& rng = thread_rng();
            int val = (int)(rng() % 1000 + 1);

            sql += "('"
                + generateRandomStr(32) + "', '"
                + generateRandomStr(100) + "', "
                + std::to_string(val) + ")";

            thisBatchRows++;
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

    execSQL(conn, "COMMIT"); // 使用单个大事务包裹所有插入，避免每条 INSERT 一个隐式事务

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

    long long totalSingleNs = 0;
    long long totalCondNs   = 0;

    auto& rng = thread_rng();

    // 主键查询（使用 prepared）
    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int id = (int)(rng() % maxId + 1);
        std::string idStr = std::to_string(id);
        const char* params[1] = { idStr.c_str() };

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = PQexecPrepared(conn, "sel_by_id", 1, params, nullptr, nullptr, 0);
        auto end = std::chrono::high_resolution_clock::now();

        if (res) PQclear(res);
        totalSingleNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    // 条件查询（使用 prepared）
    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int value = (int)(rng() % 1000 + 1);
        std::string vStr = std::to_string(value);
        const char* params[1] = { vStr.c_str() };

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = PQexecPrepared(conn, "sel_by_value", 1, params, nullptr, nullptr, 0);
        auto end = std::chrono::high_resolution_clock::now();

        if (res) PQclear(res);
        totalCondNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    // 范围查询（仍然走普通 SQL，一次）
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
    auto& rng = thread_rng();

    for (int i = 0; i < SAMPLE_SIZE; i++) {
        int id = (int)(rng() % maxId + 1);
        int newValue = (int)(rng() % 1000 + 1);

        std::string idStr = std::to_string(id);
        std::string valStr = std::to_string(newValue);
        const char* params[2] = { valStr.c_str(), idStr.c_str() };

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = PQexecPrepared(conn, "upd_by_id", 2, params, nullptr, nullptr, 0);
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
        const char* params[1] = { row[0].c_str() }; // id

        auto start = std::chrono::high_resolution_clock::now();
        PGresult* res = PQexecPrepared(conn, "del_by_id", 1, params, nullptr, nullptr, 0);
        auto end = std::chrono::high_resolution_clock::now();
        if (res) PQclear(res);
        totalNs += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    // 恢复数据（使用 prepared insert）
    for (auto& row : backup) {
        const char* params[4] = {
            row[0].c_str(), // id
            row[1].c_str(), // uid
            row[2].c_str(), // content
            row[3].c_str()  // value
        };
        PGresult* r = PQexecPrepared(conn, "ins_full_row", 4, params, nullptr, nullptr, 0);
        if (r) PQclear(r);
    }

    double avg = totalNs / 1e9 / SAMPLE_SIZE;

    std::cout << "删除性能（1000次平均）: " << avg << "秒/次\n";

    auto end = std::chrono::high_resolution_clock::now();
    double total = std::chrono::duration<double>(end - begin).count();

    std::cout << "删除测试总耗时: " << total << "秒\n";
    return total;
}

// ========================== 并发测试任务 ==========================
struct TaskResult {
    std::string action;
    double time;
    bool error = false;
    std::string message;
};

//  不在这里 PQconnectdb / PQfinish，而是直接复用线程自己的连接
TaskResult runConcurrentTask(PGconn* conn, int opId) {
    int actionType = (int)(thread_rng()() % 3);
    std::string action;

    auto start = std::chrono::high_resolution_clock::now();

    if (actionType == 0) {
        action = "select";
        // 用 prepared sel_by_id，固定 id=1
        std::string idStr = "1";
        const char* params[1] = { idStr.c_str() };
        PGresult* res = PQexecPrepared(conn, "sel_by_id", 1, params, nullptr, nullptr, 0);
        if (res) PQclear(res);

    } else if (actionType == 1) {
        action = "update";
        std::string idStr = "1";
        std::string valStr = "999";
        const char* params[2] = { valStr.c_str(), idStr.c_str() };
        PGresult* res = PQexecPrepared(conn, "upd_by_id", 2, params, nullptr, nullptr, 0);
        if (res) PQclear(res);

    } else {
        action = "delete_restore";

        PGresult* rs = execSQL(conn,
            "SELECT id, uid, content, value FROM " + TABLE_NAME +
            " ORDER BY random() LIMIT 1");

        if (rs && PQntuples(rs) > 0) {
            std::string id      = PQgetvalue(rs, 0, 0);
            std::string uid     = PQgetvalue(rs, 0, 1);
            std::string content = PQgetvalue(rs, 0, 2);
            std::string value   = PQgetvalue(rs, 0, 3);
            PQclear(rs);

            const char* pDel[1] = { id.c_str() };
            PGresult* r1 = PQexecPrepared(conn, "del_by_id", 1, pDel, nullptr, nullptr, 0);
            if (r1) PQclear(r1);

            const char* pIns[4] = {
                id.c_str(), uid.c_str(), content.c_str(), value.c_str()
            };
            PGresult* r2 = PQexecPrepared(conn, "ins_full_row", 4, pIns, nullptr, nullptr, 0);
            if (r2) PQclear(r2);
        } else if (rs) {
            PQclear(rs);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    double sec = std::chrono::duration<double>(end - start).count();
    return {action, sec, false, ""};
}

// ========================== 7. 并发测试入口 ==========================
double testConcurrentPerformance() {

    std::cout << "\n===== 6. 测试并发性能 =====\n";
    std::cout << "开始并发测试（" << CONCURRENT_WORKERS << "线程，共1000次操作）...\n";
    auto begin = std::chrono::high_resolution_clock::now();

    const int totalOps = 1000;

    // 为每个 worker 提前建立一个连接，并在其上 prepare 语句
    std::vector<PGconn*> workerConns(CONCURRENT_WORKERS, nullptr);
    for (int i = 0; i < CONCURRENT_WORKERS; ++i) {
        workerConns[i] = PQconnectdb(DB_CONNINFO.c_str());
        if (PQstatus(workerConns[i]) != CONNECTION_OK) {
            std::cerr << "并发线程连接失败: " << PQerrorMessage(workerConns[i]) << "\n";
            return 0;
        }
        prepareCommonStatements(workerConns[i]);
    }

    std::vector<std::thread> threads;
    std::vector<TaskResult> results(totalOps);

    int index = 0;
    std::mutex idxMutex;

    auto worker = [&](int workerId) {
        PGconn* conn = workerConns[workerId];
        while (true) {
            int i;
            {
                std::lock_guard<std::mutex> lock(idxMutex);
                if (index >= totalOps) return;
                i = index++;
            }
            results[i] = runConcurrentTask(conn, i);
        }
    };

    for (int i = 0; i < CONCURRENT_WORKERS; i++)
        threads.emplace_back(worker, i);
    for (auto& t : threads)
        t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double totalTime = std::chrono::duration<double>(end - begin).count();

    // 关闭 worker 连接
    for (auto* c : workerConns) {
        if (c) PQfinish(c);
    }

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

    // 在主连接上预编译常用语句
    prepareCommonStatements(conn);

    // ===== 1. 初始化 Schema 和表 =====
    auto t1 = std::chrono::high_resolution_clock::now();
    initSchemaAndTable(conn);
    auto t2 = std::chrono::high_resolution_clock::now();

    std::cout << "初始化耗时: "
              << std::chrono::duration<double>(t2 - t1).count()
              << "秒\n\n";

    // ===== 2. 插入大量数据 =====
    double insertTime  = insertLargeData(conn);

    // ===== 3. 查询性能测试 =====
    double selectTime  = testSelectPerformance(conn);

    // ===== 4. 更新性能 =====
    double updateTime  = testUpdatePerformance(conn);

    // ===== 5. 删除性能 =====
    double deleteTime  = testDeletePerformance(conn);

    // ===== 6. 并发性能 =====
    double concurTime  = testConcurrentPerformance();

    PQfinish(conn);

    std::cout << "\n所有测试完成\n";
    std::cout << "插入耗时: "  << insertTime  << "s\n";
    std::cout << "查询耗时: "  << selectTime  << "s\n";
    std::cout << "更新耗时: "  << updateTime  << "s\n";
    std::cout << "删除耗时: "  << deleteTime  << "s\n";
    std::cout << "并发耗时: "  << concurTime  << "s\n";

    return 0;
}
