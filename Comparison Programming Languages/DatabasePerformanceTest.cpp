#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <future>
#include <algorithm>
#include <pqxx/pqxx>
#include <numeric>
#include <memory>

// 配置参数
struct Config {
    std::string dbname = "project";
    std::string user = "postgres";
    std::string password = "Dr141592";
    std::string host = "localhost";
    std::string port = "5432";
    std::string schema = "test_schema";
    std::string table;
    size_t total_rows = 500000;
    size_t batch_size = 10000;
    size_t concurrent_workers = 10;

    Config() {
        table = schema + ".test_perf";
    }
};

Config cfg;

// 生成随机字符串
std::string generate_random_str(size_t length = 10) {
    static const std::string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, chars.size() - 1);

    std::string s;
    for (size_t i = 0; i < length; ++i) {
        s += chars[dis(gen)];
    }
    return s;
}

// 计时工具类
template <typename F, typename... Args>
std::pair<typename std::result_of<F(Args...)>::type, double> time_it(F&& f, Args&&... args) {
    auto start = std::chrono::high_resolution_clock::now();
    auto result = std::forward<F>(f)(std::forward<Args>(args)...);
    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double>(end - start).count();
    return {result, duration};
}

// 初始化Schema和表
void init_schema_and_table(pqxx::work& txn) {
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + cfg.schema);
    txn.exec("DROP TABLE IF EXISTS " + cfg.table);
    txn.exec("CREATE TABLE " + cfg.table + " ("
        "id SERIAL PRIMARY KEY, "
        "uid VARCHAR(32) NOT NULL UNIQUE, "
        "content TEXT NOT NULL, "
        "value INT NOT NULL, "
        "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ");");
    txn.exec("CREATE INDEX idx_" + cfg.schema + "_test_perf_value ON " + cfg.table + "(value)");
    txn.commit();
    std::cout << "Schema '" << cfg.schema << "'和表 '" << cfg.table << "' 创建完成" << std::endl;
}

// 批量插入数据
void insert_large_data(pqxx::work& txn, size_t total_rows, size_t batch_size) {
    std::cout << "开始插入 " << total_rows << " 条数据..." << std::endl;
    size_t total_batches = (total_rows + batch_size - 1) / batch_size;

    for (size_t batch = 0; batch < total_batches; ++batch) {
        size_t current_batch_size = std::min(batch_size, total_rows - batch * batch_size);
        pqxx::prepare::invocation stmt = txn.prepared("insert_batch");
        
        for (size_t i = 0; i < current_batch_size; ++i) {
            stmt(generate_random_str(32))
               (generate_random_str(100))
               (std::rand() % 1000 + 1)
               .exec();
        }

        if ((batch + 1) % 10 == 0) {
            double progress = (batch + 1) * 100.0 / total_batches;
            std::cout << "插入进度: " << progress << "% (" << batch + 1 << "/" << total_batches << "批次)" << std::endl;
        }
    }
    txn.commit();
}

// 测试查询性能
void test_select_performance(pqxx::work& txn, size_t sample_size = 1000) {
    pqxx::result res = txn.exec("SELECT MAX(id) FROM " + cfg.table);
    if (res.empty() || res[0][0].is_null()) return;
    int max_id = res[0][0].as<int>();

    // 单条主键查询
    double total_single = 0;
    for (size_t i = 0; i < sample_size; ++i) {
        int id = std::rand() % max_id + 1;
        auto [_, t] = time_it([&]() {
            txn.exec_params("SELECT * FROM " + cfg.table + " WHERE id = $1", id);
        });
        total_single += t;
    }
    double avg_single = total_single / sample_size;

    // 条件查询
    double total_cond = 0;
    for (size_t i = 0; i < sample_size; ++i) {
        int val = std::rand() % 1000 + 1;
        auto [_, t] = time_it([&]() {
            txn.exec_params("SELECT * FROM " + cfg.table + " WHERE value = $1 LIMIT 10", val);
        });
        total_cond += t;
    }
    double avg_cond = total_cond / sample_size;

    // 范围查询
    auto [__, t_range] = time_it([&]() {
        txn.exec_params("SELECT * FROM " + cfg.table + 
            " WHERE value BETWEEN $1 AND $2 ORDER BY create_time LIMIT 1000", 400, 600);
    });

    std::cout << "查询性能:" << std::endl;
    std::cout << "  单条主键查询（" << sample_size << "次平均）: " << avg_single << "秒/次" << std::endl;
    std::cout << "  条件查询（" << sample_size << "次平均）: " << avg_cond << "秒/次" << std::endl;
    std::cout << "  范围查询（1000条结果）: " << t_range << "秒" << std::endl;
}

// 测试更新性能
void test_update_performance(pqxx::work& txn, size_t sample_size = 1000) {
    pqxx::result res = txn.exec("SELECT MAX(id) FROM " + cfg.table);
    if (res.empty() || res[0][0].is_null()) return;
    int max_id = res[0][0].as<int>();

    double total_time = 0;
    for (size_t i = 0; i < sample_size; ++i) {
        int target_id = std::rand() % max_id + 1;
        int new_val = std::rand() % 1000 + 1;
        auto [_, t] = time_it([&]() {
            txn.exec_params("UPDATE " + cfg.table + " SET value = $1 WHERE id = $2", new_val, target_id);
            txn.commit();
            txn = pqxx::work(txn.connection()); // 重新创建事务
        });
        total_time += t;
    }

    double avg_update = total_time / sample_size;
    std::cout << "更新性能（" << sample_size << "次平均）: " << avg_update << "秒/次" << std::endl;
}

// 测试删除性能
void test_delete_performance(pqxx::work& txn, size_t sample_size = 1000) {
    pqxx::result backup = txn.exec("SELECT id, uid, content, value FROM " + cfg.table + 
        " ORDER BY random() LIMIT " + std::to_string(sample_size));
    if (backup.empty()) return;

    double total_time = 0;
    std::vector<pqxx::row> backup_rows;
    for (const auto& row : backup) {
        backup_rows.push_back(row);
    }

    // 执行删除
    for (const auto& row : backup_rows) {
        int id = row[0].as<int>();
        auto [_, t] = time_it([&]() {
            txn.exec_params("DELETE FROM " + cfg.table + " WHERE id = $1", id);
            txn.commit();
            txn = pqxx::work(txn.connection());
        });
        total_time += t;
    }

    // 恢复数据
    for (const auto& row : backup_rows) {
        txn.exec_params("INSERT INTO " + cfg.table + " (id, uid, content, value) "
            "VALUES ($1, $2, $3, $4)",
            row[0].as<int>(),
            row[1].as<std::string>(),
            row[2].as<std::string>(),
            row[3].as<int>()
        );
    }
    txn.commit();
    txn = pqxx::work(txn.connection());

    double avg_delete = total_time / sample_size;
    std::cout << "删除性能（" << sample_size << "次平均）: " << avg_delete << "秒/次" << std::endl;
}

// 并发操作函数
std::tuple<int, std::string, double> concurrent_operation(int operation_id) {
    try {
        pqxx::connection conn(
            "dbname=" + cfg.dbname + 
            " user=" + cfg.user + 
            " password=" + cfg.password + 
            " host=" + cfg.host + 
            " port=" + cfg.port
        );
        pqxx::work txn(conn);

        std::vector<std::string> actions = {"select", "update", "delete_restore"};
        std::string action = actions[std::rand() % actions.size()];
        auto start = std::chrono::high_resolution_clock::now();

        if (action == "select") {
            pqxx::result res = txn.exec("SELECT MAX(id) FROM " + cfg.table);
            if (!res.empty() && !res[0][0].is_null()) {
                int max_id = res[0][0].as<int>();
                int id = std::rand() % max_id + 1;
                txn.exec_params("SELECT * FROM " + cfg.table + " WHERE id = $1", id);
            }
        }
        else if (action == "update") {
            pqxx::result res = txn.exec("SELECT MAX(id) FROM " + cfg.table);
            if (!res.empty() && !res[0][0].is_null()) {
                int max_id = res[0][0].as<int>();
                int target_id = std::rand() % max_id + 1;
                int new_val = std::rand() % 1000 + 1;
                txn.exec_params("UPDATE " + cfg.table + " SET value = $1 WHERE id = $2", new_val, target_id);
                txn.commit();
            }
        }
        else if (action == "delete_restore") {
            pqxx::result res = txn.exec("SELECT id, uid, content, value FROM " + cfg.table + " ORDER BY random() LIMIT 1");
            if (!res.empty()) {
                auto row = res[0];
                int id = row[0].as<int>();
                txn.exec_params("DELETE FROM " + cfg.table + " WHERE id = $1", id);
                txn.exec_params("INSERT INTO " + cfg.table + " (id, uid, content, value) VALUES ($1, $2, $3, $4)",
                    row[0].as<int>(), row[1].as<std::string>(), row[2].as<std::string>(), row[3].as<int>());
                txn.commit();
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double>(end - start).count();
        return {operation_id, action, duration};
    }
    catch (const std::exception& e) {
        return {operation_id, "error", 0.0};
    }
}

// 测试并发性能
void test_concurrent_performance(size_t workers = cfg.concurrent_workers, size_t total_ops = 1000) {
    std::cout << "\n开始并发测试（" << workers << "线程，共" << total_ops << "次操作）..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::future<std::tuple<int, std::string, double>>> futures;
    for (size_t i = 0; i < total_ops; ++i) {
        futures.emplace_back(std::async(std::launch::async, concurrent_operation, i));
    }

    std::vector<std::tuple<int, std::string, double>> results;
    for (auto& fut : futures) {
        results.push_back(fut.get());
    }

    double total_time = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - start).count();

    size_t success = 0;
    std::vector<std::string> errors;
    std::vector<double> select_times, update_times, delete_times;

    for (const auto& res : results) {
        std::string action = std::get<1>(res);
        if (action == "error") {
            errors.push_back("操作失败");
        } else {
            success++;
            double t = std::get<2>(res);
            if (action == "select") select_times.push_back(t);
            else if (action == "update") update_times.push_back(t);
            else if (action == "delete_restore") delete_times.push_back(t);
        }
    }

    auto avg = [](const std::vector<double>& v) {
        return v.empty() ? 0 : std::accumulate(v.begin(), v.end(), 0.0) / v.size();
    };

    std::cout << "并发测试完成:" << std::endl;
    std::cout << "  总耗时: " << total_time << "秒" << std::endl;
    std::cout << "  总操作数: " << total_ops << "，成功: " << success << "，失败: " << errors.size() << std::endl;
    std::cout << "  平均耗时（按操作类型）:" << std::endl;
    std::cout << "    查询: " << avg(select_times) << "秒/次" << std::endl;
    std::cout << "    更新: " << avg(update_times) << "秒/次" << std::endl;
    std::cout << "    删除恢复: " << avg(delete_times) << "秒/次" << std::endl;
}

int main() {
    std::srand(std::time(nullptr));
    pqxx::connection conn(
        "dbname=" + cfg.dbname + 
        " user=" + cfg.user + 
        " password=" + cfg.password + 
        " host=" + cfg.host + 
        " port=" + cfg.port
    );

    try {
        // 1. 初始化Schema和表
        std::cout << "\n===== 1. 初始化Schema和表 =====" << std::endl;
        pqxx::work txn1(conn);
        auto [_, t_init] = time_it(init_schema_and_table, std::ref(txn1));
        std::cout << "初始化耗时: " << t_init << "秒" << std::endl;

        // 2. 插入数据
        std::cout << "\n===== 2. 插入大量数据 =====" << std::endl;
        pqxx::work txn2(conn);
        txn2.prepare("insert_batch", "INSERT INTO " + cfg.table + " (uid, content, value) VALUES ($1, $2, $3)");
        auto [__, t_insert] = time_it(insert_large_data, std::ref(txn2), cfg.total_rows, cfg.batch_size);
        std::cout << "插入" << cfg.total_rows << "条数据总耗时: " << t_insert << "秒，"
                  << "平均每条: " << t_insert / cfg.total_rows << "秒" << std::endl;

        // 3. 测试查询性能
        std::cout << "\n===== 3. 测试查询性能 =====" << std::endl;
        pqxx::work txn3(conn);
        auto [___, t_select] = time_it(test_select_performance, std::ref(txn3));
        std::cout << "查询测试总耗时: " << t_select << "秒" << std::endl;

        // 4. 测试更新性能
        std::cout << "\n===== 4. 测试更新性能 =====" << std::endl;
        pqxx::work txn4(conn);
        auto [____, t_update] = time_it(test_update_performance, std::ref(txn4));
        std::cout << "更新测试总耗时: " << t_update << "秒" << std::endl;

        // 5. 测试删除性能
        std::cout << "\n===== 5. 测试删除性能 =====" << std::endl;
        pqxx::work txn5(conn);
        auto [_____, t_delete] = time_it(test_delete_performance, std::ref(txn5));
        std::cout << "删除测试总耗时: " << t_delete << "秒" << std::endl;

        // 6. 测试并发性能
        std::cout << "\n===== 6. 测试并发性能 =====" << std::endl;
        test_concurrent_performance();
    }
    catch (const std::exception& e) {
        std::cerr << "测试出错: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\n所有测试完成，资源已释放" << std::endl;
    return 0;
}