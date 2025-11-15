#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <random>
#include <thread>
#include <future>
#include <algorithm>
#include <pqxx/pqxx>
#include <numeric>
#include <iomanip>
#include <sstream>  // 补全字符串拼接所需头文件
using namespace std;
using namespace pqxx;
using namespace chrono;

// 补全thread_pool简易实现（解决未定义问题）
class thread_pool {
public:
    explicit thread_pool(size_t threads) : stop(false) {
        for (size_t i = 0; i < threads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                        if (this->stop && this->tasks.empty()) return;
                        task = move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    template<class F, class... Args>
    future<typename result_of<F(Args...)>::type> submit(F&& f, Args&&... args) {
        using return_type = typename result_of<F(Args...)>::type;
        auto task = make_shared<packaged_task<return_type()>>(
            bind(forward<F>(f), forward<Args>(args)...)
        );
        future<return_type> res = task->get_future();
        {
            unique_lock<mutex> lock(queue_mutex);
            if (stop) throw runtime_error("submit on stopped thread_pool");
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    ~thread_pool() {
        {
            unique_lock<mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (thread& worker : workers) worker.join();
    }

private:
    vector<thread> workers;
    queue<function<void()>> tasks;
    mutex queue_mutex;
    condition_variable condition;
    bool stop;
};

// 数据库配置
const string DB_CONFIG = "dbname=project user=postgres password=Dr141592 host=localhost port=5432";
const string SCHEMA_NAME = "test_schema";
const string TABLE_NAME = SCHEMA_NAME + ".test_perf";
const int TOTAL_ROWS = 500000;
const int BATCH_SIZE = 10000;
const int CONCURRENT_WORKERS = 10;

// 生成随机字符串
string generate_random_str(int length = 10) {
    static const string chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static random_device rd;
    static mt19937 gen(rd());
    static uniform_int_distribution<> dis(0, chars.size() - 1);
    
    string s;
    for (int i = 0; i < length; ++i) {
        s += chars[dis(gen)];
    }
    return s;
}

// 修复time_it函数：void返回类型适配 + 简化返回逻辑
template <typename F, typename... Args>
pair<void, double> time_it(F&& f, Args&&... args) {
    auto start = high_resolution_clock::now();
    forward<F>(f)(forward<Args>(args)...);  // 直接执行无返回值函数
    auto end = high_resolution_clock::now();
    duration<double> elapsed = end - start;
    return { {}, elapsed.count() };  // 显式返回void空值
}

// 补全join函数（字符串向量拼接）
string join(const vector<string>& vec, const string& delimiter) {
    if (vec.empty()) return "";
    stringstream ss;
    ss << vec[0];
    for (size_t i = 1; i < vec.size(); ++i) {
        ss << delimiter << vec[i];
    }
    return ss.str();
}

// 初始化Schema和表
void init_schema_and_table(work& txn) {
    txn.exec("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);
    txn.exec("DROP TABLE IF EXISTS " + TABLE_NAME);
    txn.exec("CREATE TABLE " + TABLE_NAME + " ("
        "id SERIAL PRIMARY KEY, "
        "uid VARCHAR(32) NOT NULL UNIQUE, "
        "content TEXT NOT NULL, "
        "value INT NOT NULL, "
        "create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
    ");");
    txn.exec("CREATE INDEX idx_" + SCHEMA_NAME + "_test_perf_value ON " + TABLE_NAME + "(value)");
    txn.commit();
    cout << "Schema '" << SCHEMA_NAME << "'和表 '" << TABLE_NAME << "' 创建完成" << endl;
}

// 插入大量数据
void insert_large_data(connection& conn, int total_rows, int batch_size) {
    cout << "开始插入 " << total_rows << " 条数据..." << endl;
    int total_batches = (total_rows + batch_size - 1) / batch_size;
    
    for (int batch = 0; batch < total_batches; ++batch) {
        work txn(conn);
        string sql = "INSERT INTO " + TABLE_NAME + " (uid, content, value) VALUES ";
        
        int current_batch_size = batch_size;
        if (batch == total_batches - 1) {
            current_batch_size = total_rows - batch * batch_size;
        }
        
        vector<string> values;
        for (int i = 0; i < current_batch_size; ++i) {
            string uid = txn.quote(generate_random_str(32));
            string content = txn.quote(generate_random_str(100));
            int value = rand() % 1000 + 1;
            values.push_back("(" + uid + ", " + content + ", " + to_string(value) + ")");
        }
        
        sql += join(values, ", ") + ";";
        txn.exec(sql);
        txn.commit();
        
        if ((batch + 1) % 10 == 0) {
            double progress = (batch + 1) * 100.0 / total_batches;
            cout << "插入进度: " << fixed << setprecision(1) << progress << "% (" 
                 << (batch + 1) << "/" << total_batches << "批次)" << endl;
        }
    }
}

// 测试查询性能
void test_select_performance(connection& conn, int sample_size = 1000) {
    work txn(conn);
    result res = txn.exec("SELECT MAX(id) FROM " + TABLE_NAME);
    if (res[0][0].is_null()) {
        return;
    }
    int max_id = res[0][0].as<int>();
    txn.abort();
    
    double total_single = 0.0;
    for (int i = 0; i < sample_size; ++i) {
        work txn(conn);
        int id = rand() % max_id + 1;
        auto [_, t] = time_it([&]() {
            txn.exec("SELECT * FROM " + TABLE_NAME + " WHERE id = " + to_string(id));
        });
        total_single += t;
        txn.abort();
    }
    double avg_single = total_single / sample_size;
    
    double total_cond = 0.0;
    for (int i = 0; i < sample_size; ++i) {
        work txn(conn);
        int val = rand() % 1000 + 1;
        auto [_, t] = time_it([&]() {
            txn.exec("SELECT * FROM " + TABLE_NAME + " WHERE value = " + to_string(val) + " LIMIT 10");
        });
        total_cond += t;
        txn.abort();
    }
    double avg_cond = total_cond / sample_size;
    
    work txn_range(conn);
    double t_range = 0.0;
    auto [__, t] = time_it([&]() {
        txn_range.exec("SELECT * FROM " + TABLE_NAME + " WHERE value BETWEEN 400 AND 600 ORDER BY create_time LIMIT 1000");
    });
    t_range = t;
    txn_range.abort();
    
    cout << "查询性能:" << endl;
    cout << "  单条主键查询（" << sample_size << "次平均）: " << fixed << setprecision(6) << avg_single << "秒/次" << endl;
    cout << "  条件查询（" << sample_size << "次平均）: " << fixed << setprecision(6) << avg_cond << "秒/次" << endl;
    cout << "  范围查询（1000条结果）: " << fixed << setprecision(6) << t_range << "秒" << endl;
}

// 测试更新性能
void test_update_performance(connection& conn, int sample_size = 1000) {
    work txn(conn);
    result res = txn.exec("SELECT MAX(id) FROM " + TABLE_NAME);
    if (res[0][0].is_null()) {
        return;
    }
    int max_id = res[0][0].as<int>();
    txn.abort();
    
    double total_time = 0.0;
    for (int i = 0; i < sample_size; ++i) {
        work txn(conn);
        int target_id = rand() % max_id + 1;
        int new_val = rand() % 1000 + 1;
        auto [_, t] = time_it([&]() {
            txn.exec("UPDATE " + TABLE_NAME + " SET value = " + to_string(new_val) + 
                    " WHERE id = " + to_string(target_id));
        });
        total_time += t;
        txn.commit();
    }
    
    double avg_update = total_time / sample_size;
    cout << "更新性能（" << sample_size << "次平均）: " << fixed << setprecision(6) << avg_update << "秒/次" << endl;
}

// 测试删除性能
void test_delete_performance(connection& conn, int sample_size = 1000) {
    work txn(conn);
    result res = txn.exec("SELECT id, uid, content, value FROM " + TABLE_NAME + 
                         " ORDER BY random() LIMIT " + to_string(sample_size));
    if (res.empty()) {
        return;
    }
    
    vector<tuple<int, string, string, int>> backup;
    for (const auto& row : res) {
        backup.emplace_back(
            row[0].as<int>(),
            row[1].as<string>(),
            row[2].as<string>(),
            row[3].as<int>()
        );
    }
    txn.abort();
    
    double total_time = 0.0;
    for (const auto& [id, uid, content, value] : backup) {
        work txn_del(conn);
        auto [_, t] = time_it([&]() {
            txn_del.exec("DELETE FROM " + TABLE_NAME + " WHERE id = " + to_string(id));
        });
        total_time += t;
        txn_del.commit();
    }
    
    // 恢复数据
    work txn_restore(conn);
    string sql = "INSERT INTO " + TABLE_NAME + " (id, uid, content, value) VALUES ";
    vector<string> values;
    for (const auto& [id, uid, content, value] : backup) {
        values.push_back("(" + to_string(id) + ", " + txn_restore.quote(uid) + ", " + 
                        txn_restore.quote(content) + ", " + to_string(value) + ")");
    }
    sql += join(values, ", ") + ";";
    txn_restore.exec(sql);
    txn_restore.commit();
    
    double avg_delete = total_time / sample_size;
    cout << "删除性能（" << sample_size << "次平均）: " << fixed << setprecision(6) << avg_delete << "秒/次" << endl;
}

// 并发操作函数
tuple<int, string, double> concurrent_operation(int operation_id) {
    try {
        connection conn(DB_CONFIG);
        conn.set_variable("autocommit", "on");
        
        random_device rd;
        mt19937 gen(rd());
        uniform_int_distribution<> action_dist(0, 2);
        int action = action_dist(gen);
        
        auto start = high_resolution_clock::now();
        
        if (action == 0) {  // select
            work txn(conn);
            result res = txn.exec("SELECT MAX(id) FROM " + TABLE_NAME);
            if (!res[0][0].is_null()) {
                int max_id = res[0][0].as<int>();
                int id = rand() % max_id + 1;
                txn.exec("SELECT * FROM " + TABLE_NAME + " WHERE id = " + to_string(id));
            }
            txn.abort();
        }
        else if (action == 1) {  // update
            work txn(conn);
            result res = txn.exec("SELECT MAX(id) FROM " + TABLE_NAME);
            if (!res[0][0].is_null()) {
                int max_id = res[0][0].as<int>();
                int target_id = rand() % max_id + 1;
                int new_val = rand() % 1000 + 1;
                txn.exec("UPDATE " + TABLE_NAME + " SET value = " + to_string(new_val) + 
                        " WHERE id = " + to_string(target_id));
            }
            txn.commit();
        }
        else {  // delete_restore
            work txn_get(conn);
            result res = txn_get.exec("SELECT id, uid, content, value FROM " + TABLE_NAME + " ORDER BY random() LIMIT 1");
            if (!res.empty()) {
                int id = res[0][0].as<int>();
                string uid = res[0][1].as<string>();
                string content = res[0][2].as<string>();
                int value = res[0][3].as<int>();
                txn_get.abort();
                
                // 删除
                work txn_del(conn);
                txn_del.exec("DELETE FROM " + TABLE_NAME + " WHERE id = " + to_string(id));
                txn_del.commit();
                
                // 恢复
                work txn_ins(conn);
                txn_ins.exec("INSERT INTO " + TABLE_NAME + " (id, uid, content, value) VALUES (" +
                            to_string(id) + ", " + txn_ins.quote(uid) + ", " + 
                            txn_ins.quote(content) + ", " + to_string(value) + ")");
                txn_ins.commit();
            }
            else {
                txn_get.abort();
            }
        }
        
        auto end = high_resolution_clock::now();
        duration<double> elapsed = end - start;
        
        string action_str;
        switch (action) {
            case 0: action_str = "select"; break;
            case 1: action_str = "update"; break;
            case 2: action_str = "delete_restore"; break;
            default: action_str = "unknown";
        }
        
        return {operation_id, action_str, elapsed.count()};
    }
    catch (const exception& e) {
        return {operation_id, "error", 0.0};  // 错误时返回0.0耗时
    }
}

// 测试并发性能
void test_concurrent_performance(int workers = CONCURRENT_WORKERS, int total_ops = 1000) {
    cout << "\n开始并发测试（" << workers << "线程，共" << total_ops << "次操作）..." << endl;
    auto start = high_resolution_clock::now();
    
    vector<future<tuple<int, string, double>>> futures;
    thread_pool pool(workers);  // 使用自定义thread_pool
    
    for (int i = 0; i < total_ops; ++i) {
        futures.emplace_back(pool.submit(concurrent_operation, i));
    }
    
    vector<tuple<int, string, double>> results;
    for (auto& fut : futures) {
        results.push_back(fut.get());
    }
    
    auto end = high_resolution_clock::now();
    duration<double> total_time = end - start;
    
    int success = 0;
    vector<string> errors;
    vector<double> select_times, update_times, delete_restore_times;
    
    for (const auto& [op_id, action, val] : results) {
        if (action == "error") {
            errors.push_back("operation " + to_string(op_id) + " failed");
        } else {
            success++;
            if (action == "select") select_times.push_back(val);
            else if (action == "update") update_times.push_back(val);
            else if (action == "delete_restore") delete_restore_times.push_back(val);
        }
    }
    
    auto avg = [](const vector<double>& v) {
        if (v.empty()) return 0.0;
        return accumulate(v.begin(), v.end(), 0.0) / v.size();
    };
    
    cout << "并发测试完成:" << endl;
    cout << "  总耗时: " << fixed << setprecision(6) << total_time.count() << "秒" << endl;
    cout << "  总操作数: " << total_ops << "，成功: " << success << "，失败: " << errors.size() << endl;
    cout << "  平均耗时（按操作类型）:" << endl;
    cout << "    查询: " << fixed << setprecision(6) << avg(select_times) << "秒/次" << endl;
    cout << "    更新: " << fixed << setprecision(6) << avg(update_times) << "秒/次" << endl;
    cout << "    删除恢复: " << fixed << setprecision(6) << avg(delete_restore_times) << "秒/次" << endl;
    if (!errors.empty()) {
        cout << "  错误示例: " << errors[0] << endl;
    }
}

int main() {
    try {
        connection conn(DB_CONFIG);
        cout << "成功连接到数据库" << endl;
        
        cout << "\n===== 1. 初始化Schema和表 =====" << endl;
        work txn_init(conn);
        auto [_, t_init] = time_it(init_schema_and_table, ref(txn_init));
        cout << "初始化耗时: " << fixed << setprecision(6) << t_init << "秒" << endl;
        
        cout << "\n===== 2. 插入大量数据 =====" << endl;
        auto [__, t_insert] = time_it(insert_large_data, ref(conn), TOTAL_ROWS, BATCH_SIZE);
        cout << "插入" << TOTAL_ROWS << "条数据总耗时: " << fixed << setprecision(6) << t_insert << "秒，"
             << "平均每条: " << fixed << setprecision(8) << (t_insert / TOTAL_ROWS) << "秒" << endl;
        
        cout << "\n===== 3. 测试查询性能 =====" << endl;
        auto [___, t_select] = time_it(test_select_performance, ref(conn));
        cout << "查询测试总耗时: " << fixed << setprecision(6) << t_select << "秒" << endl;
        
        cout << "\n===== 4. 测试更新性能 =====" << endl;
        auto [____, t_update] = time_it(test_update_performance, ref(conn));
        cout << "更新测试总耗时: " << fixed << setprecision(6) << t_update << "秒" << endl;
        
        cout << "\n===== 5. 测试删除性能 =====" << endl;
        auto [_____, t_delete] = time_it(test_delete_performance, ref(conn));
        cout << "删除测试总耗时: " << fixed << setprecision(6) << t_delete << "秒" << endl;
        
        cout << "\n===== 6. 测试并发性能 =====" << endl;
        test_concurrent_performance();
    }
    catch (const exception& e) {
        cerr << "测试出错: " << e.what() << endl;
        return 1;
    }
    
    cout << "\n所有测试完成，资源已释放" << endl;
    return 0;
}
