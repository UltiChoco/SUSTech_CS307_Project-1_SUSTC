import psycopg2
import time
import random
import string
import os
from psycopg2 import pool
from psycopg2.extras import execute_batch
import concurrent.futures

DB_CONFIG = {
    "dbname": "sustc_db",
    "user": "postgres",
    "password": "676767",
    "host": "localhost",
    "port": "5432"
}
SCHEMA_NAME = "test_schema"
TABLE_NAME = f"{SCHEMA_NAME}.test_perf"
TOTAL_ROWS = 500000
BATCH_SIZE = 10000
CONCURRENT_WORKERS = 10


def generate_random_str(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def time_it(func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    end = time.perf_counter()
    return result, end - start


def init_schema_and_table(cursor):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
    cursor.execute(f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        CREATE TABLE {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            uid VARCHAR(32) NOT NULL UNIQUE,
            content TEXT NOT NULL,
            value INT NOT NULL,
            create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX idx_{SCHEMA_NAME}_test_perf_value ON {TABLE_NAME}(value);
    """)
    print(f"Schema '{SCHEMA_NAME}'和表 '{TABLE_NAME}' 创建完成")


def insert_large_data(cursor, total_rows, batch_size):
    print(f"开始插入 {total_rows} 条数据...")
    total_batches = (total_rows + batch_size - 1) // batch_size

    for batch in range(total_batches):
        data = [
            (
                generate_random_str(32),
                generate_random_str(100),
                random.randint(1, 1000)
            )
            for _ in range(batch_size) if (batch * batch_size + _) < total_rows
        ]

        execute_batch(cursor, f"""
            INSERT INTO {TABLE_NAME} (uid, content, value)
            VALUES (%s, %s, %s)
        """, data, page_size=1000)

        if (batch + 1) % 10 == 0:
            progress = (batch + 1) / total_batches * 100
            print(f"插入进度: {progress:.1f}% ({batch + 1}/{total_batches}批次)")


def test_select_performance(cursor, sample_size=1000):
    cursor.execute(f"SELECT MAX(id) FROM {TABLE_NAME}")
    max_id = cursor.fetchone()[0]
    if not max_id:
        return

    total_single = 0
    for _ in range(sample_size):
        _, t = time_it(
            cursor.execute,
            f"SELECT * FROM {TABLE_NAME} WHERE id = %s",
            (random.randint(1, max_id),)
        )
        total_single += t
    avg_single = total_single / sample_size

    total_cond = 0
    for _ in range(sample_size):
        val = random.randint(1, 1000)
        _, t = time_it(
            cursor.execute,
            f"SELECT * FROM {TABLE_NAME} WHERE value = %s LIMIT 10",
            (val,)
        )
        total_cond += t
    avg_cond = total_cond / sample_size

    _, t_range = time_it(
        cursor.execute,
        f"SELECT * FROM {TABLE_NAME} WHERE value BETWEEN %s AND %s ORDER BY create_time LIMIT 1000",
        (400, 600)
    )

    print(f"查询性能:")
    print(f"  单条主键查询（{sample_size}次平均）: {avg_single:.6f}秒/次")
    print(f"  条件查询（{sample_size}次平均）: {avg_cond:.6f}秒/次")
    print(f"  范围查询（1000条结果）: {t_range:.6f}秒")


def test_update_performance(cursor, sample_size=1000):
    cursor.execute(f"SELECT MAX(id) FROM {TABLE_NAME}")
    max_id = cursor.fetchone()[0]
    if not max_id:
        return

    total_time = 0
    for _ in range(sample_size):
        target_id = random.randint(1, max_id)
        new_val = random.randint(1, 1000)
        _, t = time_it(
            cursor.execute,
            f"UPDATE {TABLE_NAME} SET value = %s WHERE id = %s",
            (new_val, target_id)
        )
        total_time += t

    avg_update = total_time / sample_size
    print(f"更新性能（{sample_size}次平均）: {avg_update:.6f}秒/次")


def test_delete_performance(cursor, sample_size=1000):
    cursor.execute(f"""
        SELECT id, uid, content, value FROM {TABLE_NAME} 
        ORDER BY random() LIMIT {sample_size}
    """)
    backup = cursor.fetchall()
    if not backup:
        return

    total_time = 0
    deleted_ids = [row[0] for row in backup]
    for idx in deleted_ids:
        _, t = time_it(
            cursor.execute,
            f"DELETE FROM {TABLE_NAME} WHERE id = %s",
            (idx,)
        )
        total_time += t

    execute_batch(cursor, f"""
        INSERT INTO {TABLE_NAME} (id, uid, content, value)
        VALUES (%s, %s, %s, %s)
    """, backup)

    avg_delete = total_time / sample_size
    print(f"删除性能（{sample_size}次平均）: {avg_delete:.6f}秒/次")


def concurrent_operation(operation_id):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        # 30% select, 30% update, 40% delete_restore
        p = random.random()
        if p < 0.3:
            action = "select"
        elif p < 0.6:
            action = "update"
        else:
            action = "delete_restore"

        start = time.perf_counter()

        if action == "select":
            cursor.execute(f"SELECT MAX(id) FROM {TABLE_NAME}")
            max_id = cursor.fetchone()[0]
            if max_id:
                cursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE id = %s", (random.randint(1, max_id),))
                cursor.fetchone()     # ⭐ fetch result! 重要！

        elif action == "update":
            cursor.execute(f"SELECT MAX(id) FROM {TABLE_NAME}")
            max_id = cursor.fetchone()[0]
            if max_id:
                cursor.execute(
                    f"UPDATE {TABLE_NAME} SET value = %s WHERE id = %s",
                    (random.randint(1, 1000), random.randint(1, max_id))
                )

        elif action == "delete_restore":
            cursor.execute(f"SELECT id, uid, content, value FROM {TABLE_NAME} ORDER BY random() LIMIT 1")
            row = cursor.fetchone()
            if row:
                cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE id = %s", (row[0],))
                cursor.execute(
                    f"INSERT INTO {TABLE_NAME} (id, uid, content, value) VALUES (%s, %s, %s, %s)",
                    row
                )

        end = time.perf_counter()
        return (operation_id, action, end - start)

    except Exception as e:
        return (operation_id, "error", str(e))

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def test_concurrent_performance(workers=CONCURRENT_WORKERS, total_ops=1000):
    print(f"\n开始并发测试（{workers}线程，共{total_ops}次操作）...")
    start = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(concurrent_operation, i) for i in range(total_ops)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    total_time = time.perf_counter() - start

    success = 0
    errors = []
    action_times = {"select": [], "update": [], "delete_restore": []}
    for res in results:
        op_id, action, val = res
        if action == "error":
            errors.append(val)
        else:
            success += 1
            action_times[action].append(val)

    avg_times = {k: sum(v) / len(v) if v else 0 for k, v in action_times.items()}

    print(f"并发测试完成:")
    print(f"  总耗时: {total_time:.6f}秒")
    print(f"  总操作数: {total_ops}，成功: {success}，失败: {len(errors)}")
    print(f"  平均耗时（按操作类型）:")
    print(f"    查询: {avg_times['select']:.6f}秒/次")
    print(f"    更新: {avg_times['update']:.6f}秒/次")
    print(f"    删除恢复: {avg_times['delete_restore']:.6f}秒/次")
    if errors:
        print(f"  错误示例: {errors[0]}")


def main():
    connection_pool = None
    try:
        connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            **DB_CONFIG
        )
        conn = connection_pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()

        print("\n===== 1. 初始化Schema和表 =====")
        _, t_init = time_it(init_schema_and_table, cursor)
        print(f"初始化耗时: {t_init:.6f}秒")

        print("\n===== 2. 插入大量数据 =====")
        _, t_insert = time_it(insert_large_data, cursor, TOTAL_ROWS, BATCH_SIZE)
        print(f"插入{50}万条数据总耗时: {t_insert:.6f}秒，"
              f"平均每条: {t_insert / TOTAL_ROWS:.8f}秒")

        print("\n===== 3. 测试查询性能 =====")
        _, t_select = time_it(test_select_performance, cursor)
        print(f"查询测试总耗时: {t_select:.6f}秒")

        print("\n===== 4. 测试更新性能 =====")
        _, t_update = time_it(test_update_performance, cursor)
        print(f"更新测试总耗时: {t_update:.6f}秒")

        print("\n===== 5. 测试删除性能 =====")
        _, t_delete = time_it(test_delete_performance, cursor)
        print(f"删除测试总耗时: {t_delete:.6f}秒")

        print("\n===== 6. 测试并发性能 =====")
        test_concurrent_performance()

    except Exception as e:
        print(f"测试出错: {str(e)}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and connection_pool:
            connection_pool.putconn(conn)
        if connection_pool:
            connection_pool.closeall()
        print("\n所有测试完成，资源已释放")


if __name__ == "__main__":
    main()