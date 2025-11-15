package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// 数据库配置
const (
	DB_URL       = "host=localhost port=5432 user=postgres password=Dr141592 dbname=project sslmode=disable"
	SCHEMA_NAME  = "test_schema"
	TABLE_NAME   = SCHEMA_NAME + ".test_perf"
	TOTAL_ROWS   = 500000
	BATCH_SIZE   = 10000
	CONCURRENT   = 10
	SAMPLE_SIZE  = 1000
	CHARACTERS   = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)

var rnd *rand.Rand

func init() {
	// 初始化随机数生成器
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// 生成随机字符串
func generateRandomStr(length int) string {
	var sb strings.Builder
	sb.Grow(length)
	for i := 0; i < length; i++ {
		sb.WriteByte(CHARACTERS[rnd.Intn(len(CHARACTERS))])
	}
	return sb.String()
}

// 初始化Schema和表
func initSchemaAndTable(db *sql.DB) error {
	_, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME)
	if err != nil {
		return err
	}

	_, err = db.Exec("DROP TABLE IF EXISTS " + TABLE_NAME)
	if err != nil {
		return err
	}

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			uid VARCHAR(32) NOT NULL UNIQUE,
			content TEXT NOT NULL,
			value INT NOT NULL,
			create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`, TABLE_NAME)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(
		"CREATE INDEX idx_%s_test_perf_value ON %s(value)",
		SCHEMA_NAME, TABLE_NAME))
	if err != nil {
		return err
	}

	fmt.Printf("Schema '%s'和表 '%s' 创建完成\n", SCHEMA_NAME, TABLE_NAME)
	return nil
}

// 批量插入数据
func insertLargeData(db *sql.DB, totalRows, batchSize int) error {
	fmt.Printf("开始插入 %d 条数据...\n", totalRows)
	totalBatches := (totalRows + batchSize - 1) / batchSize

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (uid, content, value) VALUES ($1, $2, $3)",
		TABLE_NAME))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for batch := 0; batch < totalBatches; batch++ {
		currentBatchSize := batchSize
		if remaining := totalRows - batch*batchSize; remaining < batchSize {
			currentBatchSize = remaining
		}

		for i := 0; i < currentBatchSize; i++ {
			uid := generateRandomStr(32)
			content := generateRandomStr(100)
			value := rnd.Intn(1000) + 1

			_, err := stmt.Exec(uid, content, value)
			if err != nil {
				return err
			}
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		// 为下一批次准备新事务
		tx, err = db.Begin()
		if err != nil {
			return err
		}
		stmt, err = tx.Prepare(fmt.Sprintf(
			"INSERT INTO %s (uid, content, value) VALUES ($1, $2, $3)",
			TABLE_NAME))
		if err != nil {
			return err
		}

		if (batch+1)%10 == 0 {
			progress := float64(batch+1) / float64(totalBatches) * 100
			fmt.Printf("插入进度: %.1f%% (%d/%d批次)\n", progress, batch+1, totalBatches)
		}
	}

	// 提交最后一个事务
	tx.Commit()
	return nil
}

// 获取最大ID
func getMaxId(db *sql.DB) (int, error) {
	var maxId int
	err := db.QueryRow("SELECT MAX(id) FROM " + TABLE_NAME).Scan(&maxId)
	return maxId, err
}

// 测试查询性能
func testSelectPerformance(db *sql.DB) error {
	maxId, err := getMaxId(db)
	if err != nil {
		return err
	}
	if maxId == 0 {
		return nil
	}

	// 单条主键查询
	var totalSingle int64
	stmt, err := db.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE id = $1", TABLE_NAME))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < SAMPLE_SIZE; i++ {
		id := rnd.Intn(maxId) + 1
		start := time.Now()
		rows, err := stmt.Query(id)
		if err != nil {
			return err
		}
		rows.Close() // 立即关闭结果集
		totalSingle += time.Since(start).Nanoseconds()
	}
	avgSingle := float64(totalSingle) / 1e9 / float64(SAMPLE_SIZE)

	// 条件查询
	var totalCond int64
	condStmt, err := db.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE value = $1 LIMIT 10", TABLE_NAME))
	if err != nil {
		return err
	}
	defer condStmt.Close()

	for i := 0; i < SAMPLE_SIZE; i++ {
		value := rnd.Intn(1000) + 1
		start := time.Now()
		rows, err := condStmt.Query(value)
		if err != nil {
			return err
		}
		rows.Close()
		totalCond += time.Since(start).Nanoseconds()
	}
	avgCond := float64(totalCond) / 1e9 / float64(SAMPLE_SIZE)

	// 范围查询
	rangeStmt, err := db.Prepare(fmt.Sprintf(
		"SELECT * FROM %s WHERE value BETWEEN $1 AND $2 ORDER BY create_time LIMIT 1000",
		TABLE_NAME))
	if err != nil {
		return err
	}
	defer rangeStmt.Close()

	start := time.Now()
	rows, err := rangeStmt.Query(400, 600)
	if err != nil {
		return err
	}
	rows.Close()
	rangeSec := time.Since(start).Seconds()

	fmt.Println("查询性能:")
	fmt.Printf("  单条主键查询（%d次平均）: %.6f秒/次\n", SAMPLE_SIZE, avgSingle)
	fmt.Printf("  条件查询（%d次平均）: %.6f秒/次\n", SAMPLE_SIZE, avgCond)
	fmt.Printf("  范围查询（1000条结果）: %.6f秒\n", rangeSec)

	return nil
}

// 测试更新性能
func testUpdatePerformance(db *sql.DB) error {
	maxId, err := getMaxId(db)
	if err != nil {
		return err
	}
	if maxId == 0 {
		return nil
	}

	var totalTime int64
	stmt, err := db.Prepare(fmt.Sprintf("UPDATE %s SET value = $1 WHERE id = $2", TABLE_NAME))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < SAMPLE_SIZE; i++ {
		targetId := rnd.Intn(maxId) + 1
		newValue := rnd.Intn(1000) + 1

		start := time.Now()
		_, err := stmt.Exec(newValue, targetId)
		if err != nil {
			return err
		}
		totalTime += time.Since(start).Nanoseconds()
	}

	avgUpdate := float64(totalTime) / 1e9 / float64(SAMPLE_SIZE)
	fmt.Printf("更新性能（%d次平均）: %.6f秒/次\n", SAMPLE_SIZE, avgUpdate)
	return nil
}

// 测试删除性能
func testDeletePerformance(db *sql.DB) error {
	// 备份数据
	type backupRow struct {
		id      int
		uid     string
		content string
		value   int
	}
	var backup []backupRow

	rows, err := db.Query(fmt.Sprintf(
		"SELECT id, uid, content, value FROM %s ORDER BY random() LIMIT %d",
		TABLE_NAME, SAMPLE_SIZE))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var row backupRow
		if err := rows.Scan(&row.id, &row.uid, &row.content, &row.value); err != nil {
			return err
		}
		backup = append(backup, row)
	}

	if len(backup) == 0 {
		return nil
	}

	// 执行删除
	var totalTime int64
	deleteStmt, err := db.Prepare(fmt.Sprintf("DELETE FROM %s WHERE id = $1", TABLE_NAME))
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	for _, row := range backup {
		start := time.Now()
		_, err := deleteStmt.Exec(row.id)
		if err != nil {
			return err
		}
		totalTime += time.Since(start).Nanoseconds()
	}

	// 恢复数据
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	insertStmt, err := tx.Prepare(fmt.Sprintf(
		"INSERT INTO %s (id, uid, content, value) VALUES ($1, $2, $3, $4)",
		TABLE_NAME))
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for _, row := range backup {
		_, err := insertStmt.Exec(row.id, row.uid, row.content, row.value)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	avgDelete := float64(totalTime) / 1e9 / float64(SAMPLE_SIZE)
	fmt.Printf("删除性能（%d次平均）: %.6f秒/次\n", SAMPLE_SIZE, avgDelete)
	return nil
}

// 并发任务结果
type taskResult struct {
	id     int
	action string
	time   float64
	error  string
}

// 处理查询操作
func handleSelect(db *sql.DB) error {
	maxId, err := getMaxId(db)
	if err != nil || maxId <= 0 {
		return err
	}

	id := rnd.Intn(maxId) + 1
	stmt, err := db.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE id = $1", TABLE_NAME))
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query(id)
	if err != nil {
		return err
	}
	rows.Close()
	return nil
}

// 处理更新操作
func handleUpdate(db *sql.DB) error {
	maxId, err := getMaxId(db)
	if err != nil || maxId <= 0 {
		return err
	}

	targetId := rnd.Intn(maxId) + 1
	newValue := rnd.Intn(1000) + 1

	stmt, err := db.Prepare(fmt.Sprintf("UPDATE %s SET value = $1 WHERE id = $2", TABLE_NAME))
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(newValue, targetId)
	return err
}

// 处理删除恢复操作
func handleDeleteRestore(db *sql.DB) error {
	var id int
	var uid, content string
	var value int

	err := db.QueryRow(fmt.Sprintf(
		"SELECT id, uid, content, value FROM %s ORDER BY random() LIMIT 1",
		TABLE_NAME)).Scan(&id, &uid, &content, &value)
	if err != nil {
		return err
	}

	// 删除
	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", TABLE_NAME), id)
	if err != nil {
		return err
	}

	// 恢复
	_, err = db.Exec(fmt.Sprintf(
		"INSERT INTO %s (id, uid, content, value) VALUES ($1, $2, $3, $4)",
		TABLE_NAME), id, uid, content, value)
	return err
}

// 并发任务执行函数
func concurrentTask(id int, results chan<- taskResult) {
	result := taskResult{id: id}
	db, err := sql.Open("postgres", DB_URL)
	if err != nil {
		result.action = "error"
		result.error = err.Error()
		results <- result
		return
	}
	defer db.Close()

	// 随机选择操作类型
	var action string
	switch rnd.Intn(3) {
	case 0:
		action = "select"
	case 1:
		action = "update"
	default:
		action = "delete_restore"
	}

	start := time.Now()
	var execErr error

	switch action {
	case "select":
		execErr = handleSelect(db)
	case "update":
		execErr = handleUpdate(db)
	case "delete_restore":
		execErr = handleDeleteRestore(db)
	}

	if execErr != nil {
		result.action = "error"
		result.error = execErr.Error()
	} else {
		result.action = action
		result.time = time.Since(start).Seconds()
	}

	results <- result
}

// 测试并发性能
func testConcurrentPerformance() {
	totalOps := 1000
	fmt.Printf("开始并发测试（%d线程，共%d次操作）...\n", CONCURRENT, totalOps)

	start := time.Now()
	results := make(chan taskResult, totalOps)
	var wg sync.WaitGroup

	// 启动工作池
	pool := make(chan struct{}, CONCURRENT)
	for i := 0; i < totalOps; i++ {
		pool <- struct{}{}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() { <-pool }()
			concurrentTask(id, results)
		}(i)
	}

	// 等待所有任务完成并关闭通道
	go func() {
		wg.Wait()
		close(results)
	}()

	// 收集结果
	success := 0
	var errors []string
	actionTimes := map[string][]float64{
		"select":         {},
		"update":         {},
		"delete_restore": {},
	}

	for res := range results {
		if res.action == "error" {
			errors = append(errors, res.error)
		} else {
			success++
			actionTimes[res.action] = append(actionTimes[res.action], res.time)
		}
	}

	totalTime := time.Since(start).Seconds()

	// 计算平均时间
	avgTimes := make(map[string]float64)
	for action, times := range actionTimes {
		if len(times) == 0 {
			avgTimes[action] = 0
			continue
		}
		var sum float64
		for _, t := range times {
			sum += t
		}
		avgTimes[action] = sum / float64(len(times))
	}

	fmt.Println("并发测试完成:")
	fmt.Printf("  总耗时: %.6f秒\n", totalTime)
	fmt.Printf("  总操作数: %d，成功: %d，失败: %d\n", totalOps, success, len(errors))
	fmt.Println("  平均耗时（按操作类型）:")
	fmt.Printf("    查询: %.6f秒/次\n", avgTimes["select"])
	fmt.Printf("    更新: %.6f秒/次\n", avgTimes["update"])
	fmt.Printf("    删除恢复: %.6f秒/次\n", avgTimes["delete_restore"])

	if len(errors) > 0 {
		fmt.Printf("  错误示例: %s\n", errors[0])
	}
}

func main() {
	// 连接数据库
	db, err := sql.Open("postgres", DB_URL)
	if err != nil {
		fmt.Printf("无法连接数据库: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 测试连接
	if err := db.Ping(); err != nil {
		fmt.Printf("数据库连接失败: %v\n", err)
		os.Exit(1)
	}

	// 1. 初始化Schema和表
	fmt.Println("\n===== 1. 初始化Schema和表 =====")
	initStart := time.Now()
	if err := initSchemaAndTable(db); err != nil {
		fmt.Printf("初始化失败: %v\n", err)
		return
	}
	initTime := time.Since(initStart).Seconds()
	fmt.Printf("初始化耗时: %.6f秒\n", initTime)

	// 2. 插入大量数据
	fmt.Println("\n===== 2. 插入大量数据 =====")
	insertStart := time.Now()
	if err := insertLargeData(db, TOTAL_ROWS, BATCH_SIZE); err != nil {
		fmt.Printf("插入数据失败: %v\n", err)
		return
	}
	insertTime := time.Since(insertStart).Seconds()
	fmt.Printf("插入%d万条数据总耗时: %.6f秒，平均每条: %.8f秒\n",
		TOTAL_ROWS/10000, insertTime, insertTime/float64(TOTAL_ROWS))

	// 3. 测试查询性能
	fmt.Println("\n===== 3. 测试查询性能 =====")
	selectStart := time.Now()
	if err := testSelectPerformance(db); err != nil {
		fmt.Printf("查询测试失败: %v\n", err)
		return
	}
	selectTime := time.Since(selectStart).Seconds()
	fmt.Printf("查询测试总耗时: %.6f秒\n", selectTime)

	// 4. 测试更新性能
	fmt.Println("\n===== 4. 测试更新性能 =====")
	updateStart := time.Now()
	if err := testUpdatePerformance(db); err != nil {
		fmt.Printf("更新测试失败: %v\n", err)
		return
	}
	updateTime := time.Since(updateStart).Seconds()
	fmt.Printf("更新测试总耗时: %.6f秒\n", updateTime)

	// 5. 测试删除性能
	fmt.Println("\n===== 5. 测试删除性能 =====")
	deleteStart := time.Now()
	if err := testDeletePerformance(db); err != nil {
		fmt.Printf("删除测试失败: %v\n", err)
		return
	}
	deleteTime := time.Since(deleteStart).Seconds()
	fmt.Printf("删除测试总耗时: %.6f秒\n", deleteTime)

	// 6. 测试并发性能
	fmt.Println("\n===== 6. 测试并发性能 =====")
	testConcurrentPerformance()

	fmt.Println("\n所有测试完成，资源已释放")
}
