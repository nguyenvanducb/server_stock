package main

import (
	"context"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/singleflight"
)

var mongoClient *mongo.Client

func main() {
	// Khởi tạo kết nối MongoDB một lần duy nhất
	var err error
	mongoURI := "mongodb://admin:abc123@127.0.0.1:27017/admin"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("MongoDB connection failed: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	r := gin.Default()

	// Đăng ký các route sử dụng handler chung
	r.GET("/api/tc", getCachedHandler("moneyflow", "tc", 1000*time.Second))
	r.GET("/api/code", getCachedHandler("moneyflow", "stock_code", 10*time.Second))
	r.GET("/api/info", getCachedHandler("moneyflow", "info_stocks", 100000*time.Second))
	r.GET("/api/orders", getOrdersHandler)
	r.GET("/api/orders/interval", getOrdersByIntervalHandlerSimple)

	r.Run(":8001")
}

var (
	cache sync.Map
	group singleflight.Group
)

func getCachedHandler(dbName, collName string, ttl time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		cacheKey := dbName + "_" + collName

		// Check cache trước
		if data, ok := cache.Load(cacheKey); ok {
			c.JSON(http.StatusOK, gin.H{"data": data})
			return
		}

		// Dùng singleflight để tránh nhiều request cùng truy vấn DB
		v, err, _ := group.Do(cacheKey, func() (interface{}, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			collection := mongoClient.Database(dbName).Collection(collName)
			cursor, err := collection.Find(ctx, bson.D{})
			if err != nil {
				return nil, err
			}
			defer cursor.Close(ctx)

			var results []bson.M
			if err := cursor.All(ctx, &results); err != nil {
				return nil, err
			}

			// Lưu vào cache
			cache.Store(cacheKey, results)

			// Tạo goroutine xóa sau TTL
			go func() {
				time.Sleep(ttl)
				cache.Delete(cacheKey)
			}()

			return results, nil
		})

		// Xử lý lỗi nếu có
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Trả dữ liệu đã được truy vấn
		c.JSON(http.StatusOK, gin.H{"data": v})
	}
}

func getOrdersHandler(c *gin.Context) {
	symbol := c.Query("symbol")
	dateStr := c.Query("date")
	limitStr := c.DefaultQuery("limit", "20")
	pageStr := c.DefaultQuery("page", "1")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200 // giới hạn tối đa
	}
	page, err := strconv.Atoi(pageStr)
	if err != nil || page <= 0 {
		page = 1
	}
	skip := (page - 1) * limit

	// Parse ngày
	layout := "2006-01-02"
	dayStart, err := time.Parse(layout, dateStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid date format. Use YYYY-MM-DD"})
		return
	}
	dayEnd := dayStart.Add(24 * time.Hour)

	filter := bson.M{
		"symbol": symbol,
		"time": bson.M{
			"$gte": dayStart,
			"$lt":  dayEnd,
		},
	}

	collection := mongoClient.Database("moneyflow").Collection("orders")
	opts := options.Find().SetSort(bson.M{"time": -1}).SetSkip(int64(skip)).SetLimit(int64(limit))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  results,
		"page":  page,
		"limit": limit,
	})
}

// Phiên bản đơn giản hơn nữa - chỉ lấy records theo khoảng cách thời gian
func getOrdersByIntervalHandlerSimple(c *gin.Context) {
	symbol := c.Query("symbol")
	intervalStr := c.DefaultQuery("interval", "1m")
	limitStr := c.DefaultQuery("limit", "20")
	pageStr := c.DefaultQuery("page", "1")

	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid interval format"})
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 400 {
		limit = 400
	}

	page, err := strconv.Atoi(pageStr)
	if err != nil || page <= 0 {
		page = 1
	}

	collection := mongoClient.Database("moneyflow").Collection("orders")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Tính toán offset cho phân trang
	offset := (page - 1) * limit

	// Lấy nhiều records để có buffer data cho phân trang
	bufferMultiplier := 30 // Tăng buffer để đảm bảo có đủ data sau khi lọc
	totalBufferLimit := limit * bufferMultiplier
	if page > 1 {
		// Với các trang sau, cần buffer nhiều hơn
		totalBufferLimit = (offset + limit) * bufferMultiplier
	}

	filter := bson.M{"symbol": symbol}
	opts := options.Find().
		SetSort(bson.M{"time": -1}).
		SetLimit(int64(totalBufferLimit))

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer cursor.Close(ctx)

	var allResults []bson.M
	if err := cursor.All(ctx, &allResults); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(allResults) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No data found"})
		return
	}

	// Làm tròn mốc thời gian cuối về phút
	firstTime, ok := allResults[0]["time"].(primitive.DateTime)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Invalid time format"})
		return
	}
	endTime := firstTime.Time().Truncate(time.Minute)

	// Map các phút gần nhất
	resultMap := make(map[int64]bson.M)
	for _, doc := range allResults {
		t, ok := doc["time"].(primitive.DateTime)
		if !ok {
			continue
		}
		docTime := t.Time().Truncate(time.Minute)
		unixMin := docTime.Unix()

		// Chỉ lưu lần đầu tiên gặp timestamp này
		if _, exists := resultMap[unixMin]; !exists {
			doc["time_truncated"] = docTime
			resultMap[unixMin] = doc
		}
	}

	// Tạo danh sách kết quả đầy đủ từ các thời điểm mục tiêu
	var allTimeResults []bson.M
	currentTarget := endTime

	// Tạo đủ data cho phân trang (cần tính toán số lượng cần thiết)
	maxResults := offset + limit
	for len(allTimeResults) < maxResults*2 { // Buffer thêm để đảm bảo
		unixMin := currentTarget.Unix()
		if doc, ok := resultMap[unixMin]; ok {
			doc["target_time"] = currentTarget
			allTimeResults = append(allTimeResults, doc)
		}
		currentTarget = currentTarget.Add(-interval)

		// Tránh vòng lặp vô hạn
		if currentTarget.Before(endTime.Add(-interval * time.Duration(maxResults*3))) {
			break
		}
	}

	// Đảo ngược để theo thứ tự thời gian tăng dần
	for i, j := 0, len(allTimeResults)-1; i < j; i, j = i+1, j-1 {
		allTimeResults[i], allTimeResults[j] = allTimeResults[j], allTimeResults[i]
	}

	// Áp dụng phân trang
	var results []bson.M
	totalResults := len(allTimeResults)

	if offset >= totalResults {
		results = []bson.M{}
	} else {
		endIndex := offset + limit
		if endIndex > totalResults {
			endIndex = totalResults
		}
		results = allTimeResults[offset:endIndex]
	}

	// Tính toán thông tin phân trang
	totalPages := int(math.Ceil(float64(totalResults) / float64(limit)))
	hasNextPage := page < totalPages
	hasPrevPage := page > 1

	c.JSON(http.StatusOK, gin.H{
		"data":     results,
		"symbol":   symbol,
		"interval": interval.String(),
		"end_time": endTime,
		"pagination": gin.H{
			"current_page":  page,
			"per_page":      limit,
			"total_results": totalResults,
			"total_pages":   totalPages,
			"has_next_page": hasNextPage,
			"has_prev_page": hasPrevPage,
		},
		"count": len(results),
	})
}
