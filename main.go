package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/singleflight"
)

var mongoClient *mongo.Client

func main() {
	// Khởi tạo kết nối MongoDB một lần duy nhất
	var err error
	mongoURI := "mongodb://admin:abc123@34.124.191.19:27017/admin"
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
	limitStr := c.DefaultQuery("limit", "20")
	pageStr := c.DefaultQuery("page", "1")

	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > 400 {
		limit = 20
	}

	page, _ := strconv.Atoi(pageStr)
	if page <= 0 {
		page = 1
	}

	collection := mongoClient.Database("moneyflow").Collection("orders")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	offset := (page - 1) * limit

	filter := bson.M{"Symbol": symbol}
	opts := options.Find().
		SetSort(bson.M{"Time": -1}).
		SetSkip(int64(offset)).
		SetLimit(int64(limit))

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
		"data": results,
	})
}
