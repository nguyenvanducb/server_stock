package main

import (
	"context"
	"fmt"
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
	// mongoURI := "mongodb://admin:abc123@127.0.0.1:27017/admin"
	mongoURI := "mongodb://admin:abc123@34.124.132.235:27017/admin"
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
	r.GET("/api/candles", getCandlesHandlerTest)

	r.Run(":8001")
}

var (
	cache sync.Map
	group singleflight.Group
)

func getCachedHandler(dbName, collName string, ttl time.Duration) gin.HandlerFunc {
	print("getCachedHandler")
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
	fmt.Println("===> getOrdersHandler called")

	symbol := c.Query("symbol")
	dateStr := c.Query("date") // dạng DD/MM/YYYY
	limitStr := c.DefaultQuery("limit", "20")
	pageStr := c.DefaultQuery("page", "1")

	if symbol == "" || dateStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol and date are required"})
		return
	}

	// Không cần parse date nếu client gửi đúng định dạng luôn

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}

	page, err := strconv.Atoi(pageStr)
	if err != nil || page <= 0 {
		page = 1
	}
	skip := (page - 1) * limit

	filter := bson.M{
		"Symbol":      symbol,
		"TradingDate": dateStr,
	}

	collection := mongoClient.Database("moneyflow").Collection("orders")
	opts := options.Find().
		SetSort(bson.M{"Time": -1}).
		SetSkip(int64(skip)).
		SetLimit(int64(limit))

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
func getCandlesHandler(c *gin.Context) {
	fmt.Println("===> getCandlesHandler called")

	symbol := c.Query("symbol")
	interval := c.DefaultQuery("interval", "1m")
	limitStr := c.DefaultQuery("limit", "200")

	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}

	filter := bson.M{"symbol": symbol, "interval": interval}

	collection := mongoClient.Database("binance").Collection("klines")
	opts := options.Find().
		SetSort(bson.M{"openTime": 1}).
		SetLimit(int64(limit))

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

	// Convert to Binance format (array of arrays)
	var klineArrays [][]interface{}
	for _, doc := range results {
		klineArray := []interface{}{
			doc["openTime"],                 // 0
			doc["open"],                     // 1
			doc["high"],                     // 2
			doc["low"],                      // 3
			doc["close"],                    // 4
			doc["volume"],                   // 5
			doc["closeTime"],                // 6
			doc["quoteAssetVolume"],         // 7
			doc["numberOfTrades"],           // 8
			doc["takerBuyBaseAssetVolume"],  // 9
			doc["takerBuyQuoteAssetVolume"], // 10
			"0",                             // 11 - Ignore field
		}
		klineArrays = append(klineArrays, klineArray)
	}

	// Trả về trực tiếp array, không wrap trong object
	c.JSON(http.StatusOK, klineArrays)
}
func getCandlesHandlerTest(c *gin.Context) {
	fmt.Println("===> getCandlesHandler called")

	// symbol := c.Query("symbol")

	// if symbol == "" {
	// 	c.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
	// 	return
	// }

	// Test data - định nghĩa riêng để dễ đọc
	testData := [][]interface{}{
		{1754087760000, "113173.10000000", "113200.00000000", "113126.03000000", "113171.56000000", "11.51838000", 1754087819999, "1303424.10296250", 4123, "6.19218000", "700647.49135320", "0"},
		{1754087820000, "113171.55000000", "113176.23000000", "113100.00000000", "113124.12000000", "28.39235000", 1754087879999, "3212011.26845080", 3355, "14.85657000", "1680669.35164740", "0"},
		{1754087880000, "113124.11000000", "113124.12000000", "113058.38000000", "113096.65000000", "17.49324000", 1754087939999, "1978235.59642310", 3610, "8.17975000", "924994.79957390", "0"},
		{1754087940000, "113096.66000000", "113105.23000000", "113054.74000000", "113062.00000000", "14.50072000", 1754087999999, "1639712.21451880", 1876, "6.96691000", "787721.06368220", "0"},
		{1754088000000, "113062.00000000", "113100.00000000", "113062.00000000", "113082.77000000", "20.18561000", 1754088059999, "2282674.83501130", 2142, "4.94598000", "559308.70558440", "0"},
		{1754088060000, "113082.77000000", "113180.33000000", "113054.74000000", "113058.24000000", "27.56954000", 1754088119999, "3118554.38003750", 5233, "11.16208000", "1262624.49435690", "0"},
		{1754088120000, "113058.24000000", "113119.06000000", "113001.00000000", "113080.10000000", "52.05078000", 1754088179999, "5884768.92340610", 6744, "18.45167000", "2086027.38109530", "0"},
		{1754088180000, "113080.10000000", "113136.62000000", "112888.00000000", "113030.97000000", "210.52141000", 1754088239999, "23787119.51266740", 15855, "30.96439000", "3498230.87770280", "0"},
		{1754088240000, "113030.97000000", "113079.84000000", "113000.00000000", "113066.00000000", "39.17942000", 1754088299999, "4428837.97544050", 5091, "19.48840000", "2202891.63880630", "0"},
	}

	c.JSON(http.StatusOK, testData)
}
