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
	mongoURI := "mongodb://admin:abc123@127.0.0.1:27017/admin"
	// mongoURI := "mongodb://admin:abc123@34.124.132.235:27017/admin"
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
		// [OpenTime, Open, High, Low, Close, Volume, CloseTime, QuoteVolume, Trades, TakerBuyVol, TakerBuyQuoteVol, Ignore]

		// 14:36:00 - Nến đầu tiên, còn ở mức cao
		{1754094360000, "33400.00", "33450.00", "33350.00", "33380.00", "1580000", 1754094419999, "52794000000", 425, "850000", "28376000000", "0"},

		// 14:37:00 - Bắt đầu giảm nhẹ
		{1754094420000, "33380.00", "33400.00", "33320.00", "33340.00", "1750000", 1754094479999, "58345000000", 485, "900000", "30027000000", "0"},

		// 14:38:00 - Tiếp tục xu hướng giảm
		{1754094480000, "33340.00", "33360.00", "33280.00", "33300.00", "1920000", 1754094539999, "63936000000", 512, "980000", "32658000000", "0"},

		// 14:39:00 - Giảm mạnh, test vùng support
		{1754094540000, "33300.00", "33320.00", "33200.00", "33220.00", "2840000", 1754094599999, "94428800000", 678, "1420000", "47214400000", "0"},

		// 14:40:00 - Bounse nhẹ từ support
		{1754094600000, "33220.00", "33280.00", "33180.00", "33260.00", "2150000", 1754094659999, "71499000000", 589, "1180000", "39234000000", "0"},

		// 14:41:00 - Consolidation, volume giảm
		{1754094660000, "33260.00", "33290.00", "33230.00", "33250.00", "1680000", 1754094719999, "55860000000", 435, "890000", "29577000000", "0"},

		// 14:42:00 - Test lại vùng thấp
		{1754094720000, "33250.00", "33270.00", "33180.00", "33200.00", "2280000", 1754094779999, "75744000000", 624, "1140000", "37872000000", "0"},

		// 14:43:00 - Sideway tại support
		{1754094780000, "33200.00", "33240.00", "33180.00", "33210.00", "1950000", 1754094839999, "64695000000", 543, "1020000", "33858000000", "0"},

		// 14:44:00 - Volume tăng, slight recovery
		{1754094840000, "33210.00", "33250.00", "33190.00", "33220.00", "2320000", 1754094899999, "77048000000", 656, "1250000", "41530000000", "0"},

		// 14:45:00 - Nến cuối, kết thúc phiên tại 33200
		{1754094900000, "33220.00", "33230.00", "33180.00", "33200.00", "1890000", 1754094959999, "62748000000", 498, "980000", "32527000000", "0"},
	}

	c.JSON(http.StatusOK, testData)
}
