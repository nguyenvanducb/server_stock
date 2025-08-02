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
		// [OpenTime(ms), Open, High, Low, Close, Volume, CloseTime(ms), QuoteVolume, NumTrades, TakerBuyVolume, TakerBuyQuoteVolume, Ignore]

		// 10 nến cuối từ dữ liệu TradingView (daily candles)
		{1753660800000, "32.00", "33.35", "31.35", "32.25", "61716000", 1753747199999, "2008066400.00", 61716, "33943800", "1104436520.00", "0"},
		{1753747200000, "32.15", "32.35", "32.00", "32.00", "43190300", 1753833599999, "1379281792.00", 43190, "23754665", "758505985.60", "0"},
		{1753833600000, "34.80", "34.10", "34.45", "34.00", "62122900", 1753919999999, "2122379460.00", 62122, "34167595", "1167308603.00", "0"},
		{1753920000000, "36.10", "35.85", "33.30", "35.80", "49584700", 1754006399999, "1752948226.00", 49584, "27271585", "963420824.30", "0"},
		{1754006400000, "33.60", "36.20", "32.70", "33.30", "104999100", 1754092799999, "3499969830.00", 104999, "57749505", "1924982506.50", "0"},
		{1754092800000, "34.10", "34.15", "33.10", "33.90", "62743900", 1754179199999, "2127080210.00", 62743, "34509145", "1169894115.50", "0"},
		{1754179200000, "34.00", "34.60", "33.10", "34.20", "49380200", 1754265599999, "1665866840.00", 49380, "27159110", "916226762.00", "0"},
		{1754265600000, "33.80", "34.80", "33.20", "33.20", "56677600", 1754351999999, "1920682080.00", 56677, "31172680", "1056375344.00", "0"},
		{1754352000000, "34.20", "34.50", "33.80", "34.00", "48234500", 1754438399999, "1653613500.00", 48234, "26528975", "909424425.00", "0"},
		{1754438400000, "33.50", "34.00", "33.10", "33.70", "52341200", 1754524799999, "1763553040.00", 52341, "28787660", "969793972.00", "0"},
	}

	c.JSON(http.StatusOK, testData)
}
