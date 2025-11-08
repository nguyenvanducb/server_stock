package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
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
	r.GET("/api/info", getCachedHandler("moneyflow", "info_stocks", 10000*time.Second))
	r.GET("/api/orders", getOrdersHandler)
	r.GET("/api/candles", getCandlesHandlerTest)
	r.GET("/api/code", getCachedHandlerWithFilter("moneyflow", "stock_code", 10*time.Second, bson.M{
		"$expr": bson.M{
			"$eq": []interface{}{
				bson.M{"$strLenCP": "$Symbol"},
				3,
			},
		},
	}))
	r.GET("/api/exchange", getCachedHandlerWithFilter("moneyflow", "exchange", 10*time.Second, bson.M{}))
	r.GET("/api/onlycode", getOnlyCodeHandler()) // lấy duy nhất Mã Chứng khoán

	r.Run(":8001")
}

var (
	cache sync.Map
	group singleflight.Group
)

func getCachedHandlerWithFilter(dbName, collName string, ttl time.Duration, baseFilter bson.M) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Tạo filter động bằng cách copy baseFilter
		filter := bson.M{}
		for key, value := range baseFilter {
			filter[key] = value
		}

		// Thêm marketId filter nếu có
		marketId := c.Query("marketId")
		if marketId != "" {
			filter["MarketId"] = marketId
		}

		// Tạo cache key bao gồm marketId để tránh conflict
		cacheKey := dbName + "_" + collName + "_filtered"
		if marketId != "" {
			cacheKey += "_" + marketId
		}

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

			// Sử dụng filter đã được build động
			cursor, err := collection.Find(ctx, filter)
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
	dateStr := c.Query("date")
	limitStr := c.DefaultQuery("limit", "20")
	pageStr := c.DefaultQuery("page", "1")

	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "symbol is required"})
		return
	}

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

	collection := mongoClient.Database("moneyflow").Collection("matchs")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var filter bson.M
	var opts *options.FindOptions

	if dateStr == "" {
		// ✅ Không có date → lấy các lệnh mới nhất (ở cuối DB)
		filter = bson.M{"Symbol": symbol}

		opts = options.Find().
			SetSort(bson.M{"_id": -1}). // lấy document mới nhất
			SetSkip(int64(skip)).
			SetLimit(int64(limit))
	} else {
		// ✅ Chuẩn hoá ngày nếu có
		if strings.Contains(dateStr, "-") {
			parts := strings.Split(dateStr, "-")
			if len(parts) == 3 {
				dateStr = fmt.Sprintf("%s/%s/%s", parts[2], parts[1], parts[0])
			}
		}

		filter = bson.M{
			"Symbol":      symbol,
			"TradingDate": dateStr,
		}

		opts = options.Find().
			// SetSort(bson.M{"Time": -1}). // sort theo Time giảm dần
			SetSkip(int64(skip)).
			SetLimit(int64(limit))
	}

	fmt.Printf("Mongo filter: %+v\n", filter)

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

// Function trả về handler cho API onlycode
func getOnlyCodeHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		stockCode := c.Request.URL.RawQuery
		if stockCode == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing stock code"})
			return
		}

		result, err := fetchSingleStock("moneyflow", "stock_code", stockCode, c.Query("marketId"))
		if err != nil {
			if err == mongo.ErrNoDocuments {
				c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"data": result})
	}
}

// Function riêng để query MongoDB 1 stock
func fetchSingleStock(dbName, collName, stockCode, marketId string) (bson.M, error) {
	filter := bson.M{"Symbol": stockCode}
	if marketId != "" {
		filter["MarketId"] = marketId
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var result bson.M
	err := mongoClient.Database(dbName).Collection(collName).FindOne(ctx, filter).Decode(&result)
	return result, err
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
		{1752451200, 24.15, 24.25, 23.85, 23.9, 30215900},
		{1752537600, 23.95, 24.4, 23.7, 23.7, 19744300},
		{1752624000, 23.55, 23.7, 23.35, 23.35, 15335600},
		{1752710400, 23.4, 23.65, 23.3, 23.5, 24381300},
		{1752796800, 23.55, 23.9, 23.25, 23.35, 27664700},
		{1753056000, 23.35, 23.55, 23.25, 23.35, 16226300},
		{1753142400, 23.45, 23.6, 22.8, 23.35, 20912800},
		{1753228800, 23.7, 24.0, 23.5, 23.6, 24420300},
		{1753315200, 23.95, 24.2, 23.55, 23.7, 24600300},
		{1753401600, 23.85, 23.9, 23.5, 23.7, 14889300},
		{1753660800, 23.5, 23.95, 23.4, 23.5, 19752200},
		{1753747200, 23.35, 23.7, 23.35, 23.45, 12057800},
		{1753833600, 24.1, 24.85, 24.1, 23.7, 58587500},
		{1753920000, 24.7, 24.7, 24.15, 24.5, 19435500},
		{1754006400, 24.2, 24.3, 23.9, 24.2, 24655300},
		{1738540800, 23.9, 24.0, 23.6, 23.95, 28033000},
		{1738627200, 23.65, 23.7, 23.35, 23.6, 15704100},
		{1738713600, 23.4, 23.55, 23.3, 23.35, 13479500},
		{1738800000, 23.4, 23.6, 23.35, 23.35, 12900900},
		{1738886400, 23.6, 23.85, 23.55, 23.5, 13812400},
		{1739145600, 23.5, 23.55, 23.05, 23.65, 35039900},
		{1739232000, 23.1, 23.85, 23.1, 23.85, 16823100},
		{1739318400, 23.9, 23.95, 23.65, 23.85, 16346700},
		{1739404800, 23.85, 23.95, 23.6, 23.6, 21794300},
		{1739491200, 23.6, 23.95, 23.5, 23.9, 19690200},
		{1739750400, 24.05, 24.35, 23.85, 24.0, 25086200},
		{1739836800, 23.7, 23.85, 23.55, 23.75, 15661100},
		{1739923200, 24.15, 24.55, 24.0, 24.3, 48489100},
		{1740009600, 24.35, 24.8, 24.2, 24.5, 25856200},
		{1740096000, 24.5, 24.6, 24.15, 24.5, 18417100},
		{1740355200, 24.6, 24.85, 24.45, 24.65, 19508400},
		{1740441600, 25.0, 25.15, 24.7, 24.7, 26590700},
		{1740528000, 24.75, 24.8, 24.35, 24.45, 15143300},
		{1740614400, 24.4, 25.8, 24.35, 25.5, 41265000},
		{1740700800, 25.5, 26.3, 25.15, 25.75, 58323300},
		{1740960000, 25.9, 26.15, 25.6, 25.9, 21022600},
		{1741046400, 26.2, 26.45, 26.05, 26.3, 33309100},
		{1741132800, 26.4, 27.15, 26.25, 27.05, 65007600},
		{1741219200, 27.25, 28.85, 28.6, 28.55, 72536400},
		{1741305600, 28.9, 29.15, 28.95, 28.95, 51768100},
		{1741564800, 28.95, 30.0, 29.0, 29.65, 62931400},
		{1741651200, 29.8, 29.95, 29.65, 29.75, 51458700},
		{1741737600, 29.8, 31.8, 30.3, 30.5, 62849100},
		{1741824000, 30.5, 31.9, 31.35, 31.6, 47110600},
		{1741910400, 32.0, 32.1, 31.5, 31.9, 49672400},
		{1742169600, 32.05, 32.3, 30.8, 31.8, 38374200},
		{1742256000, 32.05, 32.1, 30.55, 31.35, 55033300},
		{1742342400, 31.0, 32.0, 31.9, 32.0, 61716000},
		{1742428800, 32.3, 33.35, 31.35, 32.25, 43849400},
		{1742515200, 32.3, 32.35, 32.0, 32.0, 43190300},
	}

	c.JSON(http.StatusOK, testData)
}
