package main

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	r.GET("/api/tc", getCachedHandler("moneyflow", "stock_code", 10*time.Second))
	r.GET("/api/tc", getCachedHandler("moneyflow", "info_stocks", 100000*time.Second))

	r.Run(":8001")
}

var cache sync.Map

func getCachedHandler(dbName, collName string, ttl time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		cacheKey := dbName + "_" + collName

		if data, ok := cache.Load(cacheKey); ok {
			c.JSON(http.StatusOK, gin.H{"data": data})
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		collection := mongoClient.Database(dbName).Collection(collName)
		cursor, err := collection.Find(ctx, bson.D{})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Query failed"})
			return
		}
		defer cursor.Close(ctx)

		var results []bson.M
		if err := cursor.All(ctx, &results); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Decode failed"})
			return
		}

		cache.Store(cacheKey, results)

		// Xóa cache sau TTL
		go func() {
			time.Sleep(ttl)
			cache.Delete(cacheKey)
		}()

		c.JSON(http.StatusOK, gin.H{"data": results})
	}
}
