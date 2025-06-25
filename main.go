package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	r := gin.Default()

	r.GET("/api/tc", func(c *gin.Context) {
		// MongoDB URI nội bộ
		mongoURI := "mongodb://admin:abc123@127.0.0.1:27017/admin"

		// Kết nối MongoDB
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		clientOpts := options.Client().ApplyURI(mongoURI)
		client, err := mongo.Connect(ctx, clientOpts)
		if err != nil {
			log.Println("MongoDB connection error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "MongoDB connection failed"})
			return
		}

		// Truy cập collection tc trong db moneyflow
		collection := client.Database("moneyflow").Collection("tc")

		// Truy vấn tất cả dữ liệu
		cursor, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Println("Query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Query failed"})
			return
		}
		defer cursor.Close(ctx)

		var results []bson.M
		if err := cursor.All(ctx, &results); err != nil {
			log.Println("Cursor decode error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Decode failed"})
			return
		}

		// Trả về dữ liệu JSON
		c.JSON(http.StatusOK, results)
	})

	// Chạy server tại port 8080
	r.Run(":8080")
}
