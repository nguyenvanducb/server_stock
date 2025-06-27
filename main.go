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
	r.GET("/api/tc", getCollectionHandler("moneyflow", "tc"))
	r.GET("/api/code", getCollectionHandler("moneyflow", "stock_code"))
	r.GET("/api/info", getCollectionHandler("moneyflow", "info_stocks"))

	r.Run(":8001")
}

// getCollectionHandler trả về một handler cho route cụ thể
func getCollectionHandler(dbName, collName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		collection := mongoClient.Database(dbName).Collection(collName)
		cursor, err := collection.Find(ctx, bson.D{})
		if err != nil {
			log.Println("Query error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Query failed"})
			return
		}
		defer cursor.Close(ctx)

		var results []bson.M
		if err := cursor.All(ctx, &results); err != nil {
			log.Println("Decode error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Decode failed"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"data": results})
	}
}
