package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type FCMToken struct {
	ID       primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Token    string             `bson:"token" json:"token"`
	IsActive bool               `bson:"isActive" json:"isActive"`

	UserID   *primitive.ObjectID `bson:"userId,omitempty" json:"userId,omitempty"`
	AppID    string              `bson:"appId,omitempty" json:"appId,omitempty"`
	Platform string              `bson:"platform,omitempty" json:"platform,omitempty"`
	DeviceID string              `bson:"deviceId,omitempty" json:"deviceId,omitempty"`

	DeviceModel string `bson:"deviceModel,omitempty" json:"deviceModel,omitempty"`
	OSVersion   string `bson:"osVersion,omitempty" json:"osVersion,omitempty"`
	AppVersion  string `bson:"appVersion,omitempty" json:"appVersion,omitempty"`

	LastSeenAt time.Time `bson:"lastSeenAt,omitempty" json:"lastSeenAt,omitempty"`
	CreatedAt  time.Time `bson:"createdAt" json:"createdAt"`
	UpdatedAt  time.Time `bson:"updatedAt" json:"updatedAt"`
}

const (
	fcmCollectionName = "fcm_tokens"
	defaultAppID      = "default"
)

func fcmColl(db *mongo.Database) *mongo.Collection {
	return db.Collection(fcmCollectionName)
}

func normalizeAppID(appID string) string {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return defaultAppID
	}
	return appID
}

func normalizeToken(token string) string {
	return strings.TrimSpace(token)
}

// EnsureFCMIndexes creates indexes for fcm_tokens collection.
// - unique(appId, token)
// - isActive
// - userId + isActive (for future expansion)
func EnsureFCMIndexes(ctx context.Context, db *mongo.Database) error {
	coll := fcmColl(db)

	models := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "appId", Value: 1}, {Key: "token", Value: 1}},
			Options: options.Index().
				SetUnique(true).
				SetName("uniq_appid_token"),
		},
		{
			Keys:    bson.D{{Key: "isActive", Value: 1}},
			Options: options.Index().SetName("idx_isActive"),
		},
		{
			Keys:    bson.D{{Key: "userId", Value: 1}, {Key: "isActive", Value: 1}},
			Options: options.Index().SetName("idx_userId_isActive"),
		},
	}

	_, err := coll.Indexes().CreateMany(ctx, models)
	return err
}

type RegisterTokenReq struct {
	Token       string `json:"token" binding:"required"`
	AppID       string `json:"appId"`    // optional
	Platform    string `json:"platform"` // optional
	DeviceID    string `json:"deviceId"` // optional
	DeviceModel string `json:"deviceModel"`
	OSVersion   string `json:"osVersion"`
	AppVersion  string `json:"appVersion"`

	// Mở rộng sau: nếu bạn muốn gắn token với user ngay từ bây giờ
	// UserID string `json:"userId"`
}

func RegisterFCMToken(ctx context.Context, db *mongo.Database, req RegisterTokenReq) error {
	token := normalizeToken(req.Token)
	if token == "" {
		return errors.New("token is required")
	}
	appID := normalizeAppID(req.AppID)

	now := time.Now()

	filter := bson.M{
		"appId": appID,
		"token": token,
	}
	update := bson.M{
		"$set": bson.M{
			"appId":       appID, // đảm bảo luôn có
			"token":       token,
			"platform":    strings.TrimSpace(req.Platform),
			"deviceId":    strings.TrimSpace(req.DeviceID),
			"deviceModel": strings.TrimSpace(req.DeviceModel),
			"osVersion":   strings.TrimSpace(req.OSVersion),
			"appVersion":  strings.TrimSpace(req.AppVersion),

			"isActive":   true,
			"lastSeenAt": now,
			"updatedAt":  now,
		},
		"$setOnInsert": bson.M{
			"createdAt": now,
		},
	}

	_, err := fcmColl(db).UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	return err
}

func DeactivateFCMToken(ctx context.Context, db *mongo.Database, appID, token string) error {
	appID = normalizeAppID(appID)
	token = normalizeToken(token)
	if token == "" {
		return errors.New("token is required")
	}

	_, err := fcmColl(db).UpdateOne(
		ctx,
		bson.M{"appId": appID, "token": token},
		bson.M{"$set": bson.M{"isActive": false, "updatedAt": time.Now()}},
	)
	return err
}

func ListActiveFCMTokens(ctx context.Context, db *mongo.Database, appID string) ([]string, error) {
	appID = normalizeAppID(appID)

	opts := options.Find().
		SetProjection(bson.M{"token": 1, "_id": 0})

	cur, err := fcmColl(db).Find(ctx, bson.M{"appId": appID, "isActive": true}, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	tokens := make([]string, 0, 256)
	for cur.Next(ctx) {
		var row struct {
			Token string `bson:"token"`
		}
		if err := cur.Decode(&row); err != nil {
			return nil, err
		}
		if t := normalizeToken(row.Token); t != "" {
			tokens = append(tokens, t)
		}
	}
	return tokens, cur.Err()
}

func CountActiveFCMTokens(ctx context.Context, db *mongo.Database, appID string) (int64, error) {
	appID = normalizeAppID(appID)
	return fcmColl(db).CountDocuments(ctx, bson.M{"appId": appID, "isActive": true})
}

/* -------------------- Gin Handlers -------------------- */

// POST /api/fcm/register
func RegisterFCMTokenHandler(db *mongo.Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req RegisterTokenReq
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()

		if err := RegisterFCMToken(ctx, db, req); err != nil {
			// Nếu bị duplicate do race, UpdateOne upsert + unique index vẫn có thể báo lỗi
			// Nhưng thường hiếm; bạn có thể retry 1 lần nếu muốn.
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{"ok": true})
	}
}

type DeactivateReq struct {
	Token string `json:"token" binding:"required"`
	AppID string `json:"appId"`
}

// POST /api/fcm/deactivate
func DeactivateFCMTokenHandler(db *mongo.Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req DeactivateReq
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()

		if err := DeactivateFCMToken(ctx, db, req.AppID, req.Token); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"ok": true})
	}
}

// GET /api/fcm/tokens?appId=default
func ListActiveTokensHandler(db *mongo.Database) gin.HandlerFunc {
	return func(c *gin.Context) {
		appID := c.Query("appId")

		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()

		tokens, err := ListActiveFCMTokens(ctx, db, appID)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		count, _ := CountActiveFCMTokens(ctx, db, appID) // best effort
		c.JSON(200, gin.H{
			"appId":  normalizeAppID(appID),
			"count":  count,
			"tokens": tokens,
		})
	}
}
