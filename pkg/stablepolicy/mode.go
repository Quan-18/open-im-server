package stablepolicy

import (
	"context"
	"errors"
	"fmt"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// VerifyMode 让独立旧转存进程同样尊重已经持久启用的模式。
func VerifyMode(ctx context.Context, db *mongo.Database, cfg config.StableMessage) error {
	var marker struct {
		Version   int    `bson:"version"`
		Namespace string `bson:"namespace"`
	}
	err := db.Collection("stable_message_mode").FindOne(ctx, bson.M{"_id": "mode"}).Decode(&marker)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	if err != nil {
		return err
	}
	if !cfg.Enabled || marker.Version != 1 || marker.Namespace != cfg.SourceNamespace {
		return fmt.Errorf("stable mode marker does not match this service")
	}
	return nil
}
