package stablepolicy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"google.golang.org/protobuf/proto"
)

// GuardRedisMessage 在进入旧批处理前持久隔离误入的正式单聊。
func GuardRedisMessage(ctx context.Context, db *mongo.Database, value mq.Message) (bool, error) {
	if !config.StableMode() {
		return false, nil
	}
	var data msg.MsgDataToMQ
	if proto.Unmarshal(value.Value(), &data) != nil || data.MsgData == nil {
		return false, nil
	}
	if CheckMessage(data.MsgData) == nil {
		return false, nil
	}
	// 只保存消息，丢弃旧队列外层可能携带的认证 Token。
	payload, err := proto.Marshal(data.MsgData)
	if err != nil {
		return false, err
	}
	return quarantine(ctx, db, value, "toRedis", payload)
}

// GuardMongoMessage 禁止已携带旧序号的单聊正文绕过稳定事务。
func GuardMongoMessage(ctx context.Context, db *mongo.Database, value mq.Message) (bool, error) {
	if !config.StableMode() {
		return false, nil
	}
	var data msg.MsgDataToMongoByMQ
	if proto.Unmarshal(value.Value(), &data) != nil || !Protected(data.ConversationID) {
		return false, nil
	}
	payload, err := proto.Marshal(&data)
	if err != nil {
		return false, err
	}
	return quarantine(ctx, db, value, "toMongo", payload)
}

func quarantine(ctx context.Context, db *mongo.Database, value mq.Message, queue string, payload []byte) (bool, error) {
	digest := sha256.Sum256(payload)
	id := queue + ":" + hex.EncodeToString(digest[:])
	collection := db.Collection("stable_message_quarantine", options.Collection().SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true))))
	_, err := collection.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$setOnInsert": bson.M{"queue": queue, "reason": "legacy_formal_write", "message": payload, "detected_at": time.Now().UTC()}}, options.Update().SetUpsert(true))
	if err != nil {
		return false, err
	}
	log.ZWarn(ctx, "稳定实例已隔离旧正式消息", ErrLegacy, "queue", queue, "quarantine_id", id)
	value.Mark()
	value.Commit()
	return true, nil
}
