package mgo

import (
	"context"
	"errors"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// GetStableSeq 读取真实已提交水位，不重用原生 Mongo 预留号段。
func (s *seqConversationMongo) GetStableSeq(ctx context.Context, id string) (database.SeqTime, error) {
	var row struct {
		Seq  int64 `bson:"seq"`
		Time int64 `bson:"time"`
	}
	col := s.coll.Database().Collection("stable_message_watermarks", options.Collection().SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary()))
	err := col.FindOne(ctx, bson.M{"_id": id}).Decode(&row)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return database.SeqTime{}, nil
	}
	return database.SeqTime{Seq: row.Seq, Time: row.Time}, err
}
