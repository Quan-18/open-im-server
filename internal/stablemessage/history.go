package stablemessage

import (
	"context"
	"errors"

	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Conversations 从持久正文参与关系发现正式会话，不依赖一次元数据投影。
func (s *Store) Conversations(ctx context.Context, user string) ([]string, error) {
	values, err := s.DB.Collection(ReceiptCollection).Distinct(ctx, "descriptor.conversation_id", bson.M{"status": Committed, "$or": bson.A{bson.M{"descriptor.sender_id": user}, bson.M{"descriptor.receiver_id": user}}})
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if id, ok := value.(string); ok {
			result = append(result, id)
		}
	}
	return result, nil
}

// CheckParticipant 核对技术参与者，不把知道会话 ID 当作读权。
func (s *Store) CheckParticipant(ctx context.Context, conversation, user string) error {
	err := s.DB.Collection(ReceiptCollection).FindOne(ctx, bson.M{"descriptor.conversation_id": conversation, "status": Committed, "$or": bson.A{bson.M{"descriptor.sender_id": user}, bson.M{"descriptor.receiver_id": user}}}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return ErrUnauthorized
	}
	return err
}

// ReadRange 在投影尚未生成会话列表时仍按正式水位读取。
func (s *Store) ReadRange(ctx context.Context, conversation, user string, begin, end, num int64, ascending bool) ([]*model.MsgDataModel, bool, error) {
	if err := s.CheckParticipant(ctx, conversation, user); err != nil {
		return nil, false, err
	}
	max, err := s.ReadCommittedWater(ctx, conversation)
	if err != nil {
		return nil, false, err
	}
	if num <= 0 || num > 1000 || begin < 0 || end < 0 {
		return nil, false, ErrInvalid
	}
	if begin < 1 {
		begin = 1
	}
	if end == 0 || end > max {
		end = max
	}
	if begin > end {
		return []*model.MsgDataModel{}, true, nil
	}
	if end-begin+1 > num {
		if ascending {
			end = begin + num - 1
		} else {
			begin = end - num + 1
		}
	}
	var rows []*model.MsgDataModel
	for seq := begin; seq <= end; seq++ {
		m, e := s.Message(ctx, conversation, seq)
		if e != nil {
			return nil, false, e
		}
		rows = append(rows, m)
	}
	if !ascending {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}
	return rows, (ascending && end >= max) || (!ascending && begin <= 1), nil
}
