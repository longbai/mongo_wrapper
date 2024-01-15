package mongo

import (
    "context"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo/options"
)

const CounterCollection = "_m_counters"

func (d *DbClient) NextSeqNo(ctx context.Context, sequenceName string) (uint32, error) {
    counterCollection := d.db.Collection(CounterCollection)
    filter := bson.M{"_id": sequenceName}
    update := bson.M{"$inc": bson.M{"seq": 1}}
    options := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)

    var updatedDoc struct {
        Seq uint32 `bson:"seq"`
    }

    err := counterCollection.FindOneAndUpdate(ctx, filter, update, options).Decode(&updatedDoc)
    if err != nil {
        return 0, err
    }

    return updatedDoc.Seq, nil
}
