package mongo

import (
    "context"
    "errors"
    "log"
    "strings"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readpref"
)

type DbClient struct {
    cli        *mongo.Client
    db         *mongo.Database
    counterCol string
}

type ColClient struct {
    col *mongo.Collection
    DbClient
}

// NewDbClient create a new mongo client, addr is the mongo server address, db is the database name
func NewDbClient(addr string, db string) (*DbClient, error) {
    ctx := context.Background()
    opt := options.Client()
    opt.SetConnectTimeout(300 * time.Second)
    cli, err := mongo.Connect(ctx, opt.ApplyURI(addr))
    if err != nil {
        return nil, err
    }
    err = cli.Ping(ctx, readpref.Primary())
    if err != nil {
        _ = cli.Disconnect(ctx)
        return nil, err
    }
    return &DbClient{cli: cli, db: cli.Database(db), counterCol: CounterCollection}, nil
}

func (d *DbClient) Close(ctx context.Context) error {
    return d.cli.Disconnect(ctx)
}

func (d *DbClient) Drop(ctx context.Context) error {
    return d.db.Drop(ctx)
}

func (d *DbClient) CreateCollection(ctx context.Context, col string) error {
    return d.db.CreateCollection(ctx, col)
}

func (d *DbClient) Collection(col string) *ColClient {
    return &ColClient{col: d.db.Collection(col), DbClient: *d}
}

// ServerVersion get the version of mongoDB server, like 4.4.0
func (d *DbClient) ServerVersion() string {
    var buildInfo bson.Raw
    err := d.cli.Database("admin").RunCommand(
        context.Background(),
        bson.D{{"buildInfo", 1}},
    ).Decode(&buildInfo)
    if err != nil {
        log.Println("run command err", err)
        return ""
    }
    v, err := buildInfo.LookupErr("version")
    if err != nil {
        log.Println("look up err", err)
        return ""
    }
    return v.StringValue()
}

func NewCollection(addr, db, col string) (*ColClient, error) {
    newDb, err := NewDbClient(addr, db)
    if err != nil {
        return nil, err
    }
    return newDb.Collection(col), nil
}

func (c *ColClient) Close(ctx context.Context) error {
    return c.cli.Disconnect(ctx)
}

// splitSortField handle sort symbol: "+"/"-" in front of field
// default sort is 1
// if "+"， return sort as 1
// if "-"， return sort as -1
func splitSortField(field string) (key string, sort int32) {
    sort = 1
    key = field

    if len(field) != 0 {
        switch field[0] {
        case '+':
            key = strings.TrimPrefix(field, "+")
            sort = 1
        case '-':
            key = strings.TrimPrefix(field, "-")
            sort = -1
        }
    }

    return key, sort
}

// Index is a struct for creating index
type Index struct {
    Keys   []string // index keys， like ["name", "-age"]， if the key is start with "-"， it means descending order
    Unique bool     // unique index
}

func (c *ColClient) CreateIndex(ctx context.Context, indexes ...Index) error {
    if len(indexes) == 0 {
        return errors.New("index is empty")
    }
    var indexModels = make([]mongo.IndexModel, 0, len(indexes))

    for _, idx := range indexes {
        var model mongo.IndexModel
        var keysDoc bson.D

        for _, field := range idx.Keys {
            k, sort := splitSortField(field)
            keysDoc = append(keysDoc, bson.E{Key: k, Value: sort})
        }

        opt := options.Index()
        opt.SetUnique(idx.Unique)
        model = mongo.IndexModel{
            Keys:    keysDoc,
            Options: opt,
        }

        indexModels = append(indexModels, model)
    }

    _, err := c.col.Indexes().CreateMany(ctx, indexModels)
    return err
}

func (c *ColClient) InsertOne(ctx context.Context, doc interface{}) (*mongo.InsertOneResult, error) {
    return c.col.InsertOne(ctx, doc)
}

func (c *ColClient) UpdateOne(ctx context.Context, filter interface{}, doc interface{}) error {
    _, err := c.col.UpdateOne(ctx, filter, bson.M{"$set": doc})
    return err
}

func (c *ColClient) FindOne(ctx context.Context, filter interface{}, doc interface{}) error {
    return c.col.FindOne(ctx, filter).Decode(doc)
}

func (c *ColClient) FindAll(ctx context.Context, filter interface{}, array interface{}) error {
    if filter == nil {
        filter = bson.M{}
    }
    cursor, err := c.col.Find(ctx, filter)
    if err != nil {
        return err
    }
    return cursor.All(ctx, array)
}

// FindOrInsert find one document by filter, if not found, insert doc and return the inserted document
func (c *ColClient) FindOrInsert(ctx context.Context, filter interface{}, doc interface{}, ret interface{}) error {
    opts := options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)
    update := bson.M{"$setOnInsert": doc}
    return c.col.FindOneAndUpdate(ctx, filter, update, opts).Decode(ret)
}

func (c *ColClient) DeleteOne(ctx context.Context, filter interface{}) error {
    _, err := c.col.DeleteOne(ctx, filter)
    return err
}
