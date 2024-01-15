package mongo

import (
    "context"
    "testing"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
)

func TestObjId(t *testing.T) {
    t0 := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
    p := primitive.NewObjectIDFromTimestamp(t0)
    t.Log(p.Hex())
    t.Log(p.Timestamp())
}

var (
    addr   = "mongodb://localhost:27017"
    dbName = "test_db"
)

func TestNewDbClient(t *testing.T) {
    client, err := NewDbClient(addr, dbName)
    if err != nil {
        t.Fatalf("Failed to create new DB client: %v", err)
    }
    defer func() {
        _ = client.Close(context.Background())
    }()

    if client.db.Name() != dbName {
        t.Errorf("Expected database name %s, but got %s", dbName, client.db.Name())
    }
}

func TestDbClient_Close(t *testing.T) {
    client, err := NewDbClient(addr, dbName)
    if err != nil {
        t.Fatalf("Failed to create new DB client: %v", err)
    }

    if err := client.Close(context.Background()); err != nil {
        t.Errorf("Failed to close DB client: %v", err)
    }
}

func TestDbClient_Drop(t *testing.T) {
    client, err := NewDbClient(addr, dbName)
    if err != nil {
        t.Fatalf("Failed to create new DB client: %v", err)
    }
    defer func() {
        _ = client.Close(context.Background())
    }()

    if err := client.Drop(context.Background()); err != nil {
        t.Errorf("Failed to drop database: %v", err)
    }
}

func TestDbClient_CreateCollection(t *testing.T) {
    client, err := NewDbClient(addr, dbName)
    if err != nil {
        t.Fatalf("Failed to create new DB client: %v", err)
    }
    defer func() {
        _ = client.Close(context.Background())
    }()

    colName := "test_col"
    if err := client.CreateCollection(context.Background(), colName); err != nil {
        t.Errorf("Failed to create collection: %v", err)
    }
}

func TestDbClient_Collection(t *testing.T) {
    client, err := NewDbClient(addr, dbName)
    if err != nil {
        t.Fatalf("Failed to create new DB client: %v", err)
    }
    defer func() {
        _ = client.Close(context.Background())
    }()

    colName := "test_col"
    colClient := client.Collection(colName)
    if colClient.col.Name() != colName {
        t.Errorf("Expected collection name %s, but got %s", colName, colClient.col.Name())
    }
}

func TestDbClient_ServerVersion(t *testing.T) {
    client, err := NewDbClient(addr, dbName)
    if err != nil {
        t.Fatalf("Failed to create new DB client: %v", err)
    }
    defer func() {
        _ = client.Close(context.Background())
    }()

    version := client.ServerVersion()
    if version == "" {
        t.Errorf("Failed to get server version")
    }
}

func TestNewCollection(t *testing.T) {
    colName := "test_col"
    colClient, err := NewCollection(addr, dbName, colName)
    if err != nil {
        t.Fatalf("Failed to create new collection: %v", err)
    }
    defer func() {
        _ = colClient.Close(context.Background())
    }()

    if colClient.col.Name() != colName {
        t.Errorf("Expected collection name %s, but got %s", colName, colClient.col.Name())
    }
}

func TestColClient_CreateIndex(t *testing.T) {
    colName := "test_col"
    colClient, err := NewCollection(addr, dbName, colName)
    if err != nil {
        t.Fatalf("Failed to create new collection: %v", err)
    }
    defer func() {
        _ = colClient.Close(context.Background())
    }()

    index := Index{
        Keys:   []string{"test_key"},
        Unique: true,
    }
    if err := colClient.CreateIndex(context.Background(), index); err != nil {
        t.Errorf("Failed to create index: %v", err)
    }
}

func TestColClient_Basic(t *testing.T) {
    colName := "test_col"
    colClient, err := NewCollection(addr, dbName, colName)
    if err != nil {
        t.Fatalf("Failed to create new collection: %v", err)
    }

    defer func() {
        err = colClient.Drop(context.Background())
        if err != nil {
            t.Errorf("Failed to drop collection: %v", err)
        }
        err = colClient.Close(context.Background())
        if err != nil {
            t.Errorf("Failed to close collection: %v", err)
        }
    }()
    err = colClient.CreateIndex(context.Background(), Index{
        Keys:   []string{"key"},
        Unique: true,
    })
    if err != nil {
        t.Fatalf("Failed to create index: %v", err)
    }
    var docs []bson.M
    err = colClient.FindAll(context.Background(), bson.M{}, &docs)
    if err != nil {
        t.Errorf("Failed to find all documents: %v", err)
    }
    if len(docs) != 0 {
        t.Errorf("Expected 0 documents, but got %d", len(docs))
    }

    doc := bson.M{"key": "test_key", "value": "test_value"}
    _, err = colClient.InsertOne(context.Background(), doc)
    if err != nil {
        t.Errorf("Failed to insert one document: %v", err)
    }
    _, err = colClient.InsertOne(context.Background(), doc)
    if err == nil {
        t.Errorf("Expected error when inserting duplicate document")
    }
    _, err = colClient.InsertOne(context.Background(), bson.M{"key": "test_key2"})
    if err != nil {
        t.Errorf("Failed to insert one document: %v", err)
    }
    _, err = colClient.InsertOne(context.Background(), bson.M{"key": "test_key2"})
    if err == nil {
        t.Errorf("Expected error when inserting duplicate document")
    }

    err = colClient.FindAll(context.Background(), bson.M{}, &docs)
    if err != nil {
        t.Errorf("Failed to find all documents: %v", err)
    }
    if len(docs) != 2 {
        t.Errorf("Expected 2 documents, but got %d", len(docs))
    }

    err = colClient.DeleteOne(context.Background(), bson.M{"key": "test_key"})
    if err != nil {
        t.Errorf("Failed to delete one document: %v", err)
    }
    err = colClient.DeleteOne(context.Background(), bson.M{"key": "test_key"})
    if err != nil {
        t.Errorf("Expected no error when deleting non-existent document %v", err)
    }
    err = colClient.FindAll(context.Background(), bson.M{}, &docs)
    if err != nil {
        t.Errorf("Failed to find all documents: %v", err)
    }
    if len(docs) != 1 {
        t.Errorf("Expected 1 document, but got %d", len(docs))
    }
    err = colClient.DeleteOne(context.Background(), bson.M{"key": "test_key2"})
    if err != nil {
        t.Errorf("Failed to delete one document: %v", err)
    }
    err = colClient.FindAll(context.Background(), bson.M{}, &docs)
    if err != nil {
        t.Errorf("Failed to find all documents: %v", err)
    }
    if len(docs) != 0 {
        t.Errorf("Expected 0 documents, but got %d", len(docs))
    }
}

func TestColClient_UpdateOne(t *testing.T) {
    colName := "test_col"
    colClient, err := NewCollection(addr, dbName, colName)
    if err != nil {
        t.Fatalf("Failed to create new collection: %v", err)
    }
    defer func() {
        _ = colClient.Drop(context.Background())
        _ = colClient.Close(context.Background())
    }()
    err = colClient.CreateIndex(context.Background(), Index{
        Keys:   []string{"key"},
        Unique: true,
    })
    if err != nil {
        t.Fatalf("Failed to create index: %v", err)
    }

    doc := bson.M{"key": "test_key", "value": "test_value"}
    err = colClient.UpdateOne(context.Background(), bson.M{"key": "test_key"}, doc)
    if err == nil {
        //t.Errorf("Expected error when updating non-existent document")
    }
    var d bson.M
    err = colClient.FindOne(context.Background(), bson.M{"key": "test_key"}, &d)
    if err == nil {
        t.Errorf("Expected error when finding non-existent document")
    }
    if d != nil {
        t.Errorf("Expected nil document, but got %v", d)
    }
    _, err = colClient.InsertOne(context.Background(), doc)
    if err != nil {
        t.Errorf("Failed to insert one document: %v", err)
    }
    err = colClient.UpdateOne(context.Background(), bson.M{"key": "test_key"}, doc)
    if err != nil {
        t.Errorf("Failed to update one document: %v", err)
    }
    doc["value"] = "test_value2"
    err = colClient.UpdateOne(context.Background(), bson.M{"key": "test_key"}, doc)
    if err != nil {
        t.Errorf("Failed to update one document: %v", err)
    }

    err = colClient.FindOne(context.Background(), bson.M{"key": "test_key"}, &d)
    if err != nil {
        t.Errorf("Failed to find one document: %v", err)
    }
    if d["value"] != "test_value2" {
        t.Errorf("Expected value to be test_value2, but got %v", d["value"])
    }
}

func TestColClient_FindOrInsert(t *testing.T) {
    colName := "test_col"
    colClient, err := NewCollection(addr, dbName, colName)
    if err != nil {
        t.Fatalf("Failed to create new collection: %v", err)
    }
    defer func() {
        _ = colClient.Drop(context.Background())
        _ = colClient.Close(context.Background())
    }()
    err = colClient.CreateIndex(context.Background(), Index{
        Keys:   []string{"key"},
        Unique: true,
    })
    if err != nil {
        t.Fatalf("Failed to create index: %v", err)
    }

    doc := bson.M{"key": "test_key", "value": "test_value"}
    var d bson.M

    err = colClient.FindOrInsert(context.Background(), bson.M{"key": "test_key"}, doc, &d)
    if err != nil {
        t.Errorf("Failed to find or insert document: %v", err)
    }
    if d["value"] != "test_value" || d["key"] != "test_key" {
        t.Errorf("Expected value to be test_value, but got %v", d["value"])
    }

    doc["value"] = "test_value2"
    err = colClient.FindOrInsert(context.Background(), bson.M{"key": "test_key"}, doc, &d)
    if err != nil {
        t.Errorf("Failed to find or insert document: %v", err)
    }
    if d["value"] != "test_value" || d["key"] != "test_key" {
        t.Errorf("Expected value to be test_value, but got %v", d["value"])
    }
}
