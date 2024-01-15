package mongo

import (
    "context"
    "testing"
)

func TestGetNextSequence(t *testing.T) {
    db, err := NewDbClient("mongodb://localhost:27017", "test_db")
    if err != nil {
        return
    }
    db.counterCol = "test_counter"
    ctx := context.Background()
    defer func() {
        _ = db.Collection(db.counterCol).Drop(ctx)
        _ = db.Close(ctx)
    }()
    sequenceName := "test_seq"
    seq1, err := db.NextSeqNo(ctx, sequenceName)
    if err != nil {
        t.Fatalf("Failed to get next sequence: %v", err)
    }
    if seq1 != 1 {
        t.Errorf("Expected next sequence to be 1, but got %d", seq1)
    }
    seq2, err := db.NextSeqNo(ctx, sequenceName)
    if err != nil {
        t.Fatalf("Failed to get next sequence: %v", err)
    }

    if seq2 != seq1+1 {
        t.Errorf("Expected next sequence to be %d, but got %d", seq1+1, seq2)
    }

}
