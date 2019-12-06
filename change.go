package mongo_watch

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OpType string

const (
	OpInsert  OpType = "insert"
	OpDelete  OpType = "delete"
	OpReplace OpType = "replace"
	OpUpdate  OpType = "update"

	//insert
	//delete
	//replace
	//update
	//drop
	//rename
	//dropDatabase
	//invalidate
)

// https://docs.mongodb.com/manual/reference/change-events/
type Change struct {
	ID struct {
		Data string `bson:"_data"`
	} `bson:"_id"`

	OperationType OpType `bson:"operationType"`

	FullDocument bson.Raw `bson:"fullDocument"`

	NS struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`

	To *struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"to"`

	DocumentKey bson.Raw `bson:"documentKey"`

	UpdateDescription *struct {
		UpdatedFields bson.Raw `bson:"updatedFields"`
		RemovedFields []string `bson:"removedFields"`
	} `bson:"updateDescription"`

	ClusterTime primitive.Timestamp `bson:"clusterTime"`

	TxnNumber int64 `bson:"txnNumber"`

	LSID bson.Raw `bson:"lsid"`

	//LSID *struct {
	//	ID  primitive.ObjectID `bson:"id"`
	//	UID primitive.Binary   `bson:"uid"`
	//} `bson:"lsid"`
}

func (ch *Change) DocumentKeyID() interface{} {
	var dk *struct {
		ID interface{} `bson:"_id"`
	}

	if err := bson.Unmarshal(ch.DocumentKey, &dk); err != nil {
		panic(err)
	}

	if dk != nil {
		return dk.ID
	} else {
		return nil
	}
}

func (ch *Change) path() string {
	return fmtPath(ch.NS.DB, ch.NS.Coll)
}
