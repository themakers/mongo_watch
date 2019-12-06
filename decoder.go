package mongo_watch

import "go.mongodb.org/mongo-driver/bson"

type Decoder func() (db, coll string, decode DecodeFunc)

type DecodeFunc func(doc bson.Raw) (interface{}, error)
