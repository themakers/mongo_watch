package mongo_watch

type Filter func() (db, coll string, filter FilterFunc)

type FilterFunc func(op OpType, v interface{}, change Change)
