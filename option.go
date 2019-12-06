package mongo_watch

type Option func(wa *Watch)

func WithDecoder(decoder Decoder) Option {
	return func(wa *Watch) {
		db, coll, decode := decoder()
		if cd, ok := wa.decodersMap[db]; !ok {
			wa.decodersMap[db] = map[string]DecodeFunc{coll: decode}
		} else {
			cd[coll] = decode
		}
	}
}

func WithDecodeFunc(db, coll string, decoder DecodeFunc) Option {
	return WithDecoder(func() (string, string, DecodeFunc) {
		return db, coll, decoder
	})
}
