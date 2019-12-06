package mongo_watch

import (
	"context"
	"fmt"
	"github.com/rs/xid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type collFilters struct {
	lock    sync.RWMutex
	filters map[string]FilterFunc
}

type Watch struct {
	client *mongo.Client

	decodersMap map[string]map[string]DecodeFunc
	decoders    map[string]DecodeFunc

	filters map[string]*collFilters

	errors chan error
}

func New(ctx context.Context, client *mongo.Client, ops ...Option) *Watch {
	wa := &Watch{
		client:      client,
		decodersMap: map[string]map[string]DecodeFunc{},
		decoders:    map[string]DecodeFunc{},
		filters:     map[string]*collFilters{},
		errors:      make(chan error),
	}

	for _, ops := range ops {
		ops(wa)
	}

	for db, colls := range wa.decodersMap {
		for coll, decode := range colls {
			path := fmtPath(db, coll)
			wa.decoders[path] = decode
			wa.filters[path] = &collFilters{
				filters: map[string]FilterFunc{},
			}
		}
	}

	go wa.worker(ctx)

	return wa
}

func (wa *Watch) pipeline() bson.M {
	or := bson.A{}

	for db, colls := range wa.decodersMap {
		for coll := range colls {
			or = append(or, bson.M{
				"ns.db":   db,
				"ns.coll": coll,
			})
		}
	}

	return bson.M{
		"$or": or,
	}
}

func (wa *Watch) worker(ctx context.Context) {
	var saToken string

	for {
		func() {
			defer time.Sleep(2 * time.Second)

			ops := new(options.ChangeStreamOptions).
				SetFullDocument(options.UpdateLookup)

			if saToken != "" {
				ops = ops.SetStartAfter(saToken)
			}

			cs, err := wa.client.Watch(ctx, wa.pipeline(), ops)
			if err != nil {
				wa.emitError(err)
				return
			}

			var change Change

			for cs.Next(ctx) {
				if err := cs.Decode(&change); err != nil {
					wa.emitError(err)
					return
				}

				saToken = change.ID.Data

				wa.handleChange(ctx, change)
			}

		}()
	}
}

func (wa *Watch) handleChange(ctx context.Context, change Change) {
	op := change.OperationType

	switch op {
	case OpInsert, OpReplace, OpDelete, OpUpdate:
		// OK
	default:
		return
	}

	path := change.path()

	cfs := wa.filters[path]

	if cfs == nil {
		return
	}

	var filters []FilterFunc

	cfs.lock.RLock()
	if len(cfs.filters) == 0 {
		cfs.lock.RUnlock()
		return
	} else {
		for _, filter := range cfs.filters {
			filters = append(filters, filter)
		}
		cfs.lock.RUnlock()
	}

	switch op {
	case OpDelete:
		for _, filter := range filters {
			filter(op, change.DocumentKeyID(), change)
		}
	default:
		decoder := wa.decoders[path]
		doc, err := decoder(change.FullDocument)
		if err != nil {
			panic(err)
		}
		for _, filter := range filters {
			filter(op, doc, change)
		}
	}
}

func (wa *Watch) Subscribe(f ...Filter) func() {
	subscribed := map[string][]string{}

	unsubscribe := func() {
		for path, fids := range subscribed {
			cf := wa.filters[path]
			cf.lock.Lock()
			for _, fid := range fids {
				delete(cf.filters, fid)
			}
			cf.lock.Unlock()
		}
	}

	for _, f := range f {
		db, coll, filter := f()
		path := fmtPath(db, coll)
		fid := xid.New().String()

		if cf, ok := wa.filters[path]; ok {
			subscribed[path] = append(subscribed[path], fid)
			cf.lock.Lock()
			cf.filters[fid] = filter
			cf.lock.Unlock()
		} else {
			unsubscribe()
			panic(NewNoDecodersForFilterError(db, coll))
		}
	}

	return unsubscribe
}

func (wa *Watch) emitError(err error) {
	go func() {
		wa.errors <- err
	}()
}

func (wa *Watch) Errors() <-chan error {
	return wa.errors
}

func fmtPath(db, coll string) string {
	return fmt.Sprintf("%s%c%s", db, rune(0), coll)
}
