package mongo_watch

import "fmt"

type NoDecodersForFilterError struct {
	DB   string
	Coll string
}

func (e *NoDecodersForFilterError) Error() string {
	return fmt.Sprintf("NoDecodersForFilterError: %s.%s", e.DB, e.Coll)
}

func NewNoDecodersForFilterError(db, coll string) error {
	return &NoDecodersForFilterError{
		DB:   db,
		Coll: coll,
	}
}

func IsNoDecodersForFilterError(err error) (*NoDecodersForFilterError, bool) {
	if err, ok := err.(*NoDecodersForFilterError); ok {
		return err, true
	} else {
		return err, false
	}
}
