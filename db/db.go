package db

const (
	ErrNotExisted     string = "Data Not Existed"
	ErrModeNotMatched string = "Storage Mode Not Matched"

	TypeMDB int = iota
	TypeLDB
)

type DB interface {
	Set(key string, data string) error
	Get(key string) (string, error)
	Del(key string) error
	Type() int
	Close()
}
