package model

type Backend string

const (
	GCS Backend = "gcs"
)

type Metadata map[string]string

type Payload map[string]interface{}

type Key struct {
	Key      string   `json:"key"`
	Metadata Metadata `json:"metadata,omitempty"`
}

type Keys []Key

// I is generic blob kv access interface
type I interface {
	List(key string) (keys Keys, err error)
	Get(key string) (value Payload, err error)
	Exists(key string) (err error)
	Add(key string, value Payload, metadata *Metadata) (err error)
	ForceAdd(key string, value Payload, metadata *Metadata) (err error)
	Update(key string, value Payload, metadata *Metadata) (err error)
	Delete(key string) (err error)
}
