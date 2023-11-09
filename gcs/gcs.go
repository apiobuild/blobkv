package gcs

import (
	"context"
	"encoding/json"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/apiobuild/blobkv/config"
	"github.com/apiobuild/blobkv/model"
)

// Custom Errors
var (
	ObjectDoesNotExistsError = echo.NewHTTPError(http.StatusNotFound, "Object Does Not Exists")
)

func getLogJSON(message string, err error) (json log.JSON) {
	json = log.JSON{
		"message": message,
	}
	if err != nil {
		json["error"] = err.Error()
	}
	return
}

// I ...
type I struct {
	Client stiface.Client
	Config config.Storage
	Bucket stiface.BucketHandle
}

// NewClient ...
func NewClient(conf config.Storage) (i I, err error) {
	var (
		client *storage.Client
	)

	ctx := context.Background()
	if client, err = storage.NewClient(
		ctx, option.WithCredentialsFile(conf.Creds),
	); err != nil {
		return
	}

	i = I{
		Client: stiface.AdaptClient(client),
		Config: conf,
	}
	// NOTE: need to instantiate client first
	i.Bucket = i.Client.Bucket(conf.Bucket)
	return
}

func (i I) getObject(key string) (object stiface.ObjectHandle) {
	return i.Bucket.Object(path.Join(i.Config.Prefix, key))
}

// Exists ...
func (i I) Exists(key string) (err error) {
	ctx := context.Background()

	if _, err = i.getObject(key).Attrs(ctx); err != nil {
		if err.Error() == "storage: object doesn't exist" {
			log.Warnj(
				getLogJSON("Object doesn't exist", err),
			)
			err = ObjectDoesNotExistsError
			return
		}
	}
	return
}

// Add add deploy stack metadata if not exist
func (i I) Add(key string, payload model.Payload, metadata *model.Metadata) (err error) {
	return i.add(key, payload, metadata, false)
}

func (i I) add(key string, payload model.Payload, metadata *model.Metadata, force bool) (err error) {
	ctx := context.Background()
	if !force {
		// if not force add, check if exist first
		if existsErr := i.Exists(key); existsErr == nil {
			log.Warnj(getLogJSON("Object exists, do noting", nil))
			return
		}
	}

	// otherwise write whether exist or not
	w := i.getObject(key).NewWriter(ctx)
	if metadata != nil {
		w.Attrs().Metadata = *metadata
	}
	w.ObjectAttrs().ContentType = "application/json"
	w.ObjectAttrs().CacheControl = "no-cache,max-age=0"
	defer w.Close()
	var b []byte
	if b, err = json.Marshal(payload); err != nil {
		return
	}
	if _, err = w.Write(b); err != nil {
		log.Warnj(log.JSON{
			"Message": "Error writting deployment stack",
			"Error":   err.Error(),
		})
		return
	}
	return
}

// ForceAdd add deploy stack metadata exist or not
func (i I) ForceAdd(key string, payload model.Payload, metadata *model.Metadata) (err error) {
	return i.add(key, payload, metadata, true)
}

// List ...
func (i I) List(key string) (keys model.Keys, err error) {
	ctx := context.Background()
	it := i.Bucket.Objects(ctx, &storage.Query{
		Prefix:    path.Join(i.Config.Prefix, key) + "/",
		Delimiter: "/",
	})

	for {
		attrs, err1 := it.Next()
		if err1 == iterator.Done {
			break
		}
		if err1 != nil {
			err = err1
			return
		}

		prefix := attrs.Prefix

		prefix = strings.TrimPrefix(prefix, i.Config.Prefix)
		prefix = strings.TrimPrefix(prefix, "/"+key)
		prefix = strings.Trim(prefix, "/")

		keys = append(keys, model.Key{
			Key:      prefix,
			Metadata: attrs.Metadata,
		})
	}
	return
}

// Get ...
func (i I) Get(key string) (payload model.Payload, err error) {
	if err = i.Exists(key); err != nil {
		log.Errorj(getLogJSON("Object doesn't exists", err))
		return
	}
	ctx := context.Background()
	r, err := i.getObject(key).NewReader(ctx)
	if err != nil {
		log.Errorj(getLogJSON("Error getting object", err))
		return
	}
	defer r.Close()
	err = json.NewDecoder(r).Decode(&payload)
	if err != nil {
		log.Errorj(getLogJSON("Error decoding object", err))
		return
	}
	return
}

// Update ...
func (i I) Update(key string, payload model.Payload, metadata *model.Metadata) (err error) {
	if err = i.Exists(key); err != nil {
		log.Errorj(getLogJSON("Object doesn't exists", err))
		return
	}
	return i.add(key, payload, metadata, true)
}

// Delete ...
func (i I) Delete(key string) (err error) {
	ctx := context.Background()

	if err = i.Exists(key); err != nil {
		log.Errorj(getLogJSON("Object doesn't exists", err))
		return
	}

	err = i.getObject(key).Delete(ctx)
	if err != nil {
		return
	}
	return
}
