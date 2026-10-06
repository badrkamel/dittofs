package common

import (
	"context"

	"github.com/marmos91/dittofs/pkg/block/engine"
	"github.com/marmos91/dittofs/pkg/metadata"
)

// WithFilePayloadScope keeps a file's content operation and metadata update in
// one shared payload scope. The initial read locates the payload; the callback
// must validate its operation against fresh metadata using the supplied context.
func WithFilePayloadScope[T any](authCtx *metadata.AuthContext, metaSvc *metadata.Service, blockStore *engine.Store, handle metadata.FileHandle, fn func(*metadata.AuthContext) (T, error)) (T, error) {
	var result T
	file, err := metaSvc.GetFileForRead(authCtx.Context, handle)
	if err != nil {
		return result, err
	}
	entered := false
	err = blockStore.WithPayloadScope(authCtx.Context, []string{string(file.PayloadID)}, false, func(ctx context.Context) error {
		entered = true
		scoped := *authCtx
		scoped.Context = ctx
		var err error
		result, err = fn(&scoped)
		return err
	})
	if err != nil && !entered {
		return result, normalizeBlockStoreError(err)
	}
	return result, err
}
