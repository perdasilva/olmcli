package resolution

import (
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
)

var _ v2.Entity = &OLMEntity{}

type OLMEntity struct {
	*store.CachedBundle
}

func (e OLMEntity) ID() v2.EntityID {
	return v2.EntityID(e.BundleID)
}

func (e OLMEntity) String() string {
	return e.BundleID
}
