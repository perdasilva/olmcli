package eventidprovider

import (
	"strconv"
	"sync/atomic"

	"github.com/perdasilva/olmcli/internal/pipeline"
)

var _ pipeline.EventIDProvider = &IncreasingEventIDProvider{}

var provider = &IncreasingEventIDProvider{
	id: 0,
}

func MonotonicallyIncreasingEventIDProvider() *IncreasingEventIDProvider {
	return provider
}

type IncreasingEventIDProvider struct {
	id int64
}

func (i *IncreasingEventIDProvider) NextEventID() pipeline.EventID {
	return pipeline.EventID(strconv.FormatInt(atomic.AddInt64(&i.id, 1), 10))
}
