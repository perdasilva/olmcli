package resolution

import (
	"context"

	"github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
)

var _ v2.EntitySource[*store.CachedBundle] = &OLMEntitySource{}

type OLMEntitySource struct {
	store.PackageDatabase
}

func (s OLMEntitySource) ID() v2.EntitySourceID {
	return "packageManager"
}

func (s OLMEntitySource) Get(ctx context.Context, id v2.EntityID) (*store.CachedBundle, error) {
	bundle, err := s.GetBundle(ctx, string(id))
	if err != nil {
		return nil, err
	}
	return bundle, nil
}

type IterableOLMEntitySource interface {
	v2.EntitySource[*store.CachedBundle]
	Iterate(ctx context.Context, fn func(entity *store.CachedBundle) error) error
}

var _ IterableOLMEntitySource = &iterableEntitySource{}

type OLMEntitySet map[v2.EntityID]store.CachedBundle

type iterableEntitySource struct {
	id        v2.EntitySourceID
	entitySet OLMEntitySet
}

func NewIterableEntitySource(id v2.EntitySourceID, entities OLMEntitySet) IterableOLMEntitySource {
	return &iterableEntitySource{
		id:        id,
		entitySet: entities,
	}
}

func (s *iterableEntitySource) ID() v2.EntitySourceID {
	return s.id
}

func (s *iterableEntitySource) Get(ctx context.Context, id v2.EntityID) (*store.CachedBundle, error) {
	if entity, ok := s.entitySet[id]; ok {
		return &entity, nil
	}
	return nil, nil
}

func (s *iterableEntitySource) Iterate(ctx context.Context, fn func(entity *store.CachedBundle) error) error {
	for _, entity := range s.entitySet {
		if err := fn(&entity); err != nil {
			return err
		}
	}
	return nil
}
