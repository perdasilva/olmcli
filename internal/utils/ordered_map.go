package utils

import "github.com/wk8/go-ordered-map"

type Pair[K, V any] struct {
	*orderedmap.Pair
	Key   K
	Value V
}

type OrderedMap[K, V any] struct {
	inner *orderedmap.OrderedMap
}

func NewOrderedMap[K any, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		inner: orderedmap.New(),
	}
}

func (m *OrderedMap[K, V]) Get(key K) (V, bool) {
	v, ok := m.inner.Get(key)
	return v.(V), ok
}

func (m *OrderedMap[K, V]) Set(key K, value V) (V, bool) {
	out, ok := m.inner.Set(key, value)
	return out.(V), ok
}
func (m *OrderedMap[K, V]) Delete(key K) (V, bool) {
	out, ok := m.inner.Delete(key)
	return out.(V), ok
}

func (m *OrderedMap[K, V]) Len() int {
	return m.inner.Len()
}

func (m *OrderedMap[K, V]) Newest() *Pair[K, V] {
	p := m.inner.Newest()
	if p != nil {
		return &Pair[K, V]{
			Pair:  p,
			Key:   p.Key.(K),
			Value: p.Value.(V),
		}
	} else {
		return nil
	}
}

func (m *OrderedMap[K, V]) Oldest() *Pair[K, V] {
	p := m.inner.Oldest()
	if p != nil {
		return &Pair[K, V]{
			Pair:  p,
			Key:   p.Key.(K),
			Value: p.Value.(V),
		}
	} else {
		return nil
	}
}

func (p *Pair[K, V]) Next() *Pair[K, V] {
	n := p.Pair.Next()
	if n != nil {
		return &Pair[K, V]{
			Pair:  n,
			Key:   n.Key.(K),
			Value: n.Value.(V),
		}
	} else {
		return nil
	}
}

func (p *Pair[K, V]) Prev() *Pair[K, V] {
	n := p.Pair.Prev()
	if n != nil {
		return &Pair[K, V]{
			Pair:  n,
			Key:   n.Key.(K),
			Value: n.Value.(V),
		}
	} else {
		return nil
	}
}
