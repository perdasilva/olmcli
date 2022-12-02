package repository

import (
	"context"

	"github.com/operator-framework/operator-registry/pkg/client"
)

type Repository interface {
	client.Interface
	Connect(ctx context.Context) error
	Close() error
	Source() string
}

type RepositoryContainer interface {
	Start() error
	Stop() error
	ImageURL() string
	RepositoryURL() string
}
