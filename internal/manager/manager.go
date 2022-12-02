package manager

import (
	"context"
	"fmt"
	"path"

	"github.com/perdasilva/olmcli/internal/repository"
	"github.com/perdasilva/olmcli/internal/resolution/constraints"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/sirupsen/logrus"
)

// Manager manages OLM software repositories
type Manager interface {
	AddRepository(ctx context.Context, repositoryImageUrl string) error
	ListRepositories(ctx context.Context) ([]store.CachedRepository, error)
	SearchBundles(ctx context.Context, searchTerm string) ([]store.CachedBundle, error)
	SearchPackages(ctx context.Context, searchTerm string) ([]store.CachedPackage, error)
	RemoveRepository(ctx context.Context, repoName string) error
	ListBundles(ctx context.Context) ([]store.CachedBundle, error)
	ListPackages(ctx context.Context) ([]store.CachedPackage, error)
	Install(ctx context.Context, packageName string) error
	Close() error
}

var _ Manager = &containerBasedManager{}

type containerBasedManager struct {
	store.PackageDatabase
	logger     *logrus.Logger
	configPath string
	resolver   *OLMResolver
}

func NewManager(configPath string, logger *logrus.Logger) (Manager, error) {
	if logger == nil {
		panic("no logger specified")
	}

	packageDatabase, err := store.NewPackageDatabase(path.Join(configPath, "olm.db"), logger)
	if err != nil {
		return nil, err
	}

	resolver, err := NewOLMResolver(packageDatabase)
	if err != nil {
		return nil, err
	}

	return &containerBasedManager{
		PackageDatabase: packageDatabase,
		configPath:      configPath,
		logger:          logger,
		resolver:        resolver,
	}, nil
}

func (m *containerBasedManager) Install(ctx context.Context, packageName string) error {
	requirePackage := constraints.RequirePackage{
		PackageName:  packageName,
		VersionRange: ">= 0.0.0",
	}
	solution, err := m.resolver.SolveFor(ctx, requirePackage)
	if err != nil {
		return err
	}
	for entityID, selected := range solution {
		if selected {
			fmt.Println(entityID)
		}
	}
	return nil
}

// AddRepository adds a new OLM software repository
func (m *containerBasedManager) AddRepository(ctx context.Context, repositoryImageUrl string) error {
	repository := repository.FromImageURL(repositoryImageUrl, m.logger)
	if err := repository.Connect(ctx); err != nil {
		return err
	}
	defer repository.Close()
	return m.CacheRepository(ctx, repository)
}
