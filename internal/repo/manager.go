package repo

import (
	"context"
	"fmt"
	"path"

	"github.com/perdasilva/olmcli/internal/resolution/constraints"
	"github.com/sirupsen/logrus"
)

// Manager manages OLM software repositories
type Manager interface {
	AddRepository(ctx context.Context, repo string) error
	ListRepositories(ctx context.Context) ([]CachedRepository, error)
	SearchBundles(ctx context.Context, searchTerm string) ([]CachedBundle, error)
	SearchPackages(ctx context.Context, searchTerm string) ([]CachedPackage, error)
	RemoveRepository(ctx context.Context, repoName string) error
	ListBundles(ctx context.Context) ([]CachedBundle, error)
	ListPackages(ctx context.Context) ([]CachedPackage, error)
	Close(ctx context.Context) error
	Install(ctx context.Context, packageName string) error
}

var _ Manager = &containerBasedManager{}

type containerBasedManager struct {
	logger          *logrus.Logger
	configPath      string
	packageDatabase PackageDatabase
	resolver        *OLMResolver
}

func NewManager(configPath string, logger *logrus.Logger) (Manager, error) {
	if logger == nil {
		panic("no logger specified")
	}

	packageDatabase, err := NewPackageDatabase(path.Join(configPath, "olm.db"), logger)
	if err != nil {
		return nil, err
	}

	resolver, err := NewOLMResolver(packageDatabase)
	if err != nil {
		return nil, err
	}

	return &containerBasedManager{
		configPath:      configPath,
		logger:          logger,
		packageDatabase: packageDatabase,
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

func (m *containerBasedManager) Close(_ context.Context) error {
	return m.packageDatabase.Close()
}

// AddRepository adds a new OLM software repository
func (m *containerBasedManager) AddRepository(ctx context.Context, repo string) error {
	repository := NewRegistry(repo, m.logger)
	if err := repository.Connect(); err != nil {
		return err
	}
	defer repository.Close()
	return m.packageDatabase.CacheRepository(ctx, repository)
}

func (m *containerBasedManager) ListRepositories(_ context.Context) ([]CachedRepository, error) {
	return m.packageDatabase.ListRepositories()
}

func (m *containerBasedManager) RemoveRepository(_ context.Context, repoName string) error {
	return m.packageDatabase.RemoveRepository(repoName)
}

func (m *containerBasedManager) ListPackages(_ context.Context) ([]CachedPackage, error) {
	return m.packageDatabase.ListPackages()
}

func (m *containerBasedManager) ListBundles(_ context.Context) ([]CachedBundle, error) {
	return m.packageDatabase.ListBundles()
}

func (m *containerBasedManager) SearchPackages(_ context.Context, searchTerm string) ([]CachedPackage, error) {
	return m.packageDatabase.SearchPackages(searchTerm)
}

func (m *containerBasedManager) SearchBundles(_ context.Context, searchTerm string) ([]CachedBundle, error) {
	return m.packageDatabase.SearchBundles(searchTerm)
}
