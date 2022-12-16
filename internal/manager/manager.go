package manager

import (
	"context"
	"path"

	"github.com/perdasilva/olmcli/internal/repository"
	"github.com/perdasilva/olmcli/internal/resolution"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/sirupsen/logrus"
)

// Manager manages OLM software repositories
type Manager interface {
	AddRepository(ctx context.Context, repositoryImageUrl string) error
	ListRepositories(ctx context.Context) ([]store.CachedRepository, error)
	ListGVKs(ctx context.Context) (map[string][]store.CachedBundle, error)
	ListBundlesForGVK(ctx context.Context, group string, version string, kind string) ([]store.CachedBundle, error)
	SearchBundles(ctx context.Context, searchTerm string) ([]store.CachedBundle, error)
	SearchPackages(ctx context.Context, searchTerm string) ([]store.CachedPackage, error)
	RemoveRepository(ctx context.Context, repoName string) error
	ListBundles(ctx context.Context) ([]store.CachedBundle, error)
	ListPackages(ctx context.Context) ([]store.CachedPackage, error)
	Install(ctx context.Context, packageName string) error
	Resolve(ctx context.Context, packageName string) ([]resolution.Installable, error)
	GetBundlesForPackage(ctx context.Context, packageName string, options ...store.PackageSearchOption) ([]store.CachedBundle, error)
	Close() error
}

var _ Manager = &containerBasedManager{}

type containerBasedManager struct {
	store.PackageDatabase
	logger     *logrus.Logger
	configPath string
	installer  *PackageInstaller
}

func NewManager(configPath string, logger *logrus.Logger) (Manager, error) {
	if logger == nil {
		panic("no logger specified")
	}

	packageDatabase, err := store.NewPackageDatabase(path.Join(configPath, "olm.db"), logger)
	if err != nil {
		return nil, err
	}

	installer, err := NewPackageInstaller(resolution.NewOLMSolver(packageDatabase, logger), logger)
	if err != nil {
		return nil, err
	}

	return &containerBasedManager{
		PackageDatabase: packageDatabase,
		configPath:      configPath,
		logger:          logger,
		installer:       installer,
	}, nil
}

func (m *containerBasedManager) Install(ctx context.Context, packageName string) error {
	packageRequired, err := resolution.NewRequiredPackage(packageName)
	if err != nil {
		return err
	}
	return m.installer.Install(ctx, packageRequired)
}

func (m *containerBasedManager) Resolve(ctx context.Context, packageName string) ([]resolution.Installable, error) {
	packageRequired, err := resolution.NewRequiredPackage(packageName)
	if err != nil {
		return nil, err
	}
	return m.installer.Resolve(ctx, packageRequired)
}

// AddRepository adds a new OLM software repository
func (m *containerBasedManager) AddRepository(ctx context.Context, repositoryImageUrl string) error {
	repo := repository.FromImageURL(repositoryImageUrl, m.logger)
	if err := repo.Connect(ctx); err != nil {
		return err
	}
	defer repo.Close()
	return m.CacheRepository(ctx, repo)
}
