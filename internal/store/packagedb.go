package store

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/operator-framework/operator-registry/pkg/api"
	"github.com/perdasilva/olmcli/internal/repository"
	"github.com/sirupsen/logrus"
)

const (
	imageRegexp        = `^(?P<repository>[\w.\-_]+((?::\d+|)([a-z0-9._-]+/[a-z0-9._-]+))|)(?:/|)(?P<image>[a-z0-9.\-_]+(?:/[a-z0-9.\-_]+|))(:(?P<tag>[\w.\-_]{1,127})|)$`
	repositoriesBucket = "repositories"
	bundlesBucket      = "bundles"
	packagesBucket     = "packages"
	keySeparator       = "/"
)

type CachedRepository struct {
	RepositoryName   string `json:"name"`
	RepositorySource string `json:"source"`
}

func (c CachedRepository) ID() string {
	return c.RepositoryName
}

type CachedBundle struct {
	*api.Bundle
	BundleID           string `json:"id"`
	Repository         string `json:"repository"`
	DefaultChannelName string `json:"defaultChannelName"`
}

func (c CachedBundle) ID() string {
	return c.BundleID
}

type CachedPackage struct {
	*api.Package
	PackageID  string `json:"id"`
	Repository string `json:"repository"`
}

func (c CachedPackage) ID() string {
	return c.PackageID
}

type PackageDatabase interface {
	HasRepository(ctx context.Context, repo string) (bool, error)
	ListRepositories(ctx context.Context) ([]CachedRepository, error)
	ListPackages(ctx context.Context) ([]CachedPackage, error)
	ListBundles(ctx context.Context) ([]CachedBundle, error)
	SearchPackages(ctx context.Context, searchTerm string) ([]CachedPackage, error)
	SearchBundles(ctx context.Context, searchTerm string) ([]CachedBundle, error)
	CacheRepository(ctx context.Context, repository repository.Repository) error
	RemoveRepository(ctx context.Context, repoName string) error
	GetPackage(ctx context.Context, packageID string) (*CachedPackage, error)
	GetBundle(ctx context.Context, bundleID string) (*CachedBundle, error)
	IterateBundles(ctx context.Context, fn func(bundle *CachedBundle) error) error
	Close() error
}

var _ PackageDatabase = &boltPackageDatabase{}

type boltPackageDatabase struct {
	databasePath    string
	database        *bolt.DB
	repositoryTable *BoltDBTable[CachedRepository]
	packageTable    *BoltDBTable[CachedPackage]
	bundleTable     *BoltDBTable[CachedBundle]
	logger          *logrus.Logger
}

func NewPackageDatabase(databasePath string, logger *logrus.Logger) (PackageDatabase, error) {
	if logger == nil {
		panic("logger is nil")
	}

	db, err := bolt.Open(databasePath, 0600, nil)
	if err != nil {
		return nil, err
	}

	repositoryTable, err := createTableIgnoreExists[CachedRepository](db, repositoriesBucket)
	if err != nil {
		return nil, err
	}

	packageTable, err := createTableIgnoreExists[CachedPackage](db, packagesBucket)
	if err != nil {
		return nil, err
	}

	bundleTable, err := createTableIgnoreExists[CachedBundle](db, bundlesBucket)
	if err != nil {
		return nil, err
	}

	return &boltPackageDatabase{
		databasePath:    databasePath,
		database:        db,
		repositoryTable: repositoryTable,
		packageTable:    packageTable,
		bundleTable:     bundleTable,
		logger:          logger,
	}, nil
}

func (b *boltPackageDatabase) HasRepository(_ context.Context, repoName string) (bool, error) {
	return b.repositoryTable.Has(repoName)
}

func (b *boltPackageDatabase) ListRepositories(_ context.Context) ([]CachedRepository, error) {
	return b.repositoryTable.List()
}

func (b *boltPackageDatabase) RemoveRepository(_ context.Context, repoName string) error {
	return b.database.Update(func(tx *bolt.Tx) error {
		if err := b.repositoryTable.DeleteEntryWithKeyInTransaction(tx, repoName); err != nil {
			return err
		}
		prefix := repoName + keySeparator
		if err := b.packageTable.DeleteEntriesWithPrefixInTransaction(tx, prefix); err != nil {
			return err
		}

		return b.bundleTable.DeleteEntriesWithPrefixInTransaction(tx, prefix)
	})
}

func (b *boltPackageDatabase) CacheRepository(ctx context.Context, repository repository.Repository) error {
	if repository == nil {
		panic("repository is nil")
	}

	b.logger.Debugln("Caching repository from ", repository.Source())
	err := b.database.Update(func(tx *bolt.Tx) error {
		// extract repo name (in this case the name of the image)
		repoName := getRepoName(repository.Source())

		// iterate over bundles and write them out to the database inc. their packages
		bundleIterator, err := repository.ListBundles(ctx)
		defaultChannelNameMap := map[string]string{}

		if err != nil {
			return err
		}

		b.logger.Debugln("Inserting bundles...")
		for bundle := bundleIterator.Next(); bundle != nil; bundle = bundleIterator.Next() {
			pkgName := bundle.PackageName
			if _, ok := defaultChannelNameMap[pkgName]; !ok {
				pkg, err := repository.GetPackage(ctx, pkgName)
				if err != nil {
					return err
				}
				cachedPackage := &CachedPackage{
					PackageID:  GetPackageKey(repoName, pkg.GetName()),
					Package:    pkg,
					Repository: repoName,
				}
				if err := b.packageTable.InsertInTransaction(tx, cachedPackage); err != nil {
					return err
				}
				defaultChannelNameMap[pkgName] = pkg.DefaultChannelName
			}
			cachedBundle := &CachedBundle{
				BundleID:           GetBundleKey(repoName, bundle),
				Bundle:             bundle,
				Repository:         repoName,
				DefaultChannelName: defaultChannelNameMap[bundle.PackageName],
			}
			if err := b.bundleTable.InsertInTransaction(tx, cachedBundle); err != nil {
				return nil
			}
		}

		// add repo record
		b.logger.Debugln("Adding repository record...")
		return b.repositoryTable.InsertInTransaction(tx, &CachedRepository{
			RepositoryName:   repoName,
			RepositorySource: repository.Source(),
		})
	})
	b.logger.Debugln("Done...")
	return err
}

func (b *boltPackageDatabase) ListPackages(_ context.Context) ([]CachedPackage, error) {
	return b.packageTable.List()
}

func (b *boltPackageDatabase) ListBundles(_ context.Context) ([]CachedBundle, error) {
	return b.bundleTable.List()
}

func (b *boltPackageDatabase) IterateBundles(_ context.Context, fn func(bundle *CachedBundle) error) error {
	return b.bundleTable.Iterate(fn)
}

func (b *boltPackageDatabase) SearchPackages(_ context.Context, searchTerm string) ([]CachedPackage, error) {
	return b.packageTable.Search(func(pkg *CachedPackage) (bool, error) {
		return strings.Index(pkg.GetName(), searchTerm) >= 0, nil
	})
}

func (b *boltPackageDatabase) SearchBundles(_ context.Context, searchTerm string) ([]CachedBundle, error) {
	return b.bundleTable.Search(func(bundle *CachedBundle) (bool, error) {
		return strings.Index(bundle.CsvName, searchTerm) >= 0, nil
	})
}

func (b *boltPackageDatabase) GetPackage(_ context.Context, packageID string) (*CachedPackage, error) {
	return b.packageTable.Get(packageID)
}

func (b *boltPackageDatabase) GetBundle(_ context.Context, bundleID string) (*CachedBundle, error) {
	return b.bundleTable.Get(bundleID)
}

func (b *boltPackageDatabase) Close() error {
	if b.database != nil {
		return b.database.Close()
	}
	return nil
}

func GetBundleKey(repoName string, bundle *api.Bundle) string {
	return strings.Join([]string{repoName, bundle.PackageName, bundle.ChannelName, bundle.CsvName}, keySeparator)
}

func GetPackageKey(repoName, pkg string) string {
	return strings.Join([]string{repoName, pkg}, keySeparator)
}

func getRepoName(repoSource string) string {
	regex := regexp.MustCompile(imageRegexp)
	match := regex.FindStringSubmatch(repoSource)
	imageIndex := regex.SubexpIndex("image")
	return match[imageIndex]
}

func createTableIgnoreExists[E IdentifiableEntry](database *bolt.DB, tableName string) (*BoltDBTable[E], error) {
	table, err := NewBoltDBTable[E](database, tableName)
	if err != nil {
		return nil, err
	}
	if err := table.Create(); err != nil && !errors.Is(err, bolt.ErrBucketExists) {
		return nil, err
	}
	return table, nil
}
