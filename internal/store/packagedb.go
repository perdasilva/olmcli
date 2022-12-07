package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"github.com/boltdb/bolt"
	v2 "github.com/operator-framework/deppy/pkg/v2"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/operator-framework/operator-registry/pkg/api"
	"github.com/perdasilva/olmcli/internal/repository"
	"github.com/sirupsen/logrus"
)

const (
	imageRegexp        = `^(?P<repository>[\w.\-_]+((?::\d+|)([a-z0-9._-]+/[a-z0-9._-]+))|)(?:/|)(?P<image>[a-z0-9.\-_]+(?:/[a-z0-9.\-_]+|))(:(?P<tag>[\w.\-_]{1,127})|)$`
	repositoriesBucket = "repositories"
	bundlesBucket      = "bundles"
	packagesBucket     = "packages"
	gvkBucket          = "gvks"
	keySeparator       = "/"
)

type packageSearchConfig struct {
	repositories []string
	channel      string
	versionRange semver.Range
}

func (p *packageSearchConfig) applyOptions(options ...PackageSearchOption) {
	for _, opt := range options {
		opt(p)
	}
}

func (p *packageSearchConfig) keep(bundle *CachedBundle) bool {
	if bundle == nil {
		return false
	}
	if len(p.repositories) > 0 {
		in := false
		for _, repo := range p.repositories {
			if repo == bundle.Repository {
				in = true
			}
		}
		if !in {
			return false
		}
	}
	if p.versionRange != nil {
		ver, err := semver.Parse(bundle.Version)
		if err != nil || !p.versionRange(ver) {
			return false
		}
	}
	if p.channel != "" {
		if p.channel != bundle.ChannelName {
			return false
		}
	}

	return true
}

type PackageSearchOption func(config *packageSearchConfig)

func InRepositories(repositories ...string) PackageSearchOption {
	return func(config *packageSearchConfig) {
		config.repositories = repositories
	}
}

func InVersionRange(versionRange semver.Range) PackageSearchOption {
	return func(config *packageSearchConfig) {
		config.versionRange = versionRange
	}
}

func InChannel(channel string) PackageSearchOption {
	return func(config *packageSearchConfig) {
		config.channel = channel
	}
}

type CachedRepository struct {
	RepositoryName   string `json:"name"`
	RepositorySource string `json:"source"`
}

func (c CachedRepository) EntryID() string {
	return c.RepositoryName
}

type CachedBundle struct {
	*api.Bundle
	BundleID            string             `json:"id"`
	Repository          string             `json:"repository"`
	DefaultChannelName  string             `json:"defaultChannelName"`
	PackageDependencies []property.Package `json:"packageDependencies"`
}

func (c CachedBundle) EntryID() string {
	return c.BundleID
}

func (c CachedBundle) ID() v2.EntityID {
	return v2.EntityID(c.BundleID)
}

type CachedPackage struct {
	*api.Package
	PackageID  string `json:"id"`
	Repository string `json:"repository"`
}

func (c CachedPackage) EntryID() string {
	return c.PackageID
}

type CachedGVKBundle struct {
	CachedBundle
	GVKID string `json:"gvkId"`
	GVK   string `json:"gvk"`
}

func (c CachedGVKBundle) EntryID() string {
	return c.GVKID
}

type PackageDatabase interface {
	HasRepository(ctx context.Context, repo string) (bool, error)
	ListRepositories(ctx context.Context) ([]CachedRepository, error)
	ListPackages(ctx context.Context) ([]CachedPackage, error)
	ListBundles(ctx context.Context) ([]CachedBundle, error)
	ListBundlesForGVK(ctx context.Context, group string, version string, kind string) ([]CachedBundle, error)
	ListGVKs(ctx context.Context) (map[string][]CachedBundle, error)
	SearchPackages(ctx context.Context, searchTerm string) ([]CachedPackage, error)
	SearchBundles(ctx context.Context, searchTerm string) ([]CachedBundle, error)
	CacheRepository(ctx context.Context, repository repository.Repository) error
	RemoveRepository(ctx context.Context, repoName string) error
	GetPackage(ctx context.Context, packageID string) (*CachedPackage, error)
	GetBundle(ctx context.Context, bundleID string) (*CachedBundle, error)
	IterateBundles(ctx context.Context, fn func(bundle *CachedBundle) error) error
	GetBundlesForPackage(ctx context.Context, packageName string, options ...PackageSearchOption) ([]CachedBundle, error)
	Close() error
}

var _ PackageDatabase = &boltPackageDatabase{}

type boltPackageDatabase struct {
	databasePath    string
	database        *bolt.DB
	repositoryTable *BoltDBTable[CachedRepository]
	packageTable    *BoltDBTable[CachedPackage]
	bundleTable     *BoltDBTable[CachedBundle]
	gvkTable        *BoltDBTable[CachedGVKBundle]
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

	gvkTable, err := createTableIgnoreExists[CachedGVKBundle](db, gvkBucket)

	return &boltPackageDatabase{
		databasePath:    databasePath,
		database:        db,
		repositoryTable: repositoryTable,
		packageTable:    packageTable,
		bundleTable:     bundleTable,
		gvkTable:        gvkTable,
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
	return b.database.Batch(func(tx *bolt.Tx) error {
		// delete repository entry
		if err := b.repositoryTable.DeleteEntryWithKeyInTransaction(tx, repoName); err != nil {
			return err
		}

		// delete packages
		prefix := repoName + keySeparator
		if err := b.packageTable.DeleteEntriesWithPrefixInTransaction(tx, prefix); err != nil {
			return err
		}

		// update gvk pre-calculation
		deletedBundles, err := b.bundleTable.Seek(prefix)
		if err != nil {
			return err
		}
		for _, deletedBundle := range deletedBundles {
			for _, providedAPI := range deletedBundle.ProvidedApis {
				key := GetGVKKey(providedAPI, deletedBundle.BundleID)
				if err := b.gvkTable.DeleteEntryWithKeyInTransaction(tx, key); err != nil {
					return err
				}
			}
		}

		// delete bundles
		return b.bundleTable.DeleteEntriesWithPrefixInTransaction(tx, prefix)
	})
}

func (b *boltPackageDatabase) CacheRepository(ctx context.Context, repository repository.Repository) error {
	if repository == nil {
		panic("repository is nil")
	}

	b.logger.Debugln("Caching repository from ", repository.Source())
	err := b.database.Batch(func(tx *bolt.Tx) error {
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

			var packageDependencies []property.Package
			for _, dependency := range bundle.Dependencies {
				switch dependency.GetType() {
				case property.TypePackage:
					packageDependency := &property.Package{}
					if err := json.Unmarshal([]byte(dependency.GetValue()), packageDependency); err != nil {
						return err
					}
					packageDependencies = append(packageDependencies, *packageDependency)
				}
			}

			cachedBundle := &CachedBundle{
				BundleID:            GetBundleKey(repoName, bundle),
				Bundle:              bundle,
				Repository:          repoName,
				DefaultChannelName:  defaultChannelNameMap[bundle.PackageName],
				PackageDependencies: packageDependencies,
			}
			if err := b.bundleTable.InsertInTransaction(tx, cachedBundle); err != nil {
				return nil
			}

			for _, gvk := range cachedBundle.ProvidedApis {
				key := GetGVKKey(gvk, cachedBundle.BundleID)
				if err := b.gvkTable.InsertInTransaction(tx, &CachedGVKBundle{
					CachedBundle: *cachedBundle,
					GVKID:        key,
					GVK:          strings.Join([]string{gvk.GetGroup(), gvk.GetVersion(), gvk.GetKind()}, keySeparator),
				}); err != nil {
					return err
				}
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

func (b *boltPackageDatabase) ListBundlesForGVK(_ context.Context, group string, version string, kind string) ([]CachedBundle, error) {
	gvkBundles, err := b.gvkTable.Seek(fmt.Sprintf("%s%s", strings.Join([]string{group, version, kind}, keySeparator), keySeparator))
	if err != nil {
		return nil, err
	}
	bundles := make([]CachedBundle, len(gvkBundles))
	for index, _ := range gvkBundles {
		bundles[index] = gvkBundles[index].CachedBundle
	}
	return bundles, nil
}

func (b *boltPackageDatabase) ListGVKs(ctx context.Context) (map[string][]CachedBundle, error) {
	gvkBundles, err := b.gvkTable.List()
	if err != nil {
		return nil, err
	}
	result := map[string][]CachedBundle{}
	for _, gvkBundle := range gvkBundles {
		result[gvkBundle.GVK] = append(result[gvkBundle.GVK], gvkBundle.CachedBundle)
	}
	return result, nil
}

func (b *boltPackageDatabase) ListBundles(_ context.Context) ([]CachedBundle, error) {
	start := time.Now()
	list, err := b.bundleTable.List()
	elapsed := time.Since(start)
	b.logger.Printf("took %s", elapsed)
	return list, err
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

func (b *boltPackageDatabase) GetBundlesForPackage(ctx context.Context, packageName string, options ...PackageSearchOption) ([]CachedBundle, error) {
	searchOptions, err := b.defaultPackageSearchConfig(ctx)
	if err != nil {
		return nil, err
	}
	searchOptions.applyOptions(options...)

	type result struct {
		bundles []CachedBundle
		err     error
	}
	resultsChannel := make(chan result, len(searchOptions.repositories))
	for _, repositoryName := range searchOptions.repositories {
		go func(prefix string) {
			entries, err := b.bundleTable.Seek(prefix)
			resultsChannel <- result{entries, err}
		}(fmt.Sprintf("%s%s%s%s", repositoryName, keySeparator, packageName, keySeparator))
	}

	var bundles []CachedBundle
	var errs []error
	for i := 0; i < cap(resultsChannel); i++ {
		result := <-resultsChannel
		if result.err == nil {
			for _, b := range result.bundles {
				if searchOptions.keep(&b) {
					bundles = append(bundles, b)
				}
			}
		} else {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return nil, errs[0]
	}

	return bundles, nil
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

func (b *boltPackageDatabase) defaultPackageSearchConfig(ctx context.Context) (*packageSearchConfig, error) {
	repos, err := b.ListRepositories(ctx)
	if err != nil {
		return nil, err
	}
	repoNames := make([]string, len(repos))
	for index, _ := range repos {
		repoNames[index] = repos[index].RepositoryName
	}
	return &packageSearchConfig{
		repositories: repoNames,
	}, nil
}

func GetBundleKey(repoName string, bundle *api.Bundle) string {
	return strings.Join([]string{repoName, bundle.PackageName, bundle.ChannelName, bundle.CsvName}, keySeparator)
}

func GetPackageKey(repoName, pkg string) string {
	return strings.Join([]string{repoName, pkg}, keySeparator)
}

func GetGVKKey(gvk *api.GroupVersionKind, bundleID string) string {
	return strings.Join([]string{gvk.GetGroup(), gvk.GetVersion(), gvk.GetKind(), bundleID}, keySeparator)
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
