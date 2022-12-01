package repo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/operator-framework/operator-registry/pkg/api"
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
	repositoryName   string
	repositorySource string
}

func (c *CachedRepository) RepositoryName() string {
	return c.repositoryName
}

func (c *CachedRepository) RepositorySource() string {
	return c.repositorySource
}

type CachedBundle struct {
	*api.Bundle
	ID                 string `json:"id"`
	Repository         string `json:"repository"`
	DefaultChannelName string `json:"defaultChannelName"`
}

type CachedPackage struct {
	*api.Package
	ID         string `json:"id"`
	Repository string `json:"repository"`
}

type PackageDatabase interface {
	HasRepository(repo string) bool
	ListRepositories() ([]CachedRepository, error)
	ListPackages() ([]CachedPackage, error)
	ListBundles() ([]CachedBundle, error)
	SearchPackages(searchTerm string) ([]CachedPackage, error)
	SearchBundles(searchTerm string) ([]CachedBundle, error)
	CacheRepository(ctx context.Context, repository *Repository) error
	RemoveRepository(repoName string) error
	GetPackage(packageID string) (*CachedPackage, error)
	GetBundle(bundleID string) (*CachedBundle, error)
	IterateBundles(fn func(bundle *CachedBundle) error) error
	Close() error
}

var _ PackageDatabase = &boltPackageDatabase{}

type boltPackageDatabase struct {
	databasePath string
	database     *bolt.DB
	logger       *logrus.Logger
}

func NewPackageDatabase(databasePath string, logger *logrus.Logger) (PackageDatabase, error) {
	if logger == nil {
		panic("logger is nil")
	}

	db, err := bolt.Open(databasePath, 0600, nil)
	if err != nil {
		return nil, err
	}

	// create buckets - or ignore exists errors
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(repositoriesBucket))
		if err != nil && !errors.Is(err, bolt.ErrBucketExists) {
			return err
		}
		_, err = tx.CreateBucket([]byte(packagesBucket))
		if err != nil && !errors.Is(err, bolt.ErrBucketExists) {
			return err
		}
		_, err = tx.CreateBucket([]byte(bundlesBucket))
		if err != nil && !errors.Is(err, bolt.ErrBucketExists) {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &boltPackageDatabase{
		databasePath: databasePath,
		database:     db,
		logger:       logger,
	}, nil
}

func (b *boltPackageDatabase) HasRepository(repoName string) bool {
	err := b.database.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(repositoriesBucket))
		if v := b.Get([]byte(repoName)); v == nil {
			return nil
		}
		return fmt.Errorf("repository with name %s already exists", repoName)
	})
	return err != nil
}

func (b *boltPackageDatabase) ListRepositories() ([]CachedRepository, error) {
	var repos []CachedRepository
	err := b.database.View(func(tx *bolt.Tx) error {
		repoBucket := tx.Bucket([]byte(repositoriesBucket))
		return repoBucket.ForEach(func(k, v []byte) error {
			repos = append(repos, CachedRepository{string(k), string(v)})
			return nil
		})
	})
	return repos, err
}

func (b *boltPackageDatabase) RemoveRepository(repoName string) error {
	return b.database.Update(func(tx *bolt.Tx) error {
		repoBucket := tx.Bucket([]byte(repositoriesBucket))
		if value := repoBucket.Get([]byte(repoName)); value == nil {
			return fmt.Errorf("repository %s not found", repoName)
		}

		if err := repoBucket.Delete([]byte(repoName)); err != nil {
			return err
		}

		pkgBucket := tx.Bucket([]byte(packagesBucket))
		cursor := pkgBucket.Cursor()
		prefix := []byte(repoName + keySeparator)
		for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
			b.logger.Debugln("deleting bundle: ", string(key))
			if err := pkgBucket.Delete(key); err != nil {
				return err
			}
		}

		bundleBucket := tx.Bucket([]byte(bundlesBucket))
		cursor = bundleBucket.Cursor()
		prefix = []byte(repoName + keySeparator)
		for key, _ := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, _ = cursor.Next() {
			b.logger.Debugln("deleting bundle: ", string(key))
			if err := bundleBucket.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *boltPackageDatabase) CacheRepository(ctx context.Context, repository *Repository) error {
	if repository == nil {
		panic("repository is nil")
	}

	b.logger.Debugln("Caching repository from ", repository.Source())
	err := b.database.Update(func(tx *bolt.Tx) error {
		// extract repo name (in this case the name of the image)
		repoName := getRepoName(repository.Source())

		// iterate over bundles and write them out to the database inc. their packages
		pkgBucket := tx.Bucket([]byte(packagesBucket))
		bundleBucket := tx.Bucket([]byte(bundlesBucket))
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
					Package:    pkg,
					ID:         GetPackageKey(repoName, pkg.Name),
					Repository: repoName,
				}
				pkgBytes, err := json.Marshal(cachedPackage)
				if err != nil {
					return err
				}

				pkgKey := []byte(cachedPackage.ID)
				if err := pkgBucket.Put(pkgKey, pkgBytes); err != nil {
					return err
				}
				defaultChannelNameMap[pkgName] = pkg.DefaultChannelName
			}
			cachedBundle := &CachedBundle{
				Bundle:             bundle,
				ID:                 GetBundleKey(repoName, bundle),
				Repository:         repoName,
				DefaultChannelName: defaultChannelNameMap[bundle.PackageName],
			}
			bundleBytes, err := json.Marshal(cachedBundle)
			if err != nil {
				return err
			}
			bundleKey := []byte(GetBundleKey(repoName, bundle))
			if err := bundleBucket.Put(bundleKey, bundleBytes); err != nil {
				return err
			}
		}

		// add repo record
		b.logger.Debugln("Adding repository record...")
		repoBucket := tx.Bucket([]byte(repositoriesBucket))

		// create record
		return repoBucket.Put([]byte(repoName), []byte(repository.Source()))
	})
	b.logger.Debugln("Done...")
	return err
}

func (b *boltPackageDatabase) ListPackages() ([]CachedPackage, error) {
	var results []CachedPackage
	err := b.database.View(func(tx *bolt.Tx) error {
		pkgBucket := tx.Bucket([]byte(packagesBucket))
		return pkgBucket.ForEach(func(key, data []byte) error {
			pkg := &CachedPackage{}
			if err := json.Unmarshal(data, pkg); err != nil {
				return err
			}
			results = append(results, *pkg)
			return nil
		})
	})
	return results, err
}

func (b *boltPackageDatabase) ListBundles() ([]CachedBundle, error) {
	var results []CachedBundle
	err := b.database.View(func(tx *bolt.Tx) error {
		bundleBucket := tx.Bucket([]byte(bundlesBucket))
		return bundleBucket.ForEach(func(key, data []byte) error {
			bundle := &CachedBundle{}
			if err := json.Unmarshal(data, bundle); err != nil {
				return err
			}
			results = append(results, *bundle)
			return nil
		})
	})
	return results, err
}

func (b *boltPackageDatabase) IterateBundles(fn func(bundle *CachedBundle) error) error {
	return b.database.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bundlesBucket))
		return b.ForEach(func(key, value []byte) error {
			bundle := &CachedBundle{}
			if err := json.Unmarshal(value, bundle); err != nil {
				return err
			}
			return fn(bundle)
		})
	})
}

func (b *boltPackageDatabase) SearchPackages(searchTerm string) ([]CachedPackage, error) {
	var results []CachedPackage
	err := b.database.View(func(tx *bolt.Tx) error {
		pkgBucket := tx.Bucket([]byte(packagesBucket))
		return pkgBucket.ForEach(func(key, data []byte) error {
			keyComponents := strings.Split(string(key), keySeparator)
			packageName := keyComponents[1]
			if strings.Index(packageName, searchTerm) >= 0 {
				pkg := &CachedPackage{}
				if err := json.Unmarshal(data, pkg); err != nil {
					return err
				}
				results = append(results, *pkg)
			}
			return nil
		})
	})
	return results, err
}

func (b *boltPackageDatabase) SearchBundles(searchTerm string) ([]CachedBundle, error) {
	var results []CachedBundle
	err := b.database.View(func(tx *bolt.Tx) error {
		bundleBucket := tx.Bucket([]byte(bundlesBucket))
		return bundleBucket.ForEach(func(key, data []byte) error {
			keyComponents := strings.Split(string(key), keySeparator)
			packageName := keyComponents[1]
			if strings.Index(packageName, searchTerm) >= 0 {
				bundle := &CachedBundle{}
				if err := json.Unmarshal(data, bundle); err != nil {
					return err
				}
				results = append(results, *bundle)
			}
			return nil
		})
	})
	return results, err
}

func (b *boltPackageDatabase) GetPackage(packageID string) (*CachedPackage, error) {
	var cachedPkg *CachedPackage
	err := b.database.View(func(tx *bolt.Tx) error {
		pkgBucket := tx.Bucket([]byte(packagesBucket))
		valueBytes := pkgBucket.Get([]byte(packageID))
		if valueBytes == nil {
			return nil
		}
		pkg := &CachedPackage{}
		if err := json.Unmarshal(valueBytes, pkg); err != nil {
			return err
		}
		cachedPkg = pkg
		return nil
	})
	return cachedPkg, err
}

func (b *boltPackageDatabase) GetBundle(bundleID string) (*CachedBundle, error) {
	var cachedBundle *CachedBundle
	err := b.database.View(func(tx *bolt.Tx) error {
		bundleBucket := tx.Bucket([]byte(bundlesBucket))
		valueBytes := bundleBucket.Get([]byte(bundleID))
		if valueBytes == nil {
			return nil
		}
		bundle := &CachedBundle{}
		if err := json.Unmarshal(valueBytes, bundle); err != nil {
			return err
		}
		cachedBundle = bundle
		return nil
	})
	return cachedBundle, err
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

func GetPackageKey(repoName string, pkg string) string {
	return strings.Join([]string{repoName, pkg}, keySeparator)
}

func getRepoName(repoSource string) string {
	regex := regexp.MustCompile(imageRegexp)
	match := regex.FindStringSubmatch(repoSource)
	imageIndex := regex.SubexpIndex("image")
	return match[imageIndex]
}
