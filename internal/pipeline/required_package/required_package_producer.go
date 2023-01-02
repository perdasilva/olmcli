package required_package

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/perdasilva/olmcli/internal/eventbus"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

var _ eventbus.Producer[utils.OLMVariable] = &RequiredPackageProducer{}

const anyValue = ""

type Option func(requiredPackage *RequiredPackageProducer) error

func InRepository(repositoryName string) Option {
	return func(requiredPackage *RequiredPackageProducer) error {
		requiredPackage.repositoryName = repositoryName
		requiredPackage.searchOptions = append(requiredPackage.searchOptions, store.InRepositories(repositoryName))
		return nil
	}
}

func InChannel(channelName string) Option {
	return func(requiredPackage *RequiredPackageProducer) error {
		requiredPackage.channelName = channelName
		requiredPackage.searchOptions = append(requiredPackage.searchOptions, store.InChannel(channelName))
		return nil
	}
}

func InVersionRange(versionRange string) Option {
	return func(requiredPackage *RequiredPackageProducer) error {
		r, err := semver.ParseRange(versionRange)
		if err != nil {
			return err
		}
		requiredPackage.versionRange = versionRange
		requiredPackage.searchOptions = append(requiredPackage.searchOptions, store.InVersionRange(r))
		return nil
	}
}

type RequiredPackageProducer struct {
	repositoryName string
	packageName    string
	channelName    string
	versionRange   string
	searchOptions  []store.PackageSearchOption
	source         *utils.OLMEntitySource
	variables      []utils.OLMVariable
	index          int
}

func NewRequiredPackageProducer(packageName string, source *utils.OLMEntitySource, options ...Option) (*RequiredPackageProducer, error) {
	requiredPackage := &RequiredPackageProducer{
		packageName:    packageName,
		repositoryName: anyValue,
		channelName:    anyValue,
		versionRange:   anyValue,
		source:         source,
		index:          0,
	}
	for _, opt := range options {
		if err := opt(requiredPackage); err != nil {
			return nil, err
		}
	}
	return requiredPackage, nil
}

func (r *RequiredPackageProducer) Produce() (*utils.OLMVariable, error) {
	if r.index == 0 {
		if err := r.createVariables(); err != nil {
			return nil, err
		}
	}
	if r.index == len(r.variables) {
		return nil, nil
	}
	out := r.variables[r.index]
	r.index = r.index + 1
	return &out, nil
}

func (r *RequiredPackageProducer) RequiredPackageName() string {
	return r.packageName
}

func (r *RequiredPackageProducer) createVariables() error {
	bundles, err := r.source.GetBundlesForPackage(context.Background(), r.packageName, r.searchOptions...)
	if err != nil {
		return err
	}
	utils.Sort(bundles, utils.ByChannelAndVersion)
	id := sat.Identifier(fmt.Sprintf("required package %s from repository %s, channel %s, in semver range %s", r.packageName, r.repositoryName, r.channelName, r.versionRange))
	r.variables = []utils.OLMVariable{utils.NewRequiredPackageVariable(id, bundles...)}
	return nil
}
