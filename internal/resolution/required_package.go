package resolution

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
	"github.com/perdasilva/olmcli/internal/utils"
)

const anyValue = "any"

type Option func(requiredPackage *RequiredPackage) error

func InRepo(repositoryName string) Option {
	return func(requiredPackage *RequiredPackage) error {
		requiredPackage.repositoryName = repositoryName
		requiredPackage.searchOptions = append(requiredPackage.searchOptions, store.InRepositories(repositoryName))
		return nil
	}
}

func InChan(channelName string) Option {
	return func(requiredPackage *RequiredPackage) error {
		requiredPackage.channelName = channelName
		requiredPackage.searchOptions = append(requiredPackage.searchOptions, store.InChannel(channelName))
		return nil
	}
}

func InVersionRange(versionRange string) Option {
	return func(requiredPackage *RequiredPackage) error {
		r, err := semver.ParseRange(versionRange)
		if err != nil {
			return err
		}
		requiredPackage.versionRange = versionRange
		requiredPackage.searchOptions = append(requiredPackage.searchOptions, store.InVersionRange(r))
		return nil
	}
}

var _ v2.VariableSource[*store.CachedBundle, utils.OLMVariable, *utils.OLMEntitySource] = &RequiredPackage{}

type RequiredPackage struct {
	repositoryName string
	packageName    string
	channelName    string
	versionRange   string
	searchOptions  []store.PackageSearchOption
}

func NewRequiredPackage(packageName string, options ...Option) (*RequiredPackage, error) {
	requiredPackage := &RequiredPackage{
		packageName:    packageName,
		repositoryName: anyValue,
		channelName:    anyValue,
		versionRange:   anyValue,
	}
	for _, opt := range options {
		if err := opt(requiredPackage); err != nil {
			return nil, err
		}
	}
	return requiredPackage, nil
}

func (r *RequiredPackage) GetVariables(ctx context.Context, source *utils.OLMEntitySource) ([]utils.OLMVariable, error) {
	bundles, err := source.GetBundlesForPackage(ctx, r.packageName, r.searchOptions...)
	if err != nil {
		return nil, err
	}
	utils.Sort(bundles, utils.ByChannelAndVersion)
	return []utils.OLMVariable{utils.NewRequiredPackageVariable(r.getVariableID(), bundles...)}, nil
}

func (r *RequiredPackage) getVariableID() sat.Identifier {
	return sat.Identifier(fmt.Sprintf("required package %s from repository %s, channel %s, in semver range %s", r.packageName, r.repositoryName, r.channelName, r.versionRange))
}
