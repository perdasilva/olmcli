package resolution

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/operator-framework/deppy/pkg/sat"
	"github.com/operator-framework/deppy/pkg/v2"
	"github.com/perdasilva/olmcli/internal/store"
)

const anyValue = "anyValue"

type Option func(requiredPackage *RequiredPackage) error

func InRepo(repositoryName string) Option {
	return func(requiredPackage *RequiredPackage) error {
		requiredPackage.repositoryName = repositoryName
		requiredPackage.predicates = append(requiredPackage.predicates, InRepository(repositoryName))
		return nil
	}
}

func InChan(channelName string) Option {
	return func(requiredPackage *RequiredPackage) error {
		requiredPackage.channelName = channelName
		requiredPackage.predicates = append(requiredPackage.predicates, InChannel(channelName))
		return nil
	}
}

func InPkg(packageName string) Option {
	return func(requiredPackage *RequiredPackage) error {
		requiredPackage.packageName = packageName
		requiredPackage.predicates = append(requiredPackage.predicates, InPackage(packageName))
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
		requiredPackage.predicates = append(requiredPackage.predicates, InSemverRange(r))
		return nil
	}
}

var _ v2.VariableSource[OLMEntity, OLMVariable, *OLMEntitySource] = &RequiredPackage{}

type RequiredPackage struct {
	repositoryName string
	packageName    string
	channelName    string
	versionRange   string
	predicates     []Predicate[OLMEntity]
}

func NewRequiredPackage(options ...Option) (*RequiredPackage, error) {
	requiredPackage := &RequiredPackage{
		repositoryName: anyValue,
		packageName:    anyValue,
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

func (r *RequiredPackage) GetVariables(ctx context.Context, source *OLMEntitySource) ([]OLMVariable, error) {
	var entities []OLMEntity
	err := source.IterateBundles(ctx, func(bundle *store.CachedBundle) error {
		if bundle == nil {
			return nil
		}
		entity := OLMEntity{bundle}
		if And(r.predicates...).Keep(&entity) {
			entities = append(entities, entity)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	Sort(entities, ByChannelAndVersion)
	return []OLMVariable{NewRequiredPackageVariable(r.getVariableID(), entities...)}, nil
}

func (r *RequiredPackage) getVariableID() sat.Identifier {
	return sat.Identifier(fmt.Sprintf("required package %s from repository %s, channel %s, in semver range %s", r.packageName, r.repositoryName, r.channelName, r.versionRange))
}
