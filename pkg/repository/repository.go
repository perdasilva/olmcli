package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/operator-framework/operator-registry/pkg/client"
	"github.com/sirupsen/logrus"
)

const (
	retryDelay                  = 5 * time.Second
	retryAttempts               = 6
	healthCheckReconnectTimeout = 5 * time.Second
)

type URLBasedRepository struct {
	*client.Client
	repositoryURL string
	logger        *logrus.Logger
}

type ImageBasedRepository struct {
	*URLBasedRepository
	repositoryContainer RepositoryContainer
	containerID         string
}

func FromURL(repositoryURL string, logger *logrus.Logger) *URLBasedRepository {
	if logger == nil {
		panic("logger not set")
	}

	return &URLBasedRepository{
		repositoryURL: repositoryURL,
		logger:        logger,
	}
}

func FromImageURL(repositoryImageURL string, logger *logrus.Logger) *ImageBasedRepository {
	if logger == nil {
		panic("logger not set")
	}

	repositoryContainer := &simpleRepositoryContainer{
		logger:             logger,
		repositoryImageUrl: repositoryImageURL,
	}
	return &ImageBasedRepository{
		URLBasedRepository:  FromURL(repositoryContainer.RepositoryURL(), logger),
		repositoryContainer: repositoryContainer,
	}
}

func (r *URLBasedRepository) Source() string {
	return r.repositoryURL
}

func (r *URLBasedRepository) Connect(ctx context.Context) error {
	var err error
	r.logger.Debugln("Connecting to registry...")
	r.Client, err = client.NewClient(r.repositoryURL)
	if err != nil {
		return err
	}

	r.logger.Debugln("Waiting for registry...")
	err = retry.Do(func() error {
		ok, err := r.HealthCheck(ctx, healthCheckReconnectTimeout)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("registry is not healthy")
		}
		return nil
	}, retry.Delay(retryDelay), retry.Attempts(retryAttempts))
	if err != nil {
		return err
	}

	return nil
}

func (r *URLBasedRepository) Close() error {
	if r.Client != nil {
		if err := r.Client.Close(); err != nil {
			r.logger.Debugln("error closing registry connection: ", err)
		}
	}
	return nil
}

func (r *ImageBasedRepository) Source() string {
	return r.repositoryContainer.ImageURL()
}

func (r *ImageBasedRepository) Connect(ctx context.Context) error {
	if err := r.repositoryContainer.Start(); err != nil {
		return err
	}
	return r.URLBasedRepository.Connect(ctx)
}

func (r *ImageBasedRepository) Close() error {
	if err := r.URLBasedRepository.Close(); err != nil {
		return err
	}
	return r.repositoryContainer.Stop()
}
