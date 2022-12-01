package repo

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/operator-framework/operator-registry/pkg/client"
	"github.com/sirupsen/logrus"
)

type Repository struct {
	*client.Client
	repo        string
	logger      *logrus.Logger
	containerID string
}

func NewRegistry(repo string, logger *logrus.Logger) *Repository {
	if logger == nil {
		panic("logger not set")
	}

	return &Repository{
		repo:   repo,
		logger: logger,
	}
}

func (r *Repository) Source() string {
	return r.repo
}

func (r *Repository) Connect() error {
	// start repository container
	r.logger.Debugln("Starting container...")
	startContainerCmd := []string{"docker", "run", "--rm", "-d", "-p", "50051:50051", r.repo}
	r.logger.Debugln("Executing ", strings.Join(startContainerCmd, " "))
	cmd := exec.Command(startContainerCmd[0], startContainerCmd[1:]...)
	stdout, err := cmd.Output()
	if err != nil {
		return err
	}

	// save container ID for clean up
	r.containerID = strings.TrimSpace(string(stdout))

	r.logger.Debugln("Connecting to registry...")
	r.Client, err = client.NewClient("localhost:50051")
	if err != nil {
		return err
	}

	r.logger.Debugln("Waiting for registry...")
	err = retry.Do(func() error {
		ok, err := r.HealthCheck(context.Background(), 5*time.Second)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("registry is not healthy")
		}
		return nil
	}, retry.Delay(5*time.Second), retry.Attempts(6))
	if err != nil {
		return err
	}

	return nil
}

func (r *Repository) Close() error {
	// close client
	if r.Client != nil {
		if err := r.Client.Close(); err != nil {
			r.logger.Debugln("error closing registry connection: ", err)
		}
	}

	// kill registry container
	if r.containerID != "" {
		r.logger.Debugf("Killing container %s\n", r.containerID)
		killContainerCmd := []string{"docker", "stop", r.containerID}
		r.logger.Debugf("executing %s", strings.Join(killContainerCmd, " "))
		cmd := exec.Command(killContainerCmd[0], killContainerCmd[1:]...)
		if err := cmd.Run(); err != nil {
			r.logger.Debugf("error stopping container: %v", err)
		}
	}

	return nil
}
