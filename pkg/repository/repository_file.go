package repository

import (
	"os/exec"
	"strings"

	"github.com/sirupsen/logrus"
)

var _ RepositoryContainer = &fileRepository{}

type fileRepository struct {
	containerID        string
	repositoryImageUrl string
	logger             *logrus.Logger
}

func (s *fileRepository) RepositoryURL() string {
	return "localhost:50051"
}

func (s *fileRepository) ImageURL() string {
	return s.repositoryImageUrl
}

func (s *fileRepository) Start() error {
	s.logger.Debugln("Starting container...")
	startContainerCmd := []string{"docker", "run", "--rm", "-d", "-p", "50051:50051", s.repositoryImageUrl}
	s.logger.Debugln("Executing ", strings.Join(startContainerCmd, " "))
	cmd := exec.Command(startContainerCmd[0], startContainerCmd[1:]...)
	stdout, err := cmd.Output()
	if err != nil {
		return err
	}
	// save container EntryID for clean up
	s.containerID = strings.TrimSpace(string(stdout))
	return nil
}

func (s *fileRepository) Stop() error {
	if s.containerID != "" {
		s.logger.Debugf("Killing container %s\n", s.containerID)
		killContainerCmd := []string{"docker", "stop", s.containerID}
		s.logger.Debugf("executing %s", strings.Join(killContainerCmd, " "))
		cmd := exec.Command(killContainerCmd[0], killContainerCmd[1:]...)
		if err := cmd.Run(); err != nil {
			s.logger.Debugf("error stopping container: %v", err)
		}
	}
	return nil
}
