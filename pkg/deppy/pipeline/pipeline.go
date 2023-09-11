package pipeline

import (
	"context"
	"github.com/google/uuid"
	"github.com/perdasilva/olmcli/pkg/deppy"
	"github.com/perdasilva/olmcli/pkg/deppy/pipeline/stages"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"slices"
	"strings"
	"time"
)

const PipelineIDContextKey = "pipeline-id"

// Pipeline represents a pipeline with an ordered list of stages and a unique identifier.
type Pipeline struct {
	id            deppy.Identifier
	orderedStages []stages.Stage
}

func NewPipeline(orderedStages ...stages.Stage) Pipeline {
	return Pipeline{
		id:            deppy.Identifier(uuid.NewString()),
		orderedStages: orderedStages,
	}
}

func watchErrors(ctx context.Context, stderr <-chan error) func() error {
	return func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-stderr:
				return err
			}
		}
	}
}

// Run executes the pipeline and returns a resolution problem (or dies trying)
func (r Pipeline) Run(ctx context.Context) (deppy.ResolutionProblem, error) {
	start := time.Now()
	pipeln, ctx := errgroup.WithContext(context.WithValue(ctx, PipelineIDContextKey, r.id))
	stump := make(chan deppy.Variable)
	close(stump)

	var result []deppy.Variable

	stdin := stump
	for _, stage := range r.orderedStages {
		// start stage process
		stdout, stderr := stage.Run(ctx, stdin)

		// output of one stage is the input to the next
		stdin = stdout

		// watch for stage errors
		pipeln.Go(watchErrors(ctx, stderr))
	}

	// collect results
	for v := range stdin {
		if v != nil {
			result = append(result, v)
		}
	}

	// wait for pipeline to finish and collect any errors
	if err := pipeln.Wait(); err != nil {
		slog.Error("error building problem", "runtime", time.Since(start), "err", err)
		return nil, err
	}

	weights := map[string]int{
		stages.RequiredPackageVariableKind:            0,
		stages.ChannelVariableKind:                    1,
		stages.UniquenessConstraintVariableKind:       2,
		stages.BundleDependencyConstraintVariableKind: 3,
		stages.BundleVariableKind:                     4,
	}

	slices.SortStableFunc[[]deppy.Variable](result, func(a, b deppy.Variable) int {
		if a.Kind() == b.Kind() {
			return strings.Compare(a.Identifier().String(), b.Identifier().String())
		}
		return weights[b.Kind()] - weights[a.Kind()]
	})

	// return variables as a resolution problem
	slog.Info("successfully built problem", "runtime", time.Since(start))
	return stages.NewResolutionProblem(r.id, result), nil
}
