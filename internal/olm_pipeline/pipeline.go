package olm_pipeline

import (
	"context"
	"fmt"

	"github.com/perdasilva/olmcli/internal/olm_pipeline/bundle_generator"
	"github.com/perdasilva/olmcli/internal/olm_pipeline/output"
	"github.com/perdasilva/olmcli/internal/olm_pipeline/required_package"
	"github.com/perdasilva/olmcli/internal/olm_pipeline/solver"
	"github.com/perdasilva/olmcli/internal/olm_pipeline/uniqueness"
	"github.com/perdasilva/olmcli/internal/pipeline"
	"github.com/perdasilva/olmcli/internal/pipeline/eventbus"
	"github.com/perdasilva/olmcli/internal/pipeline/eventbus/eventrouter"
	v2 "github.com/perdasilva/olmcli/internal/pipeline/eventsource/v2"
	"github.com/perdasilva/olmcli/internal/utils"
)

type ResolutionPipeline struct {
	source *utils.OLMEntitySource
}

func NewResolutionPipeline(source *utils.OLMEntitySource) *ResolutionPipeline {
	return &ResolutionPipeline{
		source: source,
	}
}

func (r *ResolutionPipeline) Execute(ctx context.Context, requiredPackages ...*required_package.RequiredPackageProducer) ([]utils.OLMVariable, error) {
	debug := false
	debugChannel := make(chan pipeline.Event)
	bus := eventbus.NewEventBus(ctx, eventrouter.WithDebugChannel(debugChannel))

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt, hasNext := <-debugChannel:
				if !hasNext {
					return
				}
				if debug {
					fmt.Println(evt)
				}
			}
		}
	}()

	// create network

	// network output
	resolverOutput := output.NewOutput()
	out := v2.NewConsumerEventSource[utils.OLMVariable]("output", resolverOutput, bus)
	out.Start(ctx)

	// solver
	resolver := v2.NewProcessorEventSource[utils.OLMVariable]("resolver", solver.NewSolver(), bus)
	resolver.AddOutput("output")
	resolver.Start(ctx)

	// global uniqueness constraints
	globalConstraints := v2.NewProcessorEventSource[utils.OLMVariable]("globalConstraints", uniqueness.NewUniqueness(), bus)
	globalConstraints.AddOutput("resolver")
	globalConstraints.Start(ctx)

	// bundle and bundle dependency variables
	bundleAndDependencies := v2.NewProcessorEventSource[utils.OLMVariable]("bundlesAndDependencies", bundle_generator.NewBundleGenerator(r.source), bus)
	bundleAndDependencies.AddOutput("resolver", "globalConstraints")
	bundleAndDependencies.Start(ctx)

	// sources
	var sources []pipeline.ProducerEventSource
	for _, rp := range requiredPackages {
		source := v2.NewProducerEventSource[utils.OLMVariable](pipeline.EventSourceID(fmt.Sprintf("%s-required", rp.RequiredPackageName())), rp, bus)
		source.AddOutput("resolver", "bundlesAndDependencies")
		source.Start(ctx)
		sources = append(sources, source)
	}

	// wait for output node to complete
	<-out.Done()
	//<-resolver.Done()
	//<-globalConstraints.Done()
	//<-bundleAndDependencies.Done()
	//for _, s := range sources {
	//	<-s.Done()
	//}
	bus.Stop()

	/*fmt.Printf("output node: %s\n", out.State())
	fmt.Printf("solver node: %s\n", resolver.State())
	fmt.Printf("global constraints node: %s\n", globalConstraints.State())
	fmt.Printf("bundles and dependencies node: %s\n", bundleAndDependencies.State())
	for _, s := range sources {
		fmt.Printf("source node (%s): %s\n", s.EventSourceID(), s.State())
	}*/

	switch out.State() {
	case pipeline.EventSourceStateSuccess:
		return resolverOutput.Variables(), nil
	case pipeline.EventSourceStateFailed:
		return nil, out.Reason().Error()
	case pipeline.EventSourceStateAborted:
		return nil, out.Reason().Error()
	default:
		return nil, fmt.Errorf("output consumer is in unknown state (%s)", out.State())
	}
}
