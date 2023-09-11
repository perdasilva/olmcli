package solver

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-air/gini"
	"github.com/go-air/gini/inter"
	"github.com/go-air/gini/z"
	"github.com/perdasilva/olmcli/pkg/deppy"
	"log/slog"
)

var ErrIncomplete = errors.New("cancelled before a solution could be found")

type solver struct {
	g      inter.S
	litMap *litMapping
	tracer deppy.Tracer
	// buffer                 []z.Lit
	disableOrderPreference bool
}

const (
	satisfiable   = 1
	unsatisfiable = -1
	unknown       = 0
)

// Solve takes a slice containing all Variables and returns a slice
// containing only those Variables that were selected for
// installation. If no solution is possible, or if the provided
// Context times out or is cancelled, an error is returned.
func (s *solver) Solve(ctx context.Context) (result []deppy.Variable, err error) {
	defer func() {
		// This likely indicates a bug, so discard whatever
		// return values were produced.
		if derr := s.litMap.Error(); derr != nil {
			result = nil
			err = derr
		}
	}()

	// teach all constraints to the solver
	s.litMap.AddConstraints(s.g)

	// collect literals of all mandatory variables to assume as a baseline
	anchors := s.litMap.AnchorIdentifiers()
	assumptions := make([]z.Lit, len(anchors))
	for i := range anchors {
		assumptions[i] = s.litMap.LitOf(anchors[i])
	}

	// assume that all constraints hold
	s.litMap.AssumeConstraints(s.g)
	s.g.Assume(assumptions...)

	if s.disableOrderPreference {
		if s.g.Solve() == satisfiable {
			return s.litMap.Variables(s.g), nil
		} else {
			return nil, deppy.NotSatisfiable(s.litMap.Conflicts(s.g))
		}
	}

	var aset map[z.Lit]struct{}
	// push a new test scope with the baseline assumptions, to prevent them from being cleared during search
	outcome, _ := s.g.Test(nil)

	if outcome != satisfiable && outcome != unsatisfiable {
		// searcher for solutions in input Order, so that preferences
		// can be taken into account (i.e. prefer one catalog to another)
		outcome, assumptions, aset = (&search{s: s.g, lits: s.litMap, tracer: s.tracer}).Do(context.Background(), assumptions)
	}
	switch outcome {
	case satisfiable:
		var buffer []z.Lit
		buffer = s.litMap.Lits(buffer)
		var extras, excluded []z.Lit
		for _, m := range buffer {
			if _, ok := aset[m]; ok {
				continue
			}
			if !s.g.Value(m) {
				excluded = append(excluded, m.Not())
				continue
			}
			extras = append(extras, m)
		}

		var asetNames []string
		var extrasNames []string
		var excludedNames []string
		var excludedVars []deppy.Variable

		for _, m := range assumptions {
			if m > 0 {
				asetNames = append(asetNames, s.litMap.VariableOf(m).Identifier().String())
			}
		}
		for _, m := range extras {
			if m > 0 {
				extrasNames = append(extrasNames, s.litMap.VariableOf(m).Identifier().String())
			}
		}
		for _, m := range excluded {
			if m > 0 {
				var v deppy.Variable = zeroVariable{}
				if s.litMap.HasVariableForLit(m.Not()) {
					v = s.litMap.VariableOf(m.Not())
				}
				excludedNames = append(excludedNames, v.Identifier().String())
				excludedVars = append(excludedVars, v)
			}
		}

		s.g.Untest()
		slog.Info("optimizing for cardinality", "assumptions", asetNames, "extras", extrasNames, "excluded", excludedNames)
		cs := s.litMap.CardinalityConstrainer(s.g, extras)
		s.g.Assume(assumptions...)
		s.g.Assume(excluded...)
		// _, buffer = s.g.Test(buffer)
		s.litMap.AssumeConstraints(s.g)
		_, buffer = s.g.Test(buffer)
		for w := 0; w <= cs.N(); w++ {
			s.g.Assume(cs.Leq(w))
			if s.g.Solve() == satisfiable {
				for _, m := range assumptions {
					if !s.g.Value(m) {
						slog.Info("assumption failed", "assumption", s.litMap.VariableOf(m).Identifier().String())
					}
				}
				return s.litMap.Variables(s.g), nil
			}
		}
		// Something is wrong if we can't find a model anymore
		// after optimizing for cardinality.
		return nil, fmt.Errorf("unexpected internal error")
	case unsatisfiable:
		return nil, deppy.NotSatisfiable(s.litMap.Conflicts(s.g))
	}

	return nil, ErrIncomplete
}

func NewSolver(options ...Option) (deppy.Solver, error) {
	s := solver{g: gini.New()}
	for _, option := range append(defaults, options...) {
		if err := option(&s); err != nil {
			return nil, err
		}
	}
	return &s, nil
}

type Option func(s *solver) error

// todo: add tests for this
func DisableOrderPreference() Option {
	return func(s *solver) error {
		s.disableOrderPreference = true
		return nil
	}
}

func WithInput(input []deppy.Variable) Option {
	return func(s *solver) error {
		var err error
		s.litMap, err = newLitMapping(input)
		return err
	}
}

func WithTracer(t deppy.Tracer) Option {
	return func(s *solver) error {
		s.tracer = t
		return nil
	}
}

var defaults = []Option{
	func(s *solver) error {
		if s.litMap == nil {
			var err error
			s.litMap, err = newLitMapping(nil)
			return err
		}
		return nil
	},
	func(s *solver) error {
		if s.tracer == nil {
			s.tracer = DefaultTracer{}
		}
		return nil
	},
	func(s *solver) error {
		s.disableOrderPreference = false
		return nil
	},
}
