// SPDX-License-Identifier: EUPL-1.2

// Service registration for the ansible package — exposes the canonical
// `NewService(opts)` + `Register(c)` shape per Mantis #1336, holding a
// pre-wired Parser + Executor pair that consumers can use through a
// typed handle:
//
//	c, _ := core.New(
//	    core.WithService(ansible.NewService(ansible.Options{
//	        BasePath: "/srv/playbooks",
//	    })),
//	)
//	svc := core.MustServiceFor[*ansible.Service](c, "ansible")
//	r := svc.Parser().ParsePlaybook("site.yml")
//
// The package-level types (Parser, Executor) remain the source of truth —
// Service is a thin Core-bound handle that pre-constructs them with the
// supplied options. Consumers that need bespoke wiring can keep using
// NewParser / NewExecutor directly.
//
// The CLI command surface lives in the sibling `ansiblecmd` package
// (cmd/ansible) and registers `ansible` + `ansible/test` commands —
// independent of this service registration, so consumers can wire either
// or both.

package ansible

import (
	core "dappco.re/go"
)

// Options configures the ansible service. All fields are optional —
// an empty Options yields a Service with a Parser rooted at the
// process working directory and no pre-built Executor (callers wire
// transports per-host through NewExecutor).
type Options struct {
	// BasePath is the playbook root the pre-wired Parser resolves
	// relative paths against. Empty → Parser uses the process working
	// directory via NewParser("").
	BasePath string
}

// Service is the registerable handle for the ansible package — embeds
// *core.ServiceRuntime[Options] for typed options access and exposes a
// pre-wired *Parser ready for playbook / inventory / role parsing.
//
// Usage example: `svc := core.MustServiceFor[*ansible.Service](c, "ansible"); r := svc.Parser().ParsePlaybook("site.yml")`
type Service struct {
	*core.ServiceRuntime[Options]
	// parser is the pre-built Parser configured with Options.BasePath.
	// Access through Service.Parser().
	parser *Parser
}

// NewService returns a factory that constructs a *Service with the
// supplied options and registers it under "ansible" via
// core.WithService.
//
//	core.WithService(ansible.NewService(ansible.Options{BasePath: "/srv/playbooks"}))
//
// The returned factory always succeeds — Parser construction is
// in-memory and never errors. Callers that need bespoke Parser
// configuration (custom Medium, etc.) can mutate svc.Parser() after
// registration or bypass the Service entirely and use NewParser.
func NewService(opts Options) func(*core.Core) core.Result {
	return func(c *core.Core) core.Result {
		return core.Ok(&Service{
			ServiceRuntime: core.NewServiceRuntime(c, opts),
			parser:         NewParser(opts.BasePath),
		})
	}
}

// Register wires the ansible service into the Core with default Options
// — the imperative-style alternative to NewService for consumers that
// don't use the WithService factory pattern.
//
//	c := core.New()
//	if r := ansible.Register(c); !r.OK { return r }
//	svc := core.MustServiceFor[*ansible.Service](c, "ansible")
func Register(c *core.Core) core.Result {
	return NewService(Options{})(c)
}

// Parser returns the service's pre-built *Parser. Callers that need a
// fresh Parser with bespoke configuration should construct one via
// NewParser instead of mutating the shared one.
//
//	p := svc.Parser()
//	r := p.ParsePlaybook("site.yml")
func (s *Service) Parser() *Parser {
	if s == nil {
		return nil
	}
	return s.parser
}
