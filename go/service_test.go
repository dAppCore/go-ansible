// SPDX-License-Identifier: EUPL-1.2

package ansible

import (
	"testing"

	core "dappco.re/go"
)

// TestNewService_DefaultOptions_RegistersWithParser verifies the canonical
// factory builds a Service with a pre-wired Parser when called with empty
// Options (Parser falls back to the process working directory).
func TestNewService_DefaultOptions_RegistersWithParser(t *testing.T) {
	c := core.New(core.WithService(NewService(Options{})))
	r := c.Service("ansible")
	if !r.OK {
		t.Fatal("ansible service not registered via NewService")
	}
	svc := r.Value.(*Service)
	if svc.Parser() == nil {
		t.Fatal("expected pre-wired Parser with default Options")
	}
}

// TestNewService_BasePath_ConstructsParserAtRoot verifies the BasePath
// option flows through to the Parser construction.
func TestNewService_BasePath_ConstructsParserAtRoot(t *testing.T) {
	dir := t.TempDir()
	c := core.New(core.WithService(NewService(Options{BasePath: dir})))
	r := c.Service("ansible")
	if !r.OK {
		t.Fatal("ansible service not registered")
	}
	svc := r.Value.(*Service)
	if svc.Parser() == nil {
		t.Fatal("expected pre-wired Parser with custom BasePath")
	}
}

// TestRegister_DefaultsRegistersWithParser verifies the imperative-style
// Register(c) shorthand registers the Service with default Options.
func TestRegister_DefaultsRegistersWithParser(t *testing.T) {
	c := core.New(core.WithService(Register))
	r := c.Service("ansible")
	if !r.OK {
		t.Fatalf("ansible service not registered via Register, got %#v", r.Value)
	}
	svc := r.Value.(*Service)
	if svc.Parser() == nil {
		t.Fatal("expected pre-wired Parser via Register")
	}
}

// TestService_NilReceiver_ParserReturnsNil verifies the nil-receiver guard
// on Parser() returns nil rather than panicking.
func TestService_NilReceiver_ParserReturnsNil(t *testing.T) {
	var svc *Service
	if p := svc.Parser(); p != nil {
		t.Fatalf("expected nil Parser from nil receiver, got %#v", p)
	}
}
