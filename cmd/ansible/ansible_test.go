package anscmd

import (
	"testing"

	"dappco.re/go/core"
	"github.com/stretchr/testify/assert"
)

func TestExtraVars_Good_RepeatableAndCommaSeparated(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "version=1.2.3,env=prod"},
		core.Option{Key: "extra-vars", Value: "region=us-east-1"},
		core.Option{Key: "extra-vars", Value: []string{"build=42"}},
	)

	vars := extraVars(opts)

	assert.Equal(t, map[string]string{
		"version": "1.2.3",
		"env":     "prod",
		"region":  "us-east-1",
		"build":   "42",
	}, vars)
}

func TestExtraVars_Good_UsesShortAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "e", Value: "version=1.2.3,env=prod"},
	)

	vars := extraVars(opts)

	assert.Equal(t, map[string]string{
		"version": "1.2.3",
		"env":     "prod",
	}, vars)
}

func TestExtraVars_Good_IgnoresMalformedPairs(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "extra-vars", Value: "missing_equals,keep=this"},
		core.Option{Key: "extra-vars", Value: "also_bad="},
	)

	vars := extraVars(opts)

	assert.Equal(t, map[string]string{
		"keep":     "this",
		"also_bad": "",
	}, vars)
}

func TestFirstString_Good_PrefersFirstNonEmptyKey(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "inventory", Value: ""},
		core.Option{Key: "i", Value: "/tmp/inventory.yml"},
	)

	assert.Equal(t, "/tmp/inventory.yml", firstString(opts, "inventory", "i"))
}

func TestFirstBool_Good_UsesAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "v", Value: true},
	)

	assert.True(t, firstBool(opts, "verbose", "v"))
}

func TestTestKeyFile_Good_PrefersExplicitKey(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "key", Value: "/tmp/id_ed25519"},
		core.Option{Key: "i", Value: "/tmp/ignored"},
	)

	assert.Equal(t, "/tmp/id_ed25519", testKeyFile(opts))
}

func TestTestKeyFile_Good_FallsBackToShortAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "i", Value: "/tmp/id_ed25519"},
	)

	assert.Equal(t, "/tmp/id_ed25519", testKeyFile(opts))
}

func TestFirstString_Good_ResolvesShortUserAlias(t *testing.T) {
	opts := core.NewOptions(
		core.Option{Key: "u", Value: "deploy"},
	)

	cfgUser := firstString(opts, "user", "u")

	assert.Equal(t, "deploy", cfgUser)
}
