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
