package ansiblecmd

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"dappco.re/go/core"
	"dappco.re/go/core/ansible"
	coreio "dappco.re/go/core/io"
	coreerr "dappco.re/go/core/log"
	"gopkg.in/yaml.v3"
)

type playbookCommandSettings struct {
	playbookPath string
	basePath     string
	limit        string
	tags         []string
	skipTags     []string
	extraVars    map[string]any
	verbose      int
	checkMode    bool
	diff         bool
}

func splitCommaSeparatedOption(value string) []string {
	if value == "" {
		return nil
	}
	return split(value, ",")
}

// positionalArgs extracts all positional arguments from Options.
func positionalArgs(opts core.Options) []string {
	var out []string
	for _, o := range opts.Items() {
		if o.Key == "_arg" {
			if s, ok := o.Value.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// firstStringOption returns the first non-empty string for any of the provided keys.
func firstStringOption(opts core.Options, keys ...string) string {
	for _, key := range keys {
		if value := opts.String(key); value != "" {
			return value
		}
	}
	return ""
}

// firstBoolOption returns true when any of the provided keys is set to true.
func firstBoolOption(opts core.Options, keys ...string) bool {
	for _, key := range keys {
		if opts.Bool(key) {
			return true
		}
	}
	return false
}

// verbosityLevel resolves the effective verbosity from parsed options and the
// raw command line arguments. The core CLI parser does not preserve repeated
// `-v` tokens, so we count them from os.Args as a fallback.
func verbosityLevel(opts core.Options, rawArgs []string) int {
	level := opts.Int("verbose")
	if firstBoolOption(opts, "v") && level < 1 {
		level = 1
	}

	for _, arg := range rawArgs {
		switch {
		case arg == "-v" || arg == "--verbose":
			level++
		case strings.HasPrefix(arg, "--verbose="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "--verbose=")); err == nil && n > level {
				level = n
			}
		case strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--"):
			short := strings.TrimPrefix(arg, "-")
			if short != "" && strings.Trim(short, "v") == "" {
				if n := len([]rune(short)); n > level {
					level = n
				}
			}
		}
	}

	return level
}

// extraVars collects all repeated extra-vars values from Options.
func extraVars(opts core.Options) (map[string]any, error) {
	vars := make(map[string]any)

	for _, o := range opts.Items() {
		if o.Key != "extra-vars" && o.Key != "e" {
			continue
		}

		var values []string
		switch v := o.Value.(type) {
		case string:
			values = append(values, v)
		case []string:
			values = append(values, v...)
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					values = append(values, s)
				}
			}
		}

		for _, value := range values {
			parsed, err := parseExtraVarsValue(value)
			if err != nil {
				return nil, err
			}
			for key, parsedValue := range parsed {
				vars[key] = parsedValue
			}
		}
	}

	return vars, nil
}

func parseExtraVarsValue(value string) (map[string]any, error) {
	trimmed := trimSpace(value)
	if trimmed == "" {
		return nil, nil
	}

	if strings.HasPrefix(trimmed, "@") {
		path := trimSpace(strings.TrimPrefix(trimmed, "@"))
		if path == "" {
			return nil, coreerr.E("parseExtraVarsValue", "extra vars file path required", nil)
		}

		data, err := coreio.Local.Read(path)
		if err != nil {
			return nil, coreerr.E("parseExtraVarsValue", "read extra vars file", err)
		}

		return parseExtraVarsValue(string(data))
	}

	if structured, ok := parseStructuredExtraVars(trimmed); ok {
		return structured, nil
	}

	if strings.Contains(trimmed, "=") {
		return parseKeyValueExtraVars(trimmed), nil
	}

	return nil, nil
}

func parseStructuredExtraVars(value string) (map[string]any, bool) {
	var parsed map[string]any
	if json.Valid([]byte(value)) {
		if err := yaml.Unmarshal([]byte(value), &parsed); err == nil && len(parsed) > 0 {
			return parsed, true
		}
	}
	if err := yaml.Unmarshal([]byte(value), &parsed); err != nil {
		return nil, false
	}
	if len(parsed) == 0 {
		return nil, false
	}
	return parsed, true
}

func parseKeyValueExtraVars(value string) map[string]any {
	vars := make(map[string]any)

	for _, pair := range split(value, ",") {
		parts := splitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := trimSpace(parts[0])
		if key == "" {
			continue
		}

		vars[key] = parts[1]
	}

	return vars
}

// resolveTestSSHKeyFile resolves the SSH key flag used by the ansible test subcommand.
func resolveTestSSHKeyFile(opts core.Options) string {
	if key := opts.String("key"); key != "" {
		return key
	}
	return opts.String("i")
}

func buildPlaybookCommandSettings(opts core.Options, rawArgs []string) (playbookCommandSettings, error) {
	positional := positionalArgs(opts)
	if len(positional) < 1 {
		return playbookCommandSettings{}, coreerr.E("buildPlaybookCommandSettings", "usage: ansible <playbook>", nil)
	}
	playbookPath := positional[0]

	if !pathIsAbs(playbookPath) {
		playbookPath = absPath(playbookPath)
	}

	if !coreio.Local.Exists(playbookPath) {
		return playbookCommandSettings{}, coreerr.E("buildPlaybookCommandSettings", sprintf("playbook not found: %s", playbookPath), nil)
	}

	vars, err := extraVars(opts)
	if err != nil {
		return playbookCommandSettings{}, coreerr.E("buildPlaybookCommandSettings", "parse extra vars", err)
	}

	return playbookCommandSettings{
		playbookPath: playbookPath,
		basePath:     pathDir(playbookPath),
		limit:        firstStringOption(opts, "limit", "l"),
		tags:         splitCommaSeparatedOption(firstStringOption(opts, "tags", "t")),
		skipTags:     splitCommaSeparatedOption(firstStringOption(opts, "skip-tags")),
		extraVars:    vars,
		verbose:      verbosityLevel(opts, rawArgs),
		checkMode:    opts.Bool("check"),
		diff:         opts.Bool("diff"),
	}, nil
}

func runPlaybookCommand(opts core.Options) core.Result {
	settings, err := buildPlaybookCommandSettings(opts, os.Args[1:])
	if err != nil {
		return core.Result{Value: err}
	}

	executor := ansible.NewExecutor(settings.basePath)
	defer executor.Close()

	executor.Limit = settings.limit
	executor.CheckMode = settings.checkMode
	executor.Diff = settings.diff
	executor.Verbose = settings.verbose
	executor.Tags = settings.tags
	executor.SkipTags = settings.skipTags

	for key, value := range settings.extraVars {
		executor.SetVar(key, value)
	}

	// Load inventory
	if invPath := firstStringOption(opts, "inventory", "i"); invPath != "" {
		if !pathIsAbs(invPath) {
			invPath = absPath(invPath)
		}

		if !coreio.Local.Exists(invPath) {
			return core.Result{Value: coreerr.E("runPlaybookCommand", sprintf("inventory not found: %s", invPath), nil)}
		}

		if coreio.Local.IsDir(invPath) {
			for _, name := range []string{"inventory.yml", "hosts.yml", "inventory.yaml", "hosts.yaml"} {
				p := joinPath(invPath, name)
				if coreio.Local.Exists(p) {
					invPath = p
					break
				}
			}
		}

		if err := executor.SetInventory(invPath); err != nil {
			return core.Result{Value: coreerr.E("runPlaybookCommand", "load inventory", err)}
		}
	}

	// Set up callbacks
	executor.OnPlayStart = func(play *ansible.Play) {
		print("")
		print("PLAY [%s]", play.Name)
		print("%s", repeat("*", 70))
	}

	executor.OnTaskStart = func(host string, task *ansible.Task) {
		taskName := task.Name
		if taskName == "" {
			taskName = task.Module
		}
		print("")
		print("TASK [%s]", taskName)
		if executor.Verbose > 0 {
			print("host: %s", host)
		}
	}

	executor.OnTaskEnd = func(host string, task *ansible.Task, result *ansible.TaskResult) {
		status := "ok"
		if result.Failed {
			status = "failed"
		} else if result.Skipped {
			status = "skipping"
		} else if result.Changed {
			status = "changed"
		}

		line := sprintf("%s: [%s]", status, host)
		if result.Msg != "" && executor.Verbose > 0 {
			line = sprintf("%s => %s", line, result.Msg)
		}
		if result.Duration > 0 && executor.Verbose > 1 {
			line = sprintf("%s (%s)", line, result.Duration.Round(time.Millisecond))
		}
		print("%s", line)

		if result.Failed && result.Stderr != "" {
			print("%s", result.Stderr)
		}

		if executor.Verbose > 1 {
			if result.Stdout != "" {
				print("stdout: %s", trimSpace(result.Stdout))
			}
		}

		if executor.Diff {
			if diff, ok := result.Data["diff"].(map[string]any); ok {
				print("diff:")
				if before, ok := diff["before"].(string); ok && before != "" {
					print("- %s", before)
				}
				if after, ok := diff["after"].(string); ok && after != "" {
					print("+ %s", after)
				}
			}
		}
	}

	executor.OnPlayEnd = func(play *ansible.Play) {
		print("")
	}

	// Run playbook
	ctx := context.Background()
	start := time.Now()

	print("Running playbook: %s", settings.playbookPath)

	if err := executor.Run(ctx, settings.playbookPath); err != nil {
		return core.Result{Value: coreerr.E("runPlaybookCommand", "playbook failed", err)}
	}

	print("")
	print("Playbook completed in %s", time.Since(start).Round(time.Millisecond))

	return core.Result{OK: true}
}

func runSSHTestCommand(opts core.Options) core.Result {
	positional := positionalArgs(opts)
	if len(positional) < 1 {
		return core.Result{Value: coreerr.E("runSSHTestCommand", "usage: ansible test <host>", nil)}
	}
	host := positional[0]

	print("Testing SSH connection to %s...", host)

	cfg := ansible.SSHConfig{
		Host:     host,
		Port:     opts.Int("port"),
		User:     firstStringOption(opts, "user", "u"),
		Password: opts.String("password"),
		KeyFile:  resolveTestSSHKeyFile(opts),
		Timeout:  30 * time.Second,
	}

	client, err := ansible.NewSSHClient(cfg)
	if err != nil {
		return core.Result{Value: coreerr.E("runSSHTestCommand", "create client", err)}
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection
	start := time.Now()
	if err := client.Connect(ctx); err != nil {
		return core.Result{Value: coreerr.E("runSSHTestCommand", "connect failed", err)}
	}
	connectTime := time.Since(start)

	print("Connected in %s", connectTime.Round(time.Millisecond))

	// Gather facts
	print("")
	print("Gathering facts...")

	stdout, _, _, _ := client.Run(ctx, "hostname -f 2>/dev/null || hostname")
	print("  Hostname: %s", trimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d'\"' -f2")
	if stdout != "" {
		print("  OS: %s", trimSpace(stdout))
	}

	stdout, _, _, _ = client.Run(ctx, "uname -r")
	print("  Kernel: %s", trimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "uname -m")
	print("  Architecture: %s", trimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "free -h | grep Mem | awk '{print $2}'")
	print("  Memory: %s", trimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "df -h / | tail -1 | awk '{print $2 \" total, \" $4 \" available\"}'")
	print("  Disk: %s", trimSpace(stdout))

	stdout, _, _, err = client.Run(ctx, "docker --version 2>/dev/null")
	if err == nil {
		print("  Docker: %s", trimSpace(stdout))
	} else {
		print("  Docker: not installed")
	}

	stdout, _, _, _ = client.Run(ctx, "docker ps 2>/dev/null | grep -q coolify && echo 'running' || echo 'not running'")
	if trimSpace(stdout) == "running" {
		print("  Coolify: running")
	} else {
		print("  Coolify: not installed")
	}

	print("")
	print("SSH test passed")

	return core.Result{OK: true}
}
