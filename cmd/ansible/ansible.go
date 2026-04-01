package anscmd

import (
	"context"
	"time"

	"dappco.re/go/core"
	"dappco.re/go/core/ansible"
	coreio "dappco.re/go/core/io"
	coreerr "dappco.re/go/core/log"
)

// args extracts all positional arguments from Options.
func args(opts core.Options) []string {
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

func runAnsible(opts core.Options) core.Result {
	positional := args(opts)
	if len(positional) < 1 {
		return core.Result{Value: coreerr.E("runAnsible", "usage: ansible <playbook>", nil)}
	}
	playbookPath := positional[0]

	// Resolve playbook path
	if !pathIsAbs(playbookPath) {
		playbookPath = absPath(playbookPath)
	}

	if !coreio.Local.Exists(playbookPath) {
		return core.Result{Value: coreerr.E("runAnsible", sprintf("playbook not found: %s", playbookPath), nil)}
	}

	// Create executor
	basePath := pathDir(playbookPath)
	executor := ansible.NewExecutor(basePath)
	defer executor.Close()

	// Set options
	executor.Limit = opts.String("limit")
	executor.CheckMode = opts.Bool("check")
	executor.Diff = opts.Bool("diff")
	executor.Verbose = opts.Int("verbose")

	if tags := opts.String("tags"); tags != "" {
		executor.Tags = split(tags, ",")
	}
	if skipTags := opts.String("skip-tags"); skipTags != "" {
		executor.SkipTags = split(skipTags, ",")
	}

	// Parse extra vars
	if extraVars := opts.String("extra-vars"); extraVars != "" {
		for _, v := range split(extraVars, ",") {
			parts := splitN(v, "=", 2)
			if len(parts) == 2 {
				executor.SetVar(parts[0], parts[1])
			}
		}
	}

	// Load inventory
	if invPath := opts.String("inventory"); invPath != "" {
		if !pathIsAbs(invPath) {
			invPath = absPath(invPath)
		}

		if !coreio.Local.Exists(invPath) {
			return core.Result{Value: coreerr.E("runAnsible", sprintf("inventory not found: %s", invPath), nil)}
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
			return core.Result{Value: coreerr.E("runAnsible", "load inventory", err)}
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

	print("Running playbook: %s", playbookPath)

	if err := executor.Run(ctx, playbookPath); err != nil {
		return core.Result{Value: coreerr.E("runAnsible", "playbook failed", err)}
	}

	print("")
	print("Playbook completed in %s", time.Since(start).Round(time.Millisecond))

	return core.Result{OK: true}
}

func runAnsibleTest(opts core.Options) core.Result {
	positional := args(opts)
	if len(positional) < 1 {
		return core.Result{Value: coreerr.E("runAnsibleTest", "usage: ansible test <host>", nil)}
	}
	host := positional[0]

	print("Testing SSH connection to %s...", host)

	cfg := ansible.SSHConfig{
		Host:     host,
		Port:     opts.Int("port"),
		User:     opts.String("user"),
		Password: opts.String("password"),
		KeyFile:  opts.String("key"),
		Timeout:  30 * time.Second,
	}

	client, err := ansible.NewSSHClient(cfg)
	if err != nil {
		return core.Result{Value: coreerr.E("runAnsibleTest", "create client", err)}
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test connection
	start := time.Now()
	if err := client.Connect(ctx); err != nil {
		return core.Result{Value: coreerr.E("runAnsibleTest", "connect failed", err)}
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
