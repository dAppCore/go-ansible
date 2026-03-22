package anscmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"dappco.re/go/core"
	"dappco.re/go/core/ansible"
	coreio "dappco.re/go/core/io"
	coreerr "dappco.re/go/core/log"
)

// args extracts all positional arguments from Options.
func args(opts core.Options) []string {
	var out []string
	for _, o := range opts {
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
	if !filepath.IsAbs(playbookPath) {
		playbookPath, _ = filepath.Abs(playbookPath)
	}

	if !coreio.Local.Exists(playbookPath) {
		return core.Result{Value: coreerr.E("runAnsible", fmt.Sprintf("playbook not found: %s", playbookPath), nil)}
	}

	// Create executor
	basePath := filepath.Dir(playbookPath)
	executor := ansible.NewExecutor(basePath)
	defer executor.Close()

	// Set options
	executor.Limit = opts.String("limit")
	executor.CheckMode = opts.Bool("check")
	executor.Verbose = opts.Int("verbose")

	if tags := opts.String("tags"); tags != "" {
		executor.Tags = strings.Split(tags, ",")
	}
	if skipTags := opts.String("skip-tags"); skipTags != "" {
		executor.SkipTags = strings.Split(skipTags, ",")
	}

	// Parse extra vars
	if extraVars := opts.String("extra-vars"); extraVars != "" {
		for _, v := range strings.Split(extraVars, ",") {
			parts := strings.SplitN(v, "=", 2)
			if len(parts) == 2 {
				executor.SetVar(parts[0], parts[1])
			}
		}
	}

	// Load inventory
	if invPath := opts.String("inventory"); invPath != "" {
		if !filepath.IsAbs(invPath) {
			invPath, _ = filepath.Abs(invPath)
		}

		if !coreio.Local.Exists(invPath) {
			return core.Result{Value: coreerr.E("runAnsible", fmt.Sprintf("inventory not found: %s", invPath), nil)}
		}

		if coreio.Local.IsDir(invPath) {
			for _, name := range []string{"inventory.yml", "hosts.yml", "inventory.yaml", "hosts.yaml"} {
				p := filepath.Join(invPath, name)
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
		fmt.Printf("\nPLAY [%s]\n", play.Name)
		fmt.Println(strings.Repeat("*", 70))
	}

	executor.OnTaskStart = func(host string, task *ansible.Task) {
		taskName := task.Name
		if taskName == "" {
			taskName = task.Module
		}
		fmt.Printf("\nTASK [%s]\n", taskName)
		if executor.Verbose > 0 {
			fmt.Printf("host: %s\n", host)
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

		fmt.Printf("%s: [%s]", status, host)
		if result.Msg != "" && executor.Verbose > 0 {
			fmt.Printf(" => %s", result.Msg)
		}
		if result.Duration > 0 && executor.Verbose > 1 {
			fmt.Printf(" (%s)", result.Duration.Round(time.Millisecond))
		}
		fmt.Println()

		if result.Failed && result.Stderr != "" {
			fmt.Printf("%s\n", result.Stderr)
		}

		if executor.Verbose > 1 {
			if result.Stdout != "" {
				fmt.Printf("stdout: %s\n", strings.TrimSpace(result.Stdout))
			}
		}
	}

	executor.OnPlayEnd = func(play *ansible.Play) {
		fmt.Println()
	}

	// Run playbook
	ctx := context.Background()
	start := time.Now()

	fmt.Printf("Running playbook: %s\n", playbookPath)

	if err := executor.Run(ctx, playbookPath); err != nil {
		return core.Result{Value: coreerr.E("runAnsible", "playbook failed", err)}
	}

	fmt.Printf("\nPlaybook completed in %s\n", time.Since(start).Round(time.Millisecond))

	return core.Result{OK: true}
}

func runAnsibleTest(opts core.Options) core.Result {
	positional := args(opts)
	if len(positional) < 1 {
		return core.Result{Value: coreerr.E("runAnsibleTest", "usage: ansible test <host>", nil)}
	}
	host := positional[0]

	fmt.Printf("Testing SSH connection to %s...\n", host)

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

	fmt.Printf("Connected in %s\n", connectTime.Round(time.Millisecond))

	// Gather facts
	fmt.Println("\nGathering facts...")

	stdout, _, _, _ := client.Run(ctx, "hostname -f 2>/dev/null || hostname")
	fmt.Printf("  Hostname: %s\n", strings.TrimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d'\"' -f2")
	if stdout != "" {
		fmt.Printf("  OS: %s\n", strings.TrimSpace(stdout))
	}

	stdout, _, _, _ = client.Run(ctx, "uname -r")
	fmt.Printf("  Kernel: %s\n", strings.TrimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "uname -m")
	fmt.Printf("  Architecture: %s\n", strings.TrimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "free -h | grep Mem | awk '{print $2}'")
	fmt.Printf("  Memory: %s\n", strings.TrimSpace(stdout))

	stdout, _, _, _ = client.Run(ctx, "df -h / | tail -1 | awk '{print $2 \" total, \" $4 \" available\"}'")
	fmt.Printf("  Disk: %s\n", strings.TrimSpace(stdout))

	stdout, _, _, err = client.Run(ctx, "docker --version 2>/dev/null")
	if err == nil {
		fmt.Printf("  Docker: %s\n", strings.TrimSpace(stdout))
	} else {
		fmt.Printf("  Docker: not installed\n")
	}

	stdout, _, _, _ = client.Run(ctx, "docker ps 2>/dev/null | grep -q coolify && echo 'running' || echo 'not running'")
	if strings.TrimSpace(stdout) == "running" {
		fmt.Printf("  Coolify: running\n")
	} else {
		fmt.Printf("  Coolify: not installed\n")
	}

	fmt.Printf("\nSSH test passed\n")

	return core.Result{OK: true}
}
