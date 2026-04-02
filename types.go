package ansible

import (
	"time"
)

// Playbook represents an Ansible playbook.
//
// Example:
//
//	playbook := Playbook{Plays: []Play{{Name: "Bootstrap", Hosts: "all"}}}
type Playbook struct {
	Plays []Play `yaml:",inline"`
}

// Play represents a single play in a playbook.
//
// Example:
//
//	play := Play{Name: "Configure web", Hosts: "webservers", Become: true}
type Play struct {
	Name           string            `yaml:"name"`
	Hosts          string            `yaml:"hosts"`
	ImportPlaybook string            `yaml:"import_playbook,omitempty"`
	Connection     string            `yaml:"connection,omitempty"`
	Become         bool              `yaml:"become,omitempty"`
	BecomeUser     string            `yaml:"become_user,omitempty"`
	GatherFacts    *bool             `yaml:"gather_facts,omitempty"`
	Vars           map[string]any    `yaml:"vars,omitempty"`
	VarsFiles      any               `yaml:"vars_files,omitempty"` // string or []string
	PreTasks       []Task            `yaml:"pre_tasks,omitempty"`
	Tasks          []Task            `yaml:"tasks,omitempty"`
	PostTasks      []Task            `yaml:"post_tasks,omitempty"`
	Roles          []RoleRef         `yaml:"roles,omitempty"`
	Handlers       []Task            `yaml:"handlers,omitempty"`
	Tags           []string          `yaml:"tags,omitempty"`
	Environment    map[string]string `yaml:"environment,omitempty"`
	Serial         any               `yaml:"serial,omitempty"` // int or string
	MaxFailPercent int               `yaml:"max_fail_percentage,omitempty"`
}

// RoleRef represents a role reference in a play.
//
// Example:
//
//	role := RoleRef{Role: "nginx", TasksFrom: "install.yml"}
type RoleRef struct {
	Role         string         `yaml:"role,omitempty"`
	Name         string         `yaml:"name,omitempty"` // Alternative to role
	TasksFrom    string         `yaml:"tasks_from,omitempty"`
	DefaultsFrom string         `yaml:"defaults_from,omitempty"`
	VarsFrom     string         `yaml:"vars_from,omitempty"`
	Vars         map[string]any `yaml:"vars,omitempty"`
	Apply        *TaskApply     `yaml:"apply,omitempty"`
	When         any            `yaml:"when,omitempty"`
	Tags         []string       `yaml:"tags,omitempty"`
}

// UnmarshalYAML handles both string and struct role refs.
//
// Example:
//
//	var ref RoleRef
//	_ = yaml.Unmarshal([]byte("common"), &ref)
func (r *RoleRef) UnmarshalYAML(unmarshal func(any) error) error {
	// Try string first
	var s string
	if err := unmarshal(&s); err == nil {
		r.Role = s
		return nil
	}

	// Try struct
	type rawRoleRef RoleRef
	var raw rawRoleRef
	if err := unmarshal(&raw); err != nil {
		return err
	}
	*r = RoleRef(raw)
	if r.Role == "" && r.Name != "" {
		r.Role = r.Name
	}
	return nil
}

// Task represents an Ansible task.
//
// Example:
//
//	task := Task{Name: "Install nginx", Module: "apt", Args: map[string]any{"name": "nginx"}}
type Task struct {
	Name         string            `yaml:"name,omitempty"`
	Module       string            `yaml:"-"` // Derived from the module key
	Args         map[string]any    `yaml:"-"` // Module arguments
	Register     string            `yaml:"register,omitempty"`
	When         any               `yaml:"when,omitempty"` // string or []string
	Loop         any               `yaml:"loop,omitempty"` // string or []any
	LoopControl  *LoopControl      `yaml:"loop_control,omitempty"`
	Vars         map[string]any    `yaml:"vars,omitempty"`
	Environment  map[string]string `yaml:"environment,omitempty"`
	ChangedWhen  any               `yaml:"changed_when,omitempty"`
	FailedWhen   any               `yaml:"failed_when,omitempty"`
	IgnoreErrors bool              `yaml:"ignore_errors,omitempty"`
	NoLog        bool              `yaml:"no_log,omitempty"`
	Become       *bool             `yaml:"become,omitempty"`
	BecomeUser   string            `yaml:"become_user,omitempty"`
	Delegate     string            `yaml:"delegate_to,omitempty"`
	RunOnce      bool              `yaml:"run_once,omitempty"`
	Tags         []string          `yaml:"tags,omitempty"`
	Block        []Task            `yaml:"block,omitempty"`
	Rescue       []Task            `yaml:"rescue,omitempty"`
	Always       []Task            `yaml:"always,omitempty"`
	Notify       any               `yaml:"notify,omitempty"` // string or []string
	Listen       any               `yaml:"listen,omitempty"` // string or []string
	Retries      int               `yaml:"retries,omitempty"`
	Delay        int               `yaml:"delay,omitempty"`
	Until        string            `yaml:"until,omitempty"`

	// Include/import directives
	IncludeTasks    string `yaml:"include_tasks,omitempty"`
	ImportTasks     string `yaml:"import_tasks,omitempty"`
	WithFile        any    `yaml:"with_file,omitempty"`
	WithFileGlob    any    `yaml:"with_fileglob,omitempty"`
	WithSequence    any    `yaml:"with_sequence,omitempty"`
	WithTogether    any    `yaml:"with_together,omitempty"`
	WithSubelements any    `yaml:"with_subelements,omitempty"`
	IncludeRole     *struct {
		Name         string         `yaml:"name"`
		TasksFrom    string         `yaml:"tasks_from,omitempty"`
		DefaultsFrom string         `yaml:"defaults_from,omitempty"`
		VarsFrom     string         `yaml:"vars_from,omitempty"`
		Vars         map[string]any `yaml:"vars,omitempty"`
		Apply        *TaskApply     `yaml:"apply,omitempty"`
	} `yaml:"include_role,omitempty"`
	ImportRole *struct {
		Name         string         `yaml:"name"`
		TasksFrom    string         `yaml:"tasks_from,omitempty"`
		DefaultsFrom string         `yaml:"defaults_from,omitempty"`
		VarsFrom     string         `yaml:"vars_from,omitempty"`
		Vars         map[string]any `yaml:"vars,omitempty"`
		Apply        *TaskApply     `yaml:"apply,omitempty"`
	} `yaml:"import_role,omitempty"`

	// Raw YAML for module extraction
	raw map[string]any
}

// LoopControl controls loop behavior.
//
// Example:
//
//	loop := LoopControl{LoopVar: "item", IndexVar: "idx"}
type LoopControl struct {
	LoopVar  string `yaml:"loop_var,omitempty"`
	IndexVar string `yaml:"index_var,omitempty"`
	Label    string `yaml:"label,omitempty"`
	Pause    int    `yaml:"pause,omitempty"`
	Extended bool   `yaml:"extended,omitempty"`
}

// TaskApply captures role-level task defaults from include_role/import_role.
//
// Example:
//
//	apply := TaskApply{Tags: []string{"deploy"}}
type TaskApply struct {
	Tags         []string          `yaml:"tags,omitempty"`
	Vars         map[string]any    `yaml:"vars,omitempty"`
	Environment  map[string]string `yaml:"environment,omitempty"`
	Become       *bool             `yaml:"become,omitempty"`
	BecomeUser   string            `yaml:"become_user,omitempty"`
	Delegate     string            `yaml:"delegate_to,omitempty"`
	RunOnce      bool              `yaml:"run_once,omitempty"`
	NoLog        bool              `yaml:"no_log,omitempty"`
	IgnoreErrors bool              `yaml:"ignore_errors,omitempty"`
}

// TaskResult holds the result of executing a task.
//
// Example:
//
//	result := TaskResult{Changed: true, Stdout: "ok"}
type TaskResult struct {
	Changed  bool           `json:"changed"`
	Failed   bool           `json:"failed"`
	Skipped  bool           `json:"skipped"`
	Msg      string         `json:"msg,omitempty"`
	Stdout   string         `json:"stdout,omitempty"`
	Stderr   string         `json:"stderr,omitempty"`
	RC       int            `json:"rc,omitempty"`
	Results  []TaskResult   `json:"results,omitempty"` // For loops
	Data     map[string]any `json:"data,omitempty"`    // Module-specific data
	Duration time.Duration  `json:"duration,omitempty"`
}

// Inventory represents Ansible inventory.
//
// Example:
//
//	inv := Inventory{All: &InventoryGroup{Hosts: map[string]*Host{"web1": {AnsibleHost: "10.0.0.1"}}}}
type Inventory struct {
	All *InventoryGroup `yaml:"all"`
}

// InventoryGroup represents a group in inventory.
//
// Example:
//
//	group := InventoryGroup{Hosts: map[string]*Host{"db1": {AnsibleHost: "10.0.1.10"}}}
type InventoryGroup struct {
	Hosts    map[string]*Host           `yaml:"hosts,omitempty"`
	Children map[string]*InventoryGroup `yaml:"children,omitempty"`
	Vars     map[string]any             `yaml:"vars,omitempty"`
}

// Host represents a host in inventory.
//
// Example:
//
//	host := Host{AnsibleHost: "192.168.1.10", AnsibleUser: "deploy"}
type Host struct {
	AnsibleHost              string `yaml:"ansible_host,omitempty"`
	AnsiblePort              int    `yaml:"ansible_port,omitempty"`
	AnsibleUser              string `yaml:"ansible_user,omitempty"`
	AnsiblePassword          string `yaml:"ansible_password,omitempty"`
	AnsibleSSHPrivateKeyFile string `yaml:"ansible_ssh_private_key_file,omitempty"`
	AnsibleConnection        string `yaml:"ansible_connection,omitempty"`
	AnsibleBecomePassword    string `yaml:"ansible_become_password,omitempty"`

	// Custom vars
	Vars map[string]any `yaml:",inline"`
}

// Facts holds gathered facts about a host.
//
// Example:
//
//	facts := Facts{Hostname: "web1", Distribution: "Ubuntu", Kernel: "Linux"}
type Facts struct {
	Hostname     string `json:"ansible_hostname"`
	FQDN         string `json:"ansible_fqdn"`
	OS           string `json:"ansible_os_family"`
	Distribution string `json:"ansible_distribution"`
	Version      string `json:"ansible_distribution_version"`
	Architecture string `json:"ansible_architecture"`
	Kernel       string `json:"ansible_kernel"`
	Memory       int64  `json:"ansible_memtotal_mb"`
	CPUs         int    `json:"ansible_processor_vcpus"`
	IPv4         string `json:"ansible_default_ipv4_address"`
}

// KnownModules lists the Ansible module names recognized by the parser.
//
// Example:
//
//	if slices.Contains(KnownModules, "ansible.builtin.command") {
//		// parser accepts command tasks
//	}
var KnownModules = []string{
	// Builtin
	"ansible.builtin.shell",
	"ansible.builtin.command",
	"ansible.builtin.raw",
	"ansible.builtin.script",
	"ansible.builtin.copy",
	"ansible.builtin.template",
	"ansible.builtin.file",
	"ansible.builtin.lineinfile",
	"ansible.builtin.blockinfile",
	"ansible.builtin.stat",
	"ansible.builtin.slurp",
	"ansible.builtin.fetch",
	"ansible.builtin.get_url",
	"ansible.builtin.uri",
	"ansible.builtin.apt",
	"ansible.builtin.apt_key",
	"ansible.builtin.apt_repository",
	"ansible.builtin.yum",
	"ansible.builtin.dnf",
	"ansible.builtin.package",
	"ansible.builtin.pip",
	"ansible.builtin.service",
	"ansible.builtin.systemd",
	"ansible.builtin.user",
	"ansible.builtin.group",
	"ansible.builtin.cron",
	"ansible.builtin.git",
	"ansible.builtin.unarchive",
	"ansible.builtin.archive",
	"ansible.builtin.debug",
	"ansible.builtin.fail",
	"ansible.builtin.assert",
	"ansible.builtin.pause",
	"ansible.builtin.wait_for",
	"ansible.builtin.set_fact",
	"ansible.builtin.include_vars",
	"ansible.builtin.add_host",
	"ansible.builtin.group_by",
	"ansible.builtin.meta",
	"ansible.builtin.setup",
	"community.general.ufw",
	"ansible.posix.authorized_key",
	"ansible.builtin.docker_compose",
	"community.docker.docker_compose",
	"community.docker.docker_compose_v2",

	// Short forms (legacy)
	"shell",
	"command",
	"raw",
	"script",
	"copy",
	"template",
	"file",
	"lineinfile",
	"blockinfile",
	"stat",
	"slurp",
	"fetch",
	"get_url",
	"uri",
	"apt",
	"apt_key",
	"apt_repository",
	"yum",
	"dnf",
	"package",
	"pip",
	"service",
	"systemd",
	"user",
	"group",
	"cron",
	"git",
	"unarchive",
	"archive",
	"debug",
	"fail",
	"assert",
	"pause",
	"wait_for",
	"set_fact",
	"include_vars",
	"add_host",
	"group_by",
	"meta",
	"setup",
	"hostname",
	"sysctl",
	"reboot",
	"authorized_key",
	"ufw",
	"docker_compose",
	"docker_compose_v2",
}

// ModuleAliases maps accepted short-form module names to their canonical
// fully-qualified collection names.
var ModuleAliases = map[string]string{
	"authorized_key":                 "ansible.posix.authorized_key",
	"ufw":                            "community.general.ufw",
	"docker_compose":                 "community.docker.docker_compose",
	"docker_compose_v2":              "community.docker.docker_compose_v2",
	"ansible.builtin.docker_compose": "community.docker.docker_compose",
}
