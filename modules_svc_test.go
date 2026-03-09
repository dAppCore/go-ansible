package ansible

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Step 1.3: service / systemd / apt / apt_key / apt_repository / package / pip module tests
// ============================================================

// --- service module ---

func TestModuleService_Good_Start(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl start nginx`, "Started", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "started",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl start nginx`))
	assert.Equal(t, 1, mock.commandCount())
}

func TestModuleService_Good_Stop(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl stop nginx`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "stopped",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl stop nginx`))
}

func TestModuleService_Good_Restart(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl restart docker`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":  "docker",
		"state": "restarted",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl restart docker`))
}

func TestModuleService_Good_Reload(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl reload nginx`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "reloaded",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl reload nginx`))
}

func TestModuleService_Good_Enable(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl enable nginx`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":    "nginx",
		"enabled": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl enable nginx`))
}

func TestModuleService_Good_Disable(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl disable nginx`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":    "nginx",
		"enabled": false,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl disable nginx`))
}

func TestModuleService_Good_StartAndEnable(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl start nginx`, "", "", 0)
	mock.expectCommand(`systemctl enable nginx`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":    "nginx",
		"state":   "started",
		"enabled": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, 2, mock.commandCount())
	assert.True(t, mock.hasExecuted(`systemctl start nginx`))
	assert.True(t, mock.hasExecuted(`systemctl enable nginx`))
}

func TestModuleService_Good_RestartAndDisable(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl restart sshd`, "", "", 0)
	mock.expectCommand(`systemctl disable sshd`, "", "", 0)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":    "sshd",
		"state":   "restarted",
		"enabled": false,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, 2, mock.commandCount())
	assert.True(t, mock.hasExecuted(`systemctl restart sshd`))
	assert.True(t, mock.hasExecuted(`systemctl disable sshd`))
}

func TestModuleService_Bad_MissingName(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleServiceWithClient(e, mock, map[string]any{
		"state": "started",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name required")
}

func TestModuleService_Good_NoStateNoEnabled(t *testing.T) {
	// When neither state nor enabled is provided, no commands run
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name": "nginx",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, 0, mock.commandCount())
}

func TestModuleService_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl start.*`, "", "Failed to start nginx.service", 1)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "started",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "Failed to start nginx.service")
	assert.Equal(t, 1, result.RC)
}

func TestModuleService_Good_FirstCommandFailsSkipsRest(t *testing.T) {
	// When state command fails, enable should not run
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl start`, "", "unit not found", 5)

	result, err := moduleServiceWithClient(e, mock, map[string]any{
		"name":    "nonexistent",
		"state":   "started",
		"enabled": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	// Only the start command should have been attempted
	assert.Equal(t, 1, mock.commandCount())
	assert.False(t, mock.hasExecuted(`systemctl enable`))
}

// --- systemd module ---

func TestModuleSystemd_Good_DaemonReloadThenStart(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl daemon-reload`, "", "", 0)
	mock.expectCommand(`systemctl start nginx`, "", "", 0)

	result, err := moduleSystemdWithClient(e, mock, map[string]any{
		"name":          "nginx",
		"state":         "started",
		"daemon_reload": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)

	// daemon-reload must run first, then start
	cmds := mock.executedCommands()
	require.GreaterOrEqual(t, len(cmds), 2)
	assert.Contains(t, cmds[0].Cmd, "daemon-reload")
	assert.Contains(t, cmds[1].Cmd, "systemctl start nginx")
}

func TestModuleSystemd_Good_DaemonReloadOnly(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl daemon-reload`, "", "", 0)

	result, err := moduleSystemdWithClient(e, mock, map[string]any{
		"name":          "nginx",
		"daemon_reload": true,
	})

	require.NoError(t, err)
	// daemon-reload runs, but no state/enabled means no further commands
	// Changed is false because moduleService returns Changed: len(cmds) > 0
	// and no cmds were built (no state, no enabled)
	assert.False(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl daemon-reload`))
}

func TestModuleSystemd_Good_DelegationToService(t *testing.T) {
	// Without daemon_reload, systemd delegates entirely to service
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl restart docker`, "", "", 0)

	result, err := moduleSystemdWithClient(e, mock, map[string]any{
		"name":  "docker",
		"state": "restarted",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl restart docker`))
	// No daemon-reload should have run
	assert.False(t, mock.hasExecuted(`daemon-reload`))
}

func TestModuleSystemd_Good_DaemonReloadWithEnable(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl daemon-reload`, "", "", 0)
	mock.expectCommand(`systemctl enable myapp`, "", "", 0)

	result, err := moduleSystemdWithClient(e, mock, map[string]any{
		"name":          "myapp",
		"enabled":       true,
		"daemon_reload": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`systemctl daemon-reload`))
	assert.True(t, mock.hasExecuted(`systemctl enable myapp`))
}

// --- apt module ---

func TestModuleApt_Good_InstallPresent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get install -y -qq nginx`, "installed", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nginx`))
}

func TestModuleApt_Good_InstallInstalled(t *testing.T) {
	// state=installed is an alias for present
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get install -y -qq curl`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":  "curl",
		"state": "installed",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`apt-get install -y -qq curl`))
}

func TestModuleApt_Good_RemoveAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get remove -y -qq nginx`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq nginx`))
}

func TestModuleApt_Good_RemoveRemoved(t *testing.T) {
	// state=removed is an alias for absent
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get remove -y -qq nginx`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "removed",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`apt-get remove -y -qq nginx`))
}

func TestModuleApt_Good_UpgradeLatest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get install -y -qq --only-upgrade nginx`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":  "nginx",
		"state": "latest",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --only-upgrade nginx`))
}

func TestModuleApt_Good_UpdateCacheBeforeInstall(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get update`, "", "", 0)
	mock.expectCommand(`apt-get install -y -qq nginx`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":         "nginx",
		"state":        "present",
		"update_cache": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)

	// apt-get update must run before install
	cmds := mock.executedCommands()
	require.GreaterOrEqual(t, len(cmds), 2)
	assert.Contains(t, cmds[0].Cmd, "apt-get update")
	assert.Contains(t, cmds[1].Cmd, "apt-get install")
}

func TestModuleApt_Good_UpdateCacheOnly(t *testing.T) {
	// update_cache with no name means update only, no install
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get update`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"update_cache": true,
	})

	require.NoError(t, err)
	// No package to install → not changed (cmd is empty)
	assert.False(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`apt-get update`))
}

func TestModuleApt_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get install`, "", "E: Unable to locate package badpkg", 100)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name":  "badpkg",
		"state": "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "Unable to locate package")
	assert.Equal(t, 100, result.RC)
}

func TestModuleApt_Good_DefaultStateIsPresent(t *testing.T) {
	// If no state is given, default is "present" (install)
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get install -y -qq vim`, "", "", 0)

	result, err := moduleAptWithClient(e, mock, map[string]any{
		"name": "vim",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`apt-get install -y -qq vim`))
}

// --- apt_key module ---

func TestModuleAptKey_Good_AddWithKeyring(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl -fsSL.*gpg --dearmor`, "", "", 0)

	result, err := moduleAptKeyWithClient(e, mock, map[string]any{
		"url":     "https://packages.example.com/key.gpg",
		"keyring": "/etc/apt/keyrings/example.gpg",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`curl -fsSL`))
	assert.True(t, mock.hasExecuted(`gpg --dearmor -o`))
	assert.True(t, mock.containsSubstring("/etc/apt/keyrings/example.gpg"))
}

func TestModuleAptKey_Good_AddWithoutKeyring(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl -fsSL.*apt-key add -`, "", "", 0)

	result, err := moduleAptKeyWithClient(e, mock, map[string]any{
		"url": "https://packages.example.com/key.gpg",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`apt-key add -`))
}

func TestModuleAptKey_Good_RemoveKey(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleAptKeyWithClient(e, mock, map[string]any{
		"keyring": "/etc/apt/keyrings/old.gpg",
		"state":   "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`rm -f`))
	assert.True(t, mock.containsSubstring("/etc/apt/keyrings/old.gpg"))
}

func TestModuleAptKey_Good_RemoveWithoutKeyring(t *testing.T) {
	// Absent with no keyring — still succeeds, just no rm command
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleAptKeyWithClient(e, mock, map[string]any{
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, 0, mock.commandCount())
}

func TestModuleAptKey_Bad_MissingURL(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAptKeyWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "url required")
}

func TestModuleAptKey_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "", "curl: (22) 404 Not Found", 22)

	result, err := moduleAptKeyWithClient(e, mock, map[string]any{
		"url":     "https://invalid.example.com/key.gpg",
		"keyring": "/etc/apt/keyrings/bad.gpg",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "404 Not Found")
}

// --- apt_repository module ---

func TestModuleAptRepository_Good_AddRepository(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo.*sources\.list\.d`, "", "", 0)
	mock.expectCommand(`apt-get update`, "", "", 0)

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo":     "deb https://packages.example.com/apt stable main",
		"filename": "example",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("/etc/apt/sources.list.d/example.list"))
}

func TestModuleAptRepository_Good_RemoveRepository(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo":     "deb https://packages.example.com/apt stable main",
		"filename": "example",
		"state":    "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`rm -f`))
	assert.True(t, mock.containsSubstring("example.list"))
}

func TestModuleAptRepository_Good_AddWithUpdateCache(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`apt-get update`, "", "", 0)

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo":         "deb https://ppa.example.com/repo main",
		"filename":     "ppa-example",
		"update_cache": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)

	// update_cache defaults to true, so apt-get update should run
	assert.True(t, mock.hasExecuted(`apt-get update`))
}

func TestModuleAptRepository_Good_AddWithoutUpdateCache(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo`, "", "", 0)

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo":         "deb https://ppa.example.com/repo main",
		"filename":     "no-update",
		"update_cache": false,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)

	// update_cache=false, so no apt-get update
	assert.False(t, mock.hasExecuted(`apt-get update`))
}

func TestModuleAptRepository_Good_CustomFilename(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`apt-get update`, "", "", 0)

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo":     "deb http://ppa.launchpad.net/test/ppa/ubuntu jammy main",
		"filename": "custom-ppa",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.containsSubstring("/etc/apt/sources.list.d/custom-ppa.list"))
}

func TestModuleAptRepository_Good_AutoGeneratedFilename(t *testing.T) {
	// When no filename is given, it auto-generates from the repo string
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`apt-get update`, "", "", 0)

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo": "deb https://example.com/repo main",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	// Filename should be derived from repo: spaces→dashes, slashes→dashes, colons removed
	assert.True(t, mock.containsSubstring("/etc/apt/sources.list.d/"))
}

func TestModuleAptRepository_Bad_MissingRepo(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"filename": "test",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo required")
}

func TestModuleAptRepository_Good_WriteFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo`, "", "permission denied", 1)

	result, err := moduleAptRepositoryWithClient(e, mock, map[string]any{
		"repo":     "deb https://example.com/repo main",
		"filename": "test",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "permission denied")
}

// --- package module ---

func TestModulePackage_Good_DetectAptAndDelegate(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// First command: which apt-get returns the path
	mock.expectCommand(`which apt-get`, "/usr/bin/apt-get", "", 0)
	// Second command: the actual apt install
	mock.expectCommand(`apt-get install -y -qq htop`, "", "", 0)

	result, err := modulePackageWithClient(e, mock, map[string]any{
		"name":  "htop",
		"state": "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`which apt-get`))
	assert.True(t, mock.hasExecuted(`apt-get install -y -qq htop`))
}

func TestModulePackage_Good_FallbackToApt(t *testing.T) {
	// When which returns nothing (no package manager found), still falls back to apt
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`which apt-get`, "", "", 1)
	mock.expectCommand(`apt-get install -y -qq vim`, "", "", 0)

	result, err := modulePackageWithClient(e, mock, map[string]any{
		"name":  "vim",
		"state": "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`apt-get install -y -qq vim`))
}

func TestModulePackage_Good_RemovePackage(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`which apt-get`, "/usr/bin/apt-get", "", 0)
	mock.expectCommand(`apt-get remove -y -qq nano`, "", "", 0)

	result, err := modulePackageWithClient(e, mock, map[string]any{
		"name":  "nano",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`apt-get remove -y -qq nano`))
}

// --- pip module ---

func TestModulePip_Good_InstallPresent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 install flask`, "Successfully installed", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":  "flask",
		"state": "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`pip3 install flask`))
}

func TestModulePip_Good_UninstallAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 uninstall -y flask`, "Successfully uninstalled", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":  "flask",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`pip3 uninstall -y flask`))
}

func TestModulePip_Good_UpgradeLatest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 install --upgrade flask`, "Successfully installed", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":  "flask",
		"state": "latest",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`pip3 install --upgrade flask`))
}

func TestModulePip_Good_CustomExecutable(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`/opt/venv/bin/pip install requests`, "", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":       "requests",
		"state":      "present",
		"executable": "/opt/venv/bin/pip",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`/opt/venv/bin/pip install requests`))
}

func TestModulePip_Good_DefaultStateIsPresent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 install django`, "", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name": "django",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`pip3 install django`))
}

func TestModulePip_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 install`, "", "ERROR: No matching distribution found", 1)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":  "nonexistent-pkg-xyz",
		"state": "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "No matching distribution found")
}

func TestModulePip_Good_InstalledAlias(t *testing.T) {
	// state=installed is an alias for present
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 install boto3`, "", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":  "boto3",
		"state": "installed",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`pip3 install boto3`))
}

func TestModulePip_Good_RemovedAlias(t *testing.T) {
	// state=removed is an alias for absent
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 uninstall -y boto3`, "", "", 0)

	result, err := modulePipWithClient(e, mock, map[string]any{
		"name":  "boto3",
		"state": "removed",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`pip3 uninstall -y boto3`))
}

// --- Cross-module dispatch tests ---

func TestExecuteModuleWithMock_Good_DispatchService(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl restart nginx`, "", "", 0)

	task := &Task{
		Module: "service",
		Args: map[string]any{
			"name":  "nginx",
			"state": "restarted",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`systemctl restart nginx`))
}

func TestExecuteModuleWithMock_Good_DispatchSystemd(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`systemctl daemon-reload`, "", "", 0)
	mock.expectCommand(`systemctl start myapp`, "", "", 0)

	task := &Task{
		Module: "ansible.builtin.systemd",
		Args: map[string]any{
			"name":          "myapp",
			"state":         "started",
			"daemon_reload": true,
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`systemctl daemon-reload`))
	assert.True(t, mock.hasExecuted(`systemctl start myapp`))
}

func TestExecuteModuleWithMock_Good_DispatchApt(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`apt-get install -y -qq nginx`, "", "", 0)

	task := &Task{
		Module: "apt",
		Args: map[string]any{
			"name":  "nginx",
			"state": "present",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`apt-get install`))
}

func TestExecuteModuleWithMock_Good_DispatchAptKey(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*gpg`, "", "", 0)

	task := &Task{
		Module: "apt_key",
		Args: map[string]any{
			"url":     "https://example.com/key.gpg",
			"keyring": "/etc/apt/keyrings/example.gpg",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchAptRepository(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`apt-get update`, "", "", 0)

	task := &Task{
		Module: "apt_repository",
		Args: map[string]any{
			"repo":     "deb https://example.com/repo main",
			"filename": "example",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchPackage(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`which apt-get`, "/usr/bin/apt-get", "", 0)
	mock.expectCommand(`apt-get install -y -qq git`, "", "", 0)

	task := &Task{
		Module: "package",
		Args: map[string]any{
			"name":  "git",
			"state": "present",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchPip(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`pip3 install ansible`, "", "", 0)

	task := &Task{
		Module: "pip",
		Args: map[string]any{
			"name":  "ansible",
			"state": "present",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`pip3 install ansible`))
}
