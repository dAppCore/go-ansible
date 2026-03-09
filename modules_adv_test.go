package ansible

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Step 1.4: user / group / cron / authorized_key / git /
//           unarchive / uri / ufw / docker_compose / blockinfile
//           advanced module tests
// ============================================================

// --- user module ---

func TestModuleUser_Good_CreateNewUser(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id deploy >/dev/null 2>&1`, "", "no such user", 1)
	mock.expectCommand(`useradd`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":        "deploy",
		"uid":         "1500",
		"group":       "www-data",
		"groups":      "docker,sudo",
		"home":        "/opt/deploy",
		"shell":       "/bin/bash",
		"create_home": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("useradd"))
	assert.True(t, mock.containsSubstring("-u 1500"))
	assert.True(t, mock.containsSubstring("-g www-data"))
	assert.True(t, mock.containsSubstring("-G docker,sudo"))
	assert.True(t, mock.containsSubstring("-d /opt/deploy"))
	assert.True(t, mock.containsSubstring("-s /bin/bash"))
	assert.True(t, mock.containsSubstring("-m"))
}

func TestModuleUser_Good_ModifyExistingUser(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// id returns success meaning user exists, so usermod branch is taken
	mock.expectCommand(`id deploy >/dev/null 2>&1 && usermod`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "deploy",
		"shell": "/bin/zsh",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("usermod"))
	assert.True(t, mock.containsSubstring("-s /bin/zsh"))
}

func TestModuleUser_Good_RemoveUser(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`userdel -r deploy`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "deploy",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`userdel -r deploy`))
}

func TestModuleUser_Good_SystemUser(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id|useradd`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":        "prometheus",
		"system":      true,
		"create_home": false,
		"shell":       "/usr/sbin/nologin",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	// system flag adds -r
	assert.True(t, mock.containsSubstring("-r"))
	assert.True(t, mock.containsSubstring("-s /usr/sbin/nologin"))
	// create_home=false means -m should NOT be present
	// Actually, looking at the production code: getBoolArg(args, "create_home", true) — default is true
	// We set it to false explicitly, so -m should NOT appear
	cmd := mock.lastCommand()
	assert.NotContains(t, cmd.Cmd, " -m ")
}

func TestModuleUser_Good_NoOptsUsesSimpleForm(t *testing.T) {
	// When no options are provided, uses the simple "id || useradd" form
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id testuser >/dev/null 2>&1 || useradd testuser`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":        "testuser",
		"create_home": false,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestModuleUser_Bad_MissingName(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUserWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name required")
}

func TestModuleUser_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id|useradd|usermod`, "", "useradd: Permission denied", 1)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "deploy",
		"shell": "/bin/bash",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "Permission denied")
}

// --- group module ---

func TestModuleGroup_Good_CreateNewGroup(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// getent fails → groupadd runs
	mock.expectCommand(`getent group appgroup`, "", "", 1)
	mock.expectCommand(`groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "appgroup",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("groupadd"))
	assert.True(t, mock.containsSubstring("appgroup"))
}

func TestModuleGroup_Good_GroupAlreadyExists(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// getent succeeds → groupadd skipped (|| short-circuits)
	mock.expectCommand(`getent group docker >/dev/null 2>&1 || groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "docker",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestModuleGroup_Good_RemoveGroup(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`groupdel oldgroup`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":  "oldgroup",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`groupdel oldgroup`))
}

func TestModuleGroup_Good_SystemGroup(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":   "prometheus",
		"system": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("-r"))
}

func TestModuleGroup_Good_CustomGID(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "custom",
		"gid":  "5000",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("-g 5000"))
}

func TestModuleGroup_Bad_MissingName(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGroupWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name required")
}

func TestModuleGroup_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "groupadd: Permission denied", 1)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "failgroup",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

// --- cron module ---

func TestModuleCron_Good_AddCronJob(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root`, "", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name": "backup",
		"job":  "/usr/local/bin/backup.sh",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	// Default schedule is * * * * *
	assert.True(t, mock.containsSubstring("* * * * *"))
	assert.True(t, mock.containsSubstring("/usr/local/bin/backup.sh"))
	assert.True(t, mock.containsSubstring("# backup"))
}

func TestModuleCron_Good_RemoveCronJob(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root -l`, "* * * * * /bin/backup # backup\n", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name":  "backup",
		"job":   "/bin/backup",
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.containsSubstring("grep -v"))
}

func TestModuleCron_Good_CustomSchedule(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root`, "", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name":    "nightly-backup",
		"job":     "/opt/scripts/backup.sh",
		"minute":  "30",
		"hour":    "2",
		"day":     "1",
		"month":   "6",
		"weekday": "0",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("30 2 1 6 0"))
	assert.True(t, mock.containsSubstring("/opt/scripts/backup.sh"))
}

func TestModuleCron_Good_CustomUser(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u www-data`, "", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name":   "cache-clear",
		"job":    "php artisan cache:clear",
		"user":   "www-data",
		"minute": "0",
		"hour":   "*/4",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("crontab -u www-data"))
	assert.True(t, mock.containsSubstring("0 */4 * * *"))
}

func TestModuleCron_Good_AbsentWithNoName(t *testing.T) {
	// Absent with no name — changed but no grep command
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	// No commands should have run since name is empty
	assert.Equal(t, 0, mock.commandCount())
}

// --- authorized_key module ---

func TestModuleAuthorizedKey_Good_AddKey(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`grep -qF`, "", "", 1) // key not found, will be appended
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
		"key":  testKey,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("mkdir -p"))
	assert.True(t, mock.containsSubstring("chmod 700"))
	assert.True(t, mock.containsSubstring("authorized_keys"))
}

func TestModuleAuthorizedKey_Good_RemoveKey(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`sed -i`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user":  "deploy",
		"key":   testKey,
		"state": "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`sed -i`))
	assert.True(t, mock.containsSubstring("authorized_keys"))
}

func TestModuleAuthorizedKey_Good_KeyAlreadyExists(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`mkdir -p`, "", "", 0)
	// grep succeeds: key already present, || short-circuits, echo not needed
	mock.expectCommand(`grep -qF.*echo`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
		"key":  testKey,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestModuleAuthorizedKey_Good_RootUserFallback(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... admin@host"
	// getent returns empty — falls back to /root for root user
	mock.expectCommand(`getent passwd root`, "", "", 0)
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`grep -qF.*echo`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "root",
		"key":  testKey,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	// Should use /root/.ssh/authorized_keys
	assert.True(t, mock.containsSubstring("/root/.ssh"))
}

func TestModuleAuthorizedKey_Bad_MissingUserAndKey(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user and key required")
}

func TestModuleAuthorizedKey_Bad_MissingKey(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user and key required")
}

func TestModuleAuthorizedKey_Bad_MissingUser(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT...",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user and key required")
}

// --- git module ---

func TestModuleGit_Good_FreshClone(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// .git does not exist → fresh clone
	mock.expectCommand(`git clone`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
		"dest": "/opt/app",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`git clone`))
	assert.True(t, mock.containsSubstring("https://github.com/example/app.git"))
	assert.True(t, mock.containsSubstring("/opt/app"))
	// Default version is HEAD
	assert.True(t, mock.containsSubstring("git checkout"))
}

func TestModuleGit_Good_UpdateExisting(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// .git exists → fetch + checkout
	mock.addFile("/opt/app/.git", []byte("gitdir"))
	mock.expectCommand(`git fetch --all && git checkout`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
		"dest": "/opt/app",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`git fetch --all`))
	assert.True(t, mock.containsSubstring("git checkout --force"))
	// Should NOT contain git clone
	assert.False(t, mock.containsSubstring("git clone"))
}

func TestModuleGit_Good_CustomVersion(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`git clone`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo":    "https://github.com/example/app.git",
		"dest":    "/opt/app",
		"version": "v2.1.0",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("v2.1.0"))
}

func TestModuleGit_Good_UpdateWithBranch(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/srv/myapp/.git", []byte("gitdir"))
	mock.expectCommand(`git fetch --all && git checkout`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo":    "git@github.com:org/repo.git",
		"dest":    "/srv/myapp",
		"version": "develop",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.containsSubstring("develop"))
}

func TestModuleGit_Bad_MissingRepoAndDest(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo and dest required")
}

func TestModuleGit_Bad_MissingRepo(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{
		"dest": "/opt/app",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo and dest required")
}

func TestModuleGit_Bad_MissingDest(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo and dest required")
}

func TestModuleGit_Good_CloneFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`git clone`, "", "fatal: repository not found", 128)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/nonexistent.git",
		"dest": "/opt/app",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "repository not found")
}

// --- unarchive module ---

func TestModuleUnarchive_Good_ExtractTarGzLocal(t *testing.T) {
	// Create a temporary "archive" file
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "package.tar.gz")
	require.NoError(t, os.WriteFile(archivePath, []byte("fake-archive-content"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xzf`, "", "", 0)
	mock.expectCommand(`rm -f`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":  archivePath,
		"dest": "/opt/app",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	// Should have uploaded the file
	assert.Equal(t, 1, mock.uploadCount())
	assert.True(t, mock.containsSubstring("tar -xzf"))
	assert.True(t, mock.containsSubstring("/opt/app"))
}

func TestModuleUnarchive_Good_ExtractZipLocal(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "release.zip")
	require.NoError(t, os.WriteFile(archivePath, []byte("fake-zip-content"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`unzip -o`, "", "", 0)
	mock.expectCommand(`rm -f`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":  archivePath,
		"dest": "/opt/releases",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, 1, mock.uploadCount())
	assert.True(t, mock.containsSubstring("unzip -o"))
}

func TestModuleUnarchive_Good_RemoteSource(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xzf`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":        "/tmp/remote-archive.tar.gz",
		"dest":       "/opt/app",
		"remote_src": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	// No upload should happen for remote sources
	assert.Equal(t, 0, mock.uploadCount())
	assert.True(t, mock.containsSubstring("tar -xzf"))
}

func TestModuleUnarchive_Good_TarXz(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xJf`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":        "/tmp/archive.tar.xz",
		"dest":       "/opt/extract",
		"remote_src": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.containsSubstring("tar -xJf"))
}

func TestModuleUnarchive_Good_TarBz2(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xjf`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":        "/tmp/archive.tar.bz2",
		"dest":       "/opt/extract",
		"remote_src": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.containsSubstring("tar -xjf"))
}

func TestModuleUnarchive_Bad_MissingSrcAndDest(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModuleUnarchive_Bad_MissingSrc(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"dest": "/opt/app",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModuleUnarchive_Bad_LocalFileNotFound(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommand(`mkdir -p`, "", "", 0)

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/archive.tar.gz",
		"dest": "/opt/app",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read src")
}

// --- uri module ---

func TestModuleURI_Good_GetRequestDefault(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*https://example.com/api/health`, "OK\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://example.com/api/health",
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed) // URI module does not set changed
	assert.Equal(t, 200, result.RC)
	assert.Equal(t, 200, result.Data["status"])
}

func TestModuleURI_Good_PostWithBodyAndHeaders(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	// Use a broad pattern since header order in map iteration is non-deterministic
	mock.expectCommand(`curl.*api\.example\.com`, "{\"id\":1}\n201", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":         "https://api.example.com/users",
		"method":      "POST",
		"body":        `{"name":"test"}`,
		"status_code": 201,
		"headers": map[string]any{
			"Content-Type":  "application/json",
			"Authorization": "Bearer token123",
		},
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, 201, result.RC)
	assert.True(t, mock.containsSubstring("-X POST"))
	assert.True(t, mock.containsSubstring("-d"))
	assert.True(t, mock.containsSubstring("Content-Type"))
	assert.True(t, mock.containsSubstring("Authorization"))
}

func TestModuleURI_Good_WrongStatusCode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "Not Found\n404", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://example.com/missing",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed) // Expected 200, got 404
	assert.Equal(t, 404, result.RC)
}

func TestModuleURI_Good_CurlCommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommandError(`curl`, assert.AnError)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://unreachable.example.com",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, assert.AnError.Error())
}

func TestModuleURI_Good_CustomExpectedStatus(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "\n204", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":         "https://api.example.com/resource/1",
		"method":      "DELETE",
		"status_code": 204,
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, 204, result.RC)
}

func TestModuleURI_Bad_MissingURL(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleURIWithClient(e, mock, map[string]any{
		"method": "GET",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "url required")
}

// --- ufw module ---

func TestModuleUFW_Good_AllowRuleWithPort(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw allow 443/tcp`, "Rule added", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule": "allow",
		"port": "443",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw allow 443/tcp`))
}

func TestModuleUFW_Good_EnableFirewall(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force enable`, "Firewall is active", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "enabled",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw --force enable`))
}

func TestModuleUFW_Good_DenyRuleWithProto(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw deny 53/udp`, "Rule added", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule":  "deny",
		"port":  "53",
		"proto": "udp",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw deny 53/udp`))
}

func TestModuleUFW_Good_ResetFirewall(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force reset`, "Resetting", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "reset",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw --force reset`))
}

func TestModuleUFW_Good_DisableFirewall(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw disable`, "Firewall stopped", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "disabled",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw disable`))
}

func TestModuleUFW_Good_ReloadFirewall(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw reload`, "Firewall reloaded", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "reloaded",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw reload`))
}

func TestModuleUFW_Good_LimitRule(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw limit 22/tcp`, "Rule added", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule": "limit",
		"port": "22",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw limit 22/tcp`))
}

func TestModuleUFW_Good_StateCommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force enable`, "", "ERROR: problem running ufw", 1)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "enabled",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

// --- docker_compose module ---

func TestModuleDockerCompose_Good_StatePresent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Creating container_1\nCreating container_2\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose up -d`))
	assert.True(t, mock.containsSubstring("/opt/myapp"))
}

func TestModuleDockerCompose_Good_StateAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose down`, "Removing container_1\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "absent",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose down`))
}

func TestModuleDockerCompose_Good_AlreadyUpToDate(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Container myapp-web-1  Up to date\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed) // "Up to date" in stdout → changed=false
	assert.False(t, result.Failed)
}

func TestModuleDockerCompose_Good_StateRestarted(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose restart`, "Restarting container_1\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/stack",
		"state":       "restarted",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose restart`))
}

func TestModuleDockerCompose_Bad_MissingProjectSrc(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "project_src required")
}

func TestModuleDockerCompose_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "", "Error response from daemon", 1)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/broken",
		"state":       "present",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "Error response from daemon")
}

func TestModuleDockerCompose_Good_DefaultStateIsPresent(t *testing.T) {
	// When no state is specified, default is "present"
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Starting\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/app",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose up -d`))
}

// --- Cross-module dispatch tests for advanced modules ---

func TestExecuteModuleWithMock_Good_DispatchUser(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id|useradd|usermod`, "", "", 0)

	task := &Task{
		Module: "user",
		Args: map[string]any{
			"name":  "appuser",
			"shell": "/bin/bash",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchGroup(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	task := &Task{
		Module: "group",
		Args: map[string]any{
			"name": "docker",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchCron(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab`, "", "", 0)

	task := &Task{
		Module: "cron",
		Args: map[string]any{
			"name": "logrotate",
			"job":  "/usr/sbin/logrotate",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchGit(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`git clone`, "", "", 0)

	task := &Task{
		Module: "git",
		Args: map[string]any{
			"repo": "https://github.com/org/repo.git",
			"dest": "/opt/repo",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}

func TestExecuteModuleWithMock_Good_DispatchURI(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "OK\n200", "", 0)

	task := &Task{
		Module: "uri",
		Args: map[string]any{
			"url": "https://example.com",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.False(t, result.Failed)
}

func TestExecuteModuleWithMock_Good_DispatchDockerCompose(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Creating\n", "", 0)

	task := &Task{
		Module: "ansible.builtin.docker_compose",
		Args: map[string]any{
			"project_src": "/opt/stack",
			"state":       "present",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
}
