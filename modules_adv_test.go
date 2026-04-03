package ansible

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Step 1.4: user / group / cron / authorized_key / git /
//           unarchive / uri / ufw / docker_compose / blockinfile
//           advanced module tests
// ============================================================

// --- user module ---

func TestModulesAdv_ModuleUser_Good_CreateNewUser(t *testing.T) {
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

func TestModulesAdv_ModuleUser_Good_ModifyExistingUser(t *testing.T) {
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

func TestModulesAdv_ModuleUser_Good_GroupListInput(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id deploy >/dev/null 2>&1`, "", "no such user", 1)
	mock.expectCommand(`useradd`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":   "deploy",
		"groups": []any{"docker", "sudo"},
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("-G docker,sudo"))
}

func TestModulesAdv_ModuleUser_Good_AppendSupplementaryGroups(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id deploy >/dev/null 2>&1 && usermod -a -G docker,sudo deploy \|\| useradd -G docker,sudo deploy`, "", "", 0)

	result, err := e.moduleUser(context.Background(), mock, map[string]any{
		"name":        "deploy",
		"groups":      []any{"docker", "sudo"},
		"append":      true,
		"create_home": false,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`usermod -a -G docker,sudo deploy`))
	assert.True(t, mock.hasExecuted(`useradd -G docker,sudo deploy`))
}

func TestModulesAdv_ModuleUser_Good_RemoveUser(t *testing.T) {
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

func TestModulesAdv_ModuleUser_Good_SystemUser(t *testing.T) {
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

func TestModulesAdv_ModuleUser_Good_NoOptsUsesSimpleForm(t *testing.T) {
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

func TestModulesAdv_ModuleUser_Bad_MissingName(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUserWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name required")
}

func TestModulesAdv_ModuleUser_Good_CommandFailure(t *testing.T) {
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

// --- hostname module ---

func TestModulesAdv_ModuleHostname_Good_IdempotentWhenAlreadySet(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^hostname$`, "web01\n", "", 0)

	result, err := e.moduleHostname(context.Background(), mock, map[string]any{
		"name": "web01",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.Equal(t, "hostname already set", result.Msg)
	assert.True(t, mock.hasExecuted(`^hostname$`))
	assert.False(t, mock.hasExecuted(`hostnamectl set-hostname`))
}

func TestModulesAdv_ModuleHostname_Good_ChangesWhenDifferent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^hostname$`, "old-host\n", "", 0)
	mock.expectCommand(`hostnamectl set-hostname "new-host" \|\| hostname "new-host"`, "", "", 0)
	mock.expectCommand(`sed -i 's/127\.0\.1\.1\..*/127.0.1.1\tnew-host/' /etc/hosts`, "", "", 0)

	result, err := e.moduleHostname(context.Background(), mock, map[string]any{
		"name": "new-host",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`^hostname$`))
	assert.True(t, mock.hasExecuted(`hostnamectl set-hostname`))
	assert.True(t, mock.hasExecuted(`sed -i`))
}

func TestModulesAdv_ModuleHostname_Good_HostnameAlias(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^hostname$`, "old-host\n", "", 0)
	mock.expectCommand(`hostnamectl set-hostname "alias-host" \|\| hostname "alias-host"`, "", "", 0)
	mock.expectCommand(`sed -i 's/127\.0\.1\.1\..*/127.0.1.1\talias-host/' /etc/hosts`, "", "", 0)

	result, err := e.moduleHostname(context.Background(), mock, map[string]any{
		"hostname": "alias-host",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`hostnamectl set-hostname`))
	assert.True(t, mock.hasExecuted(`sed -i`))
}

// --- group module ---

func TestModulesAdv_ModuleGroup_Good_CreateNewGroup(t *testing.T) {
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

func TestModulesAdv_ModuleGroup_Good_GroupAlreadyExists(t *testing.T) {
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

func TestModulesAdv_ModuleGroup_Good_RemoveGroup(t *testing.T) {
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

func TestModulesAdv_ModuleGroup_Good_SystemGroup(t *testing.T) {
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

func TestModulesAdv_ModuleGroup_Good_CustomGID(t *testing.T) {
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

func TestModulesAdv_ModuleGroup_Good_LocalGroup(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group localusers`, "", "", 1)
	mock.expectCommand(`lgroupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":  "localusers",
		"local": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("lgroupadd"))
}

func TestModulesAdv_ModuleGroup_Good_LocalGroupRemove(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`lgroupdel localusers`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":  "localusers",
		"state": "absent",
		"local": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.True(t, mock.hasExecuted(`lgroupdel localusers`))
}

func TestModulesAdv_ModuleGroup_Good_NonUniqueGID(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":       "sharedgid",
		"gid":        "5000",
		"non_unique": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("-g 5000"))
	assert.True(t, mock.containsSubstring("-o"))
}

func TestModulesAdv_ModuleGroup_Bad_MissingName(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGroupWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name required")
}

func TestModulesAdv_ModuleGroup_Good_CommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "groupadd: Permission denied", 1)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "failgroup",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

// --- cron module ---

func TestModulesAdv_ModuleCron_Good_AddCronJob(t *testing.T) {
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

func TestModulesAdv_ModuleCron_Good_RemoveCronJob(t *testing.T) {
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

func TestModulesAdv_ModuleCron_Good_CustomSchedule(t *testing.T) {
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

func TestModulesAdv_ModuleCron_Good_CustomUser(t *testing.T) {
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

func TestModulesAdv_ModuleCron_Good_DisabledJobCommentsEntry(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root`, "", "", 0)

	result, err := e.moduleCron(context.Background(), mock, map[string]any{
		"name":     "backup",
		"job":      "/usr/local/bin/backup.sh",
		"minute":   "15",
		"hour":     "1",
		"disabled": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring(`# 15 1 * * * /usr/local/bin/backup.sh # backup`))
}

func TestModulesAdv_ModuleCron_Good_AbsentWithNoName(t *testing.T) {
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

func TestModulesAdv_ModuleAuthorizedKey_Good_AddKey(t *testing.T) {
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

func TestModulesAdv_ModuleAuthorizedKey_Good_ShortKeyDoesNotPanic(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA short@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`grep -qF.*echo`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
		"key":  testKey,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`grep -qF`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_RemoveKey(t *testing.T) {
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

func TestModulesAdv_ModuleAuthorizedKey_Good_KeyAlreadyExists(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.addFile("/home/deploy/.ssh/authorized_keys", []byte(testKey+"\n"))
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)

	result, err := e.moduleAuthorizedKey(context.Background(), mock, map[string]any{
		"user": "deploy",
		"key":  testKey,
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Contains(t, result.Msg, "already up to date")
}

func TestModulesAdv_ModuleAuthorizedKey_Good_ExclusiveRewritesFile(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"

	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`printf '%s\\n'`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := e.moduleAuthorizedKey(context.Background(), mock, map[string]any{
		"user":      "deploy",
		"key":       testKey,
		"exclusive": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`printf '%s\\n'`))
	assert.False(t, mock.hasExecuted(`grep -qF`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_CustomPath(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`mkdir -p "/srv/keys"`, "", "", 0)
	mock.expectCommand(`grep -qF`, "", "", 1)
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
		"key":  testKey,
		"path": "/srv/keys/deploy_keys",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("/srv/keys/deploy_keys"))
	assert.True(t, mock.hasExecuted(`mkdir -p "/srv/keys"`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_ManageDirDisabled(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`grep -qF`, "", "", 1)
	mock.expectCommand(`echo`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user":       "deploy",
		"key":        testKey,
		"manage_dir": false,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.False(t, mock.hasExecuted(`mkdir -p`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_RootUserFallback(t *testing.T) {
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

func TestModulesAdv_ModuleAuthorizedKey_Bad_MissingUserAndKey(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user and key required")
}

func TestModulesAdv_ModuleAuthorizedKey_Bad_MissingKey(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user and key required")
}

func TestModulesAdv_ModuleAuthorizedKey_Bad_MissingUser(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT...",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user and key required")
}

// --- git module ---

func TestModulesAdv_ModuleGit_Good_FreshClone(t *testing.T) {
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

func TestModulesAdv_ModuleGit_Good_UpdateExisting(t *testing.T) {
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

func TestModulesAdv_ModuleGit_Good_CustomVersion(t *testing.T) {
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

func TestModulesAdv_ModuleGit_Good_UpdateWithBranch(t *testing.T) {
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

func TestModulesAdv_ModuleGit_Bad_MissingRepoAndDest(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo and dest required")
}

func TestModulesAdv_ModuleGit_Bad_MissingRepo(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{
		"dest": "/opt/app",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo and dest required")
}

func TestModulesAdv_ModuleGit_Bad_MissingDest(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "repo and dest required")
}

func TestModulesAdv_ModuleGit_Good_CloneFailure(t *testing.T) {
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

func TestModulesAdv_ModuleUnarchive_Good_ExtractTarGzLocal(t *testing.T) {
	// Create a temporary "archive" file
	tmpDir := t.TempDir()
	archivePath := joinPath(tmpDir, "package.tar.gz")
	require.NoError(t, writeTestFile(archivePath, []byte("fake-archive-content"), 0644))

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

func TestModulesAdv_ModuleUnarchive_Good_ExtractZipLocal(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := joinPath(tmpDir, "release.zip")
	require.NoError(t, writeTestFile(archivePath, []byte("fake-zip-content"), 0644))

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

func TestModulesAdv_ModuleUnarchive_Good_RemoteSource(t *testing.T) {
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

func TestModulesAdv_ModuleUnarchive_Good_TarXz(t *testing.T) {
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

func TestModulesAdv_ModuleUnarchive_Good_TarBz2(t *testing.T) {
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

func TestModulesAdv_ModuleUnarchive_Bad_MissingSrcAndDest(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModulesAdv_ModuleUnarchive_Bad_MissingSrc(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"dest": "/opt/app",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "src and dest required")
}

func TestModulesAdv_ModuleUnarchive_Bad_LocalFileNotFound(t *testing.T) {
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

// --- pause module ---

func TestModulesAdv_ModulePause_Good_WaitsForSeconds(t *testing.T) {
	e := NewExecutor("/tmp")

	start := time.Now()
	result, err := e.modulePause(context.Background(), map[string]any{
		"seconds": 1,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Changed)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
}

func TestModulesAdv_ModulePause_Good_PromptReturnsImmediatelyWithoutTTY(t *testing.T) {
	e := NewExecutor("/tmp")

	start := time.Now()
	result, err := e.modulePause(context.Background(), map[string]any{
		"prompt": "Press enter to continue",
		"echo":   false,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Changed)
	assert.Equal(t, "Press enter to continue", result.Msg)
	assert.Less(t, elapsed, 250*time.Millisecond)
}

// --- wait_for module ---

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPathPresent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/ready", []byte("ok"))

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path": "/tmp/ready",
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPathAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/vanish", []byte("ok"))

	go func() {
		time.Sleep(150 * time.Millisecond)
		mock.mu.Lock()
		delete(mock.files, "/tmp/vanish")
		mock.mu.Unlock()
	}()

	start := time.Now()
	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path":    "/tmp/vanish",
		"state":   "absent",
		"timeout": 2,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPathRegexMatch(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/config", []byte("ready=false\n"))

	go func() {
		time.Sleep(150 * time.Millisecond)
		mock.mu.Lock()
		mock.files["/tmp/config"] = []byte("ready=true\n")
		mock.mu.Unlock()
	}()

	start := time.Now()
	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path":         "/tmp/config",
		"search_regex": "ready=true",
		"timeout":      2,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.GreaterOrEqual(t, elapsed, 150*time.Millisecond)
}

func TestModulesAdv_ModuleWaitFor_Good_HonoursInitialDelay(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/delayed", []byte("ready=false\n"))

	go func() {
		time.Sleep(150 * time.Millisecond)
		mock.mu.Lock()
		mock.files["/tmp/delayed"] = []byte("ready=true\n")
		mock.mu.Unlock()
	}()

	start := time.Now()
	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path":    "/tmp/delayed",
		"delay":   1,
		"timeout": 2,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.GreaterOrEqual(t, elapsed, 1*time.Second)
}

func TestModulesAdv_ModuleWaitFor_Bad_CustomTimeoutMessage(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/config", []byte("ready=false\n"))

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path":         "/tmp/config",
		"search_regex": "ready=true",
		"timeout":      0,
		"msg":          "service never became ready",
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Failed)
	assert.Equal(t, "service never became ready", result.Msg)
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPortAbsent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 2 bash -c 'until ! nc -z 127.0.0.1 8080; do sleep 1; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "absent",
		"timeout": 2,
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.True(t, mock.hasExecuted(`until ! nc -z 127.0.0.1 8080`))
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPortStopped(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 2 bash -c 'until ! nc -z 127.0.0.1 8080; do sleep 1; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "stopped",
		"timeout": 2,
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.True(t, mock.hasExecuted(`until ! nc -z 127.0.0.1 8080`))
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPortDrained(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 2 bash -c 'until ! ss -Htan state established`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "drained",
		"timeout": 2,
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.True(t, mock.hasExecuted(`ss -Htan state established`))
}

func TestModulesAdv_ModuleWaitFor_Good_AcceptsStringNumericArgs(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 0 bash -c 'until ! nc -z 127.0.0.1 8080; do sleep 1; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    "8080",
		"state":   "stopped",
		"timeout": "0",
	})

	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.Failed)
	assert.False(t, result.Changed)
	assert.True(t, mock.hasExecuted(`until ! nc -z 127.0.0.1 8080`))
}

// --- include_vars module ---

func TestModulesAdv_ModuleIncludeVars_Good_LoadSingleFile(t *testing.T) {
	dir := t.TempDir()
	varsPath := joinPath(dir, "vars.yml")
	require.NoError(t, writeTestFile(varsPath, []byte("app_name: demo\napp_port: 8080\nnested:\n  enabled: true\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"file": varsPath,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Contains(t, result.Msg, varsPath)
	require.NotNil(t, result.Data)
	require.Contains(t, result.Data, "ansible_included_var_files")
	assert.Equal(t, []string{varsPath}, result.Data["ansible_included_var_files"])
	assert.Equal(t, "demo", e.vars["app_name"])
	assert.Equal(t, 8080, e.vars["app_port"])

	nested, ok := e.vars["nested"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, nested["enabled"])
}

func TestModulesAdv_ModuleIncludeVars_Good_LoadJSONFileByDefault(t *testing.T) {
	dir := t.TempDir()
	varsPath := joinPath(dir, "vars.json")
	require.NoError(t, writeTestFile(varsPath, []byte(`{"app_name":"demo","app_port":8080}`), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"file": varsPath,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, "demo", e.vars["app_name"])
	assert.Equal(t, 8080, e.vars["app_port"])
}

func TestModulesAdv_ModuleIncludeVars_Good_CustomExtensionsFilter(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "01-ignored.yml"), []byte("ignored_value: false\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "02-selected.vars"), []byte("selected_value: included\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":        dir,
		"extensions": []any{"vars"},
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "included", e.vars["selected_value"])
	_, hasIgnored := e.vars["ignored_value"]
	assert.False(t, hasIgnored)
	assert.Contains(t, result.Msg, joinPath(dir, "02-selected.vars"))
	assert.NotContains(t, result.Msg, joinPath(dir, "01-ignored.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_LoadDirectoryWithMerge(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "01-base.yml"), []byte("app_name: demo\nnested:\n  a: 1\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "02-override.yaml"), []byte("app_port: 8080\nnested:\n  b: 2\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":            dir,
		"hash_behaviour": "merge",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Contains(t, result.Msg, joinPath(dir, "01-base.yml"))
	assert.Contains(t, result.Msg, joinPath(dir, "02-override.yaml"))
	require.NotNil(t, result.Data)
	assert.Equal(t, []string{
		joinPath(dir, "01-base.yml"),
		joinPath(dir, "02-override.yaml"),
	}, result.Data["ansible_included_var_files"])
	assert.Equal(t, "demo", e.vars["app_name"])
	assert.Equal(t, 8080, e.vars["app_port"])

	nested, ok := e.vars["nested"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 1, nested["a"])
	assert.Equal(t, 2, nested["b"])
}

func TestModulesAdv_ModuleIncludeVars_Good_ResolvesRelativePathsAgainstBasePath(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "vars.yml"), []byte("app_name: demo\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "vars", "01-extra.yaml"), []byte("app_port: 8080\n"), 0644))

	e := NewExecutor(dir)

	result, err := e.moduleIncludeVars(map[string]any{
		"file": "vars.yml",
		"dir":  "vars",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Contains(t, result.Msg, "vars.yml")
	assert.Contains(t, result.Msg, joinPath(dir, "vars", "01-extra.yaml"))
	assert.Equal(t, "demo", e.vars["app_name"])
	assert.Equal(t, 8080, e.vars["app_port"])
}

func TestModulesAdv_ModuleIncludeVars_Good_RecursesIntoNestedDirectories(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "01-root.yml"), []byte("root_value: root\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "nested", "02-child.yaml"), []byte("child_value: child\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "nested", "deep", "03-grandchild.yml"), []byte("grandchild_value: grandchild\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir": dir,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "root", e.vars["root_value"])
	assert.Equal(t, "child", e.vars["child_value"])
	assert.Equal(t, "grandchild", e.vars["grandchild_value"])
	assert.Contains(t, result.Msg, joinPath(dir, "01-root.yml"))
	assert.Contains(t, result.Msg, joinPath(dir, "nested", "02-child.yaml"))
	assert.Contains(t, result.Msg, joinPath(dir, "nested", "deep", "03-grandchild.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_RespectsDepthLimit(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "01-root.yml"), []byte("root_value: root\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "nested", "02-child.yaml"), []byte("child_value: child\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "nested", "deep", "03-grandchild.yml"), []byte("grandchild_value: grandchild\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":   dir,
		"depth": 1,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "root", e.vars["root_value"])
	assert.Equal(t, "child", e.vars["child_value"])
	_, hasGrandchild := e.vars["grandchild_value"]
	assert.False(t, hasGrandchild)
	assert.NotContains(t, result.Msg, joinPath(dir, "nested", "deep", "03-grandchild.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_FiltersFilesMatching(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "01-base.yml"), []byte("base_value: base\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "02-extra.yaml"), []byte("extra_value: extra\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "notes.txt"), []byte("ignored: true\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":            dir,
		"files_matching": `^02-.*\.ya?ml$`,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "extra", e.vars["extra_value"])
	_, hasBase := e.vars["base_value"]
	assert.False(t, hasBase)
	assert.Contains(t, result.Msg, joinPath(dir, "02-extra.yaml"))
	assert.NotContains(t, result.Msg, joinPath(dir, "01-base.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_IgnoresNamedFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(joinPath(dir, "01-base.yml"), []byte("base_value: base\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "02-skip.yml"), []byte("skip_value: skipped\n"), 0644))
	require.NoError(t, writeTestFile(joinPath(dir, "nested", "02-skip.yml"), []byte("nested_skip_value: skipped\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":          dir,
		"ignore_files": []any{"02-skip.yml"},
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "base", e.vars["base_value"])
	_, hasSkip := e.vars["skip_value"]
	assert.False(t, hasSkip)
	_, hasNestedSkip := e.vars["nested_skip_value"]
	assert.False(t, hasNestedSkip)
	assert.Contains(t, result.Msg, joinPath(dir, "01-base.yml"))
	assert.NotContains(t, result.Msg, joinPath(dir, "02-skip.yml"))
	assert.NotContains(t, result.Msg, joinPath(dir, "nested", "02-skip.yml"))
}

// --- sysctl module ---

func TestModulesAdv_ModuleSysctl_Good_ReloadsAfterPersisting(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sysctl -w net.ipv4.ip_forward=1`, "", "", 0)
	mock.expectCommand(`grep -q .*net.ipv4.ip_forward`, "", "", 0)
	mock.expectCommand(`sysctl -p`, "", "", 0)

	result, err := e.moduleSysctl(context.Background(), mock, map[string]any{
		"name":   "net.ipv4.ip_forward",
		"value":  "1",
		"reload": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`sysctl -w net.ipv4.ip_forward=1`))
	assert.True(t, mock.hasExecuted(`sysctl -p`))
}

func TestModulesAdv_ModuleSysctl_Good_UsesCustomSysctlFile(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sed -i '/net\\.ipv4\\.ip_forward/d' .*custom\.conf`, "", "", 0)

	result, err := e.moduleSysctl(context.Background(), mock, map[string]any{
		"name":        "net.ipv4.ip_forward",
		"state":       "absent",
		"sysctl_file": "/etc/sysctl.d/custom.conf",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`sed -i '/net\\.ipv4\\.ip_forward/d' .*custom\.conf`))
}

// --- uri module ---

func TestModulesAdv_ModuleURI_Good_GetRequestDefault(t *testing.T) {
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

func TestModulesAdv_ModuleURI_Good_PostWithBodyAndHeaders(t *testing.T) {
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

func TestModulesAdv_ModuleURI_Good_FormURLEncodedBody(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*form\.example\.com`, "created\n201", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":         "https://form.example.com/submit",
		"method":      "POST",
		"body_format": "form-urlencoded",
		"body": map[string]any{
			"name":  "Alice Example",
			"scope": []any{"read", "write"},
		},
		"status_code": 201,
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, 201, result.RC)
	assert.True(t, mock.containsSubstring(`-d "name=Alice+Example&scope=read&scope=write"`))
	assert.True(t, mock.containsSubstring("Content-Type: application/x-www-form-urlencoded"))
}

func TestModulesAdv_ModuleURI_Good_WrongStatusCode(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "Not Found\n404", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://example.com/missing",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed) // Expected 200, got 404
	assert.Equal(t, 404, result.RC)
}

func TestModulesAdv_ModuleURI_Good_CurlCommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommandError(`curl`, assert.AnError)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://unreachable.example.com",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, assert.AnError.Error())
}

func TestModulesAdv_ModuleURI_Good_CustomExpectedStatus(t *testing.T) {
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

func TestModulesAdv_ModuleURI_Good_ReturnContent(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "{\"ok\":true}\n200", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":            "https://example.com/api/status",
		"return_content": true,
		"status_code":    200,
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "{\"ok\":true}", result.Data["content"])
	assert.Equal(t, 200, result.Data["status"])
}

func TestModulesAdv_ModuleURI_Good_WritesResponseToDest(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "{\"ok\":true}\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":  "https://example.com/api/status",
		"dest": "/tmp/api-status.json",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	require.NotNil(t, result.Data)
	assert.Equal(t, "/tmp/api-status.json", result.Data["dest"])

	content, err := mock.Download(context.Background(), "/tmp/api-status.json")
	require.NoError(t, err)
	assert.Equal(t, []byte("{\"ok\":true}"), content)
}

func TestModulesAdv_ModuleURI_Good_JSONBodyFormat(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "{\"created\":true}\n201", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":         "https://api.example.com/users",
		"method":      "POST",
		"body_format": "json",
		"body": map[string]any{
			"name": "test",
		},
		"status_code": 201,
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, 201, result.RC)
	assert.True(t, mock.containsSubstring(`-d "{\"name\":\"test\"}"`))
	assert.True(t, mock.containsSubstring("Content-Type: application/json"))
}

func TestModulesAdv_ModuleURI_Good_TimeoutAndInsecureSkipVerify(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "OK\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":            "https://insecure.example.com/health",
		"timeout":        15,
		"validate_certs": false,
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, 200, result.RC)
	assert.True(t, mock.containsSubstring("-k"))
	assert.True(t, mock.containsSubstring("--max-time 15"))
}

func TestModulesAdv_ModuleURI_Good_MultipleExpectedStatuses(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "\n202", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":         "https://example.com/jobs/123",
		"status_code": []any{200, 202, 204},
	})

	require.NoError(t, err)
	assert.False(t, result.Failed)
	assert.Equal(t, 202, result.RC)
	assert.Equal(t, 202, result.Data["status"])
}

func TestModulesAdv_ModuleURI_Bad_MissingURL(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleURIWithClient(e, mock, map[string]any{
		"method": "GET",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "url required")
}

// --- ufw module ---

func TestModulesAdv_ModuleUFW_Good_AllowRuleWithPort(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_EnableFirewall(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_DenyRuleWithProto(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_ResetFirewall(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_DisableFirewall(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_ReloadFirewall(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_LimitRule(t *testing.T) {
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

func TestModulesAdv_ModuleUFW_Good_DeleteRule(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw delete allow 443/tcp`, "Rule deleted", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule":   "allow",
		"port":   "443",
		"delete": true,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw delete allow 443/tcp`))
}

func TestModulesAdv_ModuleUFW_Good_LoggingMode(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`ufw logging high`, "Logging enabled\n", "", 0)

	task := &Task{
		Module: "community.general.ufw",
		Args: map[string]any{
			"logging": "high",
		},
	}

	result, err := e.executeModule(context.Background(), "host1", mock, task, &Play{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw logging high`))
}

func TestModulesAdv_ModuleUFW_Good_BuiltinAliasDispatch(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`ufw --force enable`, "", "", 0)

	task := &Task{
		Module: "ansible.builtin.ufw",
		Args: map[string]any{
			"state": "enabled",
		},
	}

	result, err := e.executeModule(context.Background(), "host1", mock, task, &Play{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`ufw --force enable`))
}

func TestModulesAdv_ModuleUFW_Good_StateCommandFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force enable`, "", "ERROR: problem running ufw", 1)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "enabled",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
}

// --- docker_compose module ---

func TestModulesAdv_ModuleDockerCompose_Good_StatePresent(t *testing.T) {
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

func TestModulesAdv_ModuleDockerCompose_Good_StateAbsent(t *testing.T) {
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

func TestModulesAdv_ModuleDockerCompose_Good_StateStopped(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose stop`, "Stopping container_1\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "stopped",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose stop`))
}

func TestModulesAdv_ModuleDockerCompose_Good_AlreadyUpToDate(t *testing.T) {
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

func TestModulesAdv_ModuleDockerCompose_Good_StateRestarted(t *testing.T) {
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

func TestModulesAdv_ModuleDockerCompose_Bad_MissingProjectSrc(t *testing.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"state": "present",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "project_src required")
}

func TestModulesAdv_ModuleDockerCompose_Good_CommandFailure(t *testing.T) {
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

func TestModulesAdv_ModuleDockerCompose_Good_DefaultStateIsPresent(t *testing.T) {
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

func TestModulesAdv_ModuleDockerCompose_Good_ProjectNameAndFiles(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d`, "Starting\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src":  "/opt/app",
		"project_name": "demo-app",
		"files":        []any{"docker-compose.yml", "docker-compose.prod.yml"},
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.containsSubstring("docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d"))
}

func TestModulesAdv_ModuleDockerCompose_Production_Good_AlreadyUpToDate(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`docker compose up -d`, "Container myapp-web-1  Up to date\n", "", 0)

	result, err := e.moduleDockerCompose(context.Background(), mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})

	require.NoError(t, err)
	assert.False(t, result.Changed)
	assert.False(t, result.Failed)
	assert.Equal(t, "Container myapp-web-1  Up to date\n", result.Stdout)
}

func TestModulesAdv_ModuleDockerCompose_Production_Good_ProjectNameAndFiles(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d`, "Starting\n", "", 0)

	result, err := e.moduleDockerCompose(context.Background(), mock, map[string]any{
		"project_src":  "/opt/app",
		"project_name": "demo-app",
		"files":        []any{"docker-compose.yml", "docker-compose.prod.yml"},
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d`))
}

// --- Cross-module dispatch tests for advanced modules ---

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchUser(t *testing.T) {
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

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchGroup(t *testing.T) {
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

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchCron(t *testing.T) {
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

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchGit(t *testing.T) {
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

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchURI(t *testing.T) {
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

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchDockerCompose(t *testing.T) {
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

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchDockerComposeV2(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Creating\n", "", 0)

	task := &Task{
		Module: "community.docker.docker_compose_v2",
		Args: map[string]any{
			"project_src": "/opt/stack",
			"state":       "present",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
}

func TestModulesAdv_ExecuteModule_Good_DispatchBuiltinDockerCompose(t *testing.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`docker compose up -d`, "Creating\n", "", 0)

	task := &Task{
		Module: "ansible.builtin.docker_compose",
		Args: map[string]any{
			"project_src": "/opt/stack",
			"state":       "present",
		},
	}

	result, err := e.executeModule(context.Background(), "host1", mock, task, &Play{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Changed)
	assert.False(t, result.Failed)
	assert.True(t, mock.hasExecuted(`docker compose up -d`))
}

// --- reboot module ---

func TestModulesAdv_ModuleReboot_Good_WaitsForTestCommand(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sleep 2 && shutdown -r now 'Maintenance window' &`, "", "", 0)
	mock.expectCommand(`sleep 3`, "", "", 0)
	mock.expectCommand(`whoami`, "root\n", "", 0)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{
		"msg":               "Maintenance window",
		"pre_reboot_delay":  2,
		"post_reboot_delay": 3,
		"reboot_timeout":    5,
		"test_command":      "whoami",
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "Reboot initiated", result.Msg)
	assert.Equal(t, 3, mock.commandCount())
	assert.True(t, mock.hasExecuted(`sleep 2 && shutdown -r now 'Maintenance window' &`))
	assert.True(t, mock.hasExecuted(`sleep 3`))
	assert.True(t, mock.hasExecuted(`whoami`))
}

func TestModulesAdv_ModuleReboot_Good_CustomRebootCommand(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sleep 1 && /sbin/reboot`, "", "", 0)
	mock.expectCommand(`whoami`, "root\n", "", 0)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{
		"reboot_command":   "/sbin/reboot",
		"pre_reboot_delay": 1,
		"reboot_timeout":   5,
	})

	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, "Reboot initiated", result.Msg)
	assert.Equal(t, 2, mock.commandCount())
	assert.True(t, mock.hasExecuted(`sleep 1 && /sbin/reboot`))
	assert.True(t, mock.hasExecuted(`whoami`))
}

func TestModulesAdv_ModuleReboot_Bad_TimesOutWaitingForTestCommand(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`shutdown -r now 'Reboot initiated by Ansible' &`, "", "", 0)
	mock.expectCommand(`whoami`, "", "host unreachable", 1)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{
		"reboot_timeout": 0,
		"test_command":   "whoami",
	})

	require.NoError(t, err)
	assert.True(t, result.Failed)
	assert.Contains(t, result.Msg, "timed out")
	assert.Equal(t, "host unreachable", result.Stderr)
	assert.Equal(t, 1, result.RC)
}

func TestModulesAdv_ModuleReboot_Bad_ReportsInitialShutdownFailure(t *testing.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`shutdown -r now 'Reboot initiated by Ansible' &`, "", "permission denied", 1)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{})

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Failed)
	assert.Equal(t, "permission denied", result.Msg)
	assert.Equal(t, 1, result.RC)
	assert.Equal(t, 1, mock.commandCount())
	assert.False(t, mock.hasExecuted(`whoami`))
}
