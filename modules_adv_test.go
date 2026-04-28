package ansible

import (
	"context"
	core "dappco.re/go"
	"time"
)

// ============================================================
// Step 1.4: user / group / cron / authorized_key / git /
//           unarchive / uri / ufw / docker_compose / blockinfile
//           advanced module tests
// ============================================================

// --- user module ---

func TestModulesAdv_ModuleUser_Good_CreateNewUser(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("useradd"))
	core.AssertTrue(t, mock.containsSubstring("-u 1500"))
	core.AssertTrue(t, mock.containsSubstring("-g www-data"))
	core.AssertTrue(t, mock.containsSubstring("-G docker,sudo"))
	core.AssertTrue(t, mock.containsSubstring("-d /opt/deploy"))
	core.AssertTrue(t, mock.containsSubstring("-s /bin/bash"))
	core.AssertTrue(t, mock.containsSubstring("-m"))
}

func TestModulesAdv_ModuleUser_Good_ModifyExistingUser(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// id returns success meaning user exists, so usermod branch is taken
	mock.expectCommand(`id deploy >/dev/null 2>&1 && usermod`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "deploy",
		"shell": "/bin/zsh",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("usermod"))
	core.AssertTrue(t, mock.containsSubstring("-s /bin/zsh"))
}

func TestModulesAdv_ModuleUser_Good_GroupListInput(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id deploy >/dev/null 2>&1`, "", "no such user", 1)
	mock.expectCommand(`useradd`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":   "deploy",
		"groups": []any{"docker", "sudo"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("-G docker,sudo"))
}

func TestModulesAdv_ModuleUser_Good_AppendSupplementaryGroups(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id deploy >/dev/null 2>&1 && usermod -a -G docker,sudo deploy \|\| useradd -G docker,sudo deploy`, "", "", 0)

	result, err := e.moduleUser(context.Background(), mock, map[string]any{
		"name":        "deploy",
		"groups":      []any{"docker", "sudo"},
		"append":      true,
		"create_home": false,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`usermod -a -G docker,sudo deploy`))
	core.AssertTrue(t, mock.hasExecuted(`useradd -G docker,sudo deploy`))
}

func TestModulesAdv_ModuleUser_Good_RemoveUser(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`userdel -r deploy`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "deploy",
		"state": "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`userdel -r deploy`))
}

func TestModulesAdv_ModuleUser_Good_SystemUser(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id|useradd`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":        "prometheus",
		"system":      true,
		"create_home": false,
		"shell":       "/usr/sbin/nologin",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	// system flag adds -r
	core.AssertTrue(t, mock.containsSubstring("-r"))
	core.AssertTrue(t, mock.containsSubstring("-s /usr/sbin/nologin"))
	// create_home=false means -m should NOT be present
	// Actually, looking at the production code: getBoolArg(args, "create_home", true) — default is true
	// We set it to false explicitly, so -m should NOT appear
	cmd := mock.lastCommand()
	core.AssertNotContains(t, cmd.Cmd, " -m ")
}

func TestModulesAdv_ModuleUser_Good_NoOptsUsesSimpleForm(t *core.T) {
	// When no options are provided, uses the simple "id || useradd" form
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id testuser >/dev/null 2>&1 || useradd testuser`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":        "testuser",
		"create_home": false,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesAdv_ModuleUser_Good_LocalModeUsesLocalCommands(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id localuser >/dev/null 2>&1 && lusermod`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":   "localuser",
		"local":  true,
		"shell":  "/bin/zsh",
		"home":   "/var/lib/localuser",
		"append": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("lusermod"))
	core.AssertTrue(t, mock.containsSubstring("luseradd"))
	core.AssertTrue(t, mock.containsSubstring("-s /bin/zsh"))
	core.AssertTrue(t, mock.containsSubstring("-d /var/lib/localuser"))
}

func TestModulesAdv_ModuleUser_Good_LocalModeRemovesLocalUser(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`luserdel -r localuser`, "", "", 0)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "localuser",
		"local": true,
		"state": "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`luserdel -r localuser`))
}

func TestModulesAdv_ModuleUser_Bad_MissingName(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUserWithClient(e, mock, map[string]any{
		"state": "present",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "name required")
}

func TestModulesAdv_ModuleUser_Good_CommandFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`id|useradd|usermod`, "", "useradd: Permission denied", 1)

	result, err := moduleUserWithClient(e, mock, map[string]any{
		"name":  "deploy",
		"shell": "/bin/bash",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "Permission denied")
}

// --- hostname module ---

func TestModulesAdv_ModuleHostname_Good_IdempotentWhenAlreadySet(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^hostname$`, "web01\n", "", 0)

	result, err := e.moduleHostname(context.Background(), mock, map[string]any{
		"name": "web01",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, "hostname already set", result.Msg)
	core.AssertTrue(t, mock.hasExecuted(`^hostname$`))
	core.AssertFalse(t, mock.hasExecuted(`hostnamectl set-hostname`))
}

func TestModulesAdv_ModuleHostname_Good_ChangesWhenDifferent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^hostname$`, "old-host\n", "", 0)
	mock.expectCommand(`hostnamectl set-hostname "new-host" \|\| hostname "new-host"`, "", "", 0)
	mock.expectCommand(`sed -i 's/127\.0\.1\.1\..*/127.0.1.1\tnew-host/' /etc/hosts`, "", "", 0)

	result, err := e.moduleHostname(context.Background(), mock, map[string]any{
		"name": "new-host",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`^hostname$`))
	core.AssertTrue(t, mock.hasExecuted(`hostnamectl set-hostname`))
	core.AssertTrue(t, mock.hasExecuted(`sed -i`))
}

func TestModulesAdv_ModuleHostname_Good_HostnameAlias(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^hostname$`, "old-host\n", "", 0)
	mock.expectCommand(`hostnamectl set-hostname "alias-host" \|\| hostname "alias-host"`, "", "", 0)
	mock.expectCommand(`sed -i 's/127\.0\.1\.1\..*/127.0.1.1\talias-host/' /etc/hosts`, "", "", 0)

	result, err := e.moduleHostname(context.Background(), mock, map[string]any{
		"hostname": "alias-host",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`hostnamectl set-hostname`))
	core.AssertTrue(t, mock.hasExecuted(`sed -i`))
}

// --- group module ---

func TestModulesAdv_ModuleGroup_Good_CreateNewGroup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// getent fails → groupadd runs
	mock.expectCommand(`getent group appgroup`, "", "", 1)
	mock.expectCommand(`groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "appgroup",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("groupadd"))
	core.AssertTrue(t, mock.containsSubstring("appgroup"))
}

func TestModulesAdv_ModuleGroup_Good_GroupAlreadyExists(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// getent succeeds → groupadd skipped (|| short-circuits)
	mock.expectCommand(`getent group docker >/dev/null 2>&1 || groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "docker",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesAdv_ModuleGroup_Good_RemoveGroup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`groupdel oldgroup`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":  "oldgroup",
		"state": "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`groupdel oldgroup`))
}

func TestModulesAdv_ModuleGroup_Good_SystemGroup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":   "prometheus",
		"system": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("-r"))
}

func TestModulesAdv_ModuleGroup_Good_CustomGID(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "custom",
		"gid":  "5000",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("-g 5000"))
}

func TestModulesAdv_ModuleGroup_Good_LocalGroup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group localusers`, "", "", 1)
	mock.expectCommand(`lgroupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":  "localusers",
		"local": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("lgroupadd"))
}

func TestModulesAdv_ModuleGroup_Good_LocalGroupRemove(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`lgroupdel localusers`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":  "localusers",
		"state": "absent",
		"local": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`lgroupdel localusers`))
}

func TestModulesAdv_ModuleGroup_Good_NonUniqueGID(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name":       "sharedgid",
		"gid":        "5000",
		"non_unique": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("-g 5000"))
	core.AssertTrue(t, mock.containsSubstring("-o"))
}

func TestModulesAdv_ModuleGroup_Bad_MissingName(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGroupWithClient(e, mock, map[string]any{
		"state": "present",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "name required")
}

func TestModulesAdv_ModuleGroup_Good_CommandFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "groupadd: Permission denied", 1)

	result, err := moduleGroupWithClient(e, mock, map[string]any{
		"name": "failgroup",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
}

// --- cron module ---

func TestModulesAdv_ModuleCron_Good_AddCronJob(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root`, "", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name": "backup",
		"job":  "/usr/local/bin/backup.sh",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	// Default schedule is * * * * *
	core.AssertTrue(t, mock.containsSubstring("* * * * *"))
	core.AssertTrue(t, mock.containsSubstring("/usr/local/bin/backup.sh"))
	core.AssertTrue(t, mock.containsSubstring("# backup"))
}

func TestModulesAdv_ModuleCron_Good_RemoveCronJob(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root -l`, "* * * * * /bin/backup # backup\n", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name":  "backup",
		"job":   "/bin/backup",
		"state": "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.containsSubstring("grep -v"))
}

func TestModulesAdv_ModuleCron_Good_CustomSchedule(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("30 2 1 6 0"))
	core.AssertTrue(t, mock.containsSubstring("/opt/scripts/backup.sh"))
}

func TestModulesAdv_ModuleCron_Good_CustomUser(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u www-data`, "", "", 0)

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"name":   "cache-clear",
		"job":    "php artisan cache:clear",
		"user":   "www-data",
		"minute": "0",
		"hour":   "*/4",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("crontab -u www-data"))
	core.AssertTrue(t, mock.containsSubstring("0 */4 * * *"))
}

func TestModulesAdv_ModuleCron_Good_SpecialTime(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`crontab -u root`, "", "", 0)

	result, err := e.moduleCron(context.Background(), mock, map[string]any{
		"name":         "daily-backup",
		"job":          "/usr/local/bin/backup.sh",
		"special_time": "daily",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring(`@daily /usr/local/bin/backup.sh # daily-backup`))
}

func TestModulesAdv_ModuleCron_Good_BackupCreatesBackupFile(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`crontab -u root`, "", "", 0)
	mock.expectCommand(`crontab -u root -l`, "0 0 * * * /usr/local/bin/backup.sh # daily-backup\n", "", 0)

	result, err := e.moduleCron(context.Background(), mock, map[string]any{
		"name":   "daily-backup",
		"job":    "/usr/local/bin/backup.sh",
		"minute": "0",
		"hour":   "1",
		"backup": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertNotNil(t, result.Data)
	backupPath, ok := result.Data["backup_file"].(string)
	core.RequireTrue(t, ok)
	core.AssertContains(t, backupPath, "/tmp/ansible-cron-root-daily-backup.")
	core.AssertEqual(t, 1, mock.uploadCount())
	lastUpload := mock.lastUpload()
	core.AssertNotNil(t, lastUpload)
	core.AssertEqual(t, backupPath, lastUpload.Remote)
	core.AssertEqual(t, []byte("0 0 * * * /usr/local/bin/backup.sh # daily-backup\n"), lastUpload.Content)
	core.AssertTrue(t, mock.containsSubstring("crontab -u root -l"))
	core.AssertTrue(t, mock.containsSubstring("crontab -u root"))
}

func TestModulesAdv_ModuleCron_Good_DisabledJobCommentsEntry(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`crontab -u root`, "", "", 0)

	result, err := e.moduleCron(context.Background(), mock, map[string]any{
		"name":     "backup",
		"job":      "/usr/local/bin/backup.sh",
		"minute":   "15",
		"hour":     "1",
		"disabled": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring(`# 15 1 * * * /usr/local/bin/backup.sh # backup`))
}

func TestModulesAdv_ModuleCron_Good_AbsentWithNoName(t *core.T) {
	// Absent with no name — changed but no grep command
	e, mock := newTestExecutorWithMock("host1")

	result, err := moduleCronWithClient(e, mock, map[string]any{
		"state": "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	// No commands should have run since name is empty
	core.AssertEqual(t, 0, mock.commandCount())
}

// --- authorized_key module ---

func TestModulesAdv_ModuleAuthorizedKey_Good_AddKey(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("mkdir -p"))
	core.AssertTrue(t, mock.containsSubstring("chmod 700"))
	core.AssertTrue(t, mock.containsSubstring("authorized_keys"))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_ShortKeyDoesNotPanic(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`grep -qF`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_RemoveKey(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`sed -i`, "", "", 0)

	result, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user":  "deploy",
		"key":   testKey,
		"state": "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`sed -i`))
	core.AssertTrue(t, mock.containsSubstring("authorized_keys"))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_KeyAlreadyExists(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"
	mock.addFile("/home/deploy/.ssh/authorized_keys", []byte(testKey+"\n"))
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)

	result, err := e.moduleAuthorizedKey(context.Background(), mock, map[string]any{
		"user": "deploy",
		"key":  testKey,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertContains(t, result.Msg, "already up to date")
}

func TestModulesAdv_ModuleAuthorizedKey_Good_RewritesKeyOptionsAndComment(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	testKey := "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA user@host"
	authPath := "/home/deploy/.ssh/authorized_keys"
	mock.addFile(authPath, []byte(testKey+"\n"))
	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)

	result, err := e.moduleAuthorizedKey(context.Background(), mock, map[string]any{
		"user":        "deploy",
		"key":         testKey,
		"key_options": "command=\"/usr/local/bin/backup-only\"",
		"comment":     "backup access",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`chmod 600`))

	content, err := mock.Download(context.Background(), authPath)
	core.RequireNoError(t, err)
	core.AssertContains(t, string(content), `command="/usr/local/bin/backup-only" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA backup access`)
	core.AssertNotContains(t, string(content), testKey)
}

func TestModulesAdv_ModuleAuthorizedKey_Good_ExclusiveRewritesFile(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	testKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT... user@host"

	mock.expectCommand(`getent passwd deploy`, "/home/deploy", "", 0)
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`chmod 600`, "", "", 0)

	result, err := e.moduleAuthorizedKey(context.Background(), mock, map[string]any{
		"user":      "deploy",
		"key":       testKey,
		"exclusive": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	content, err := mock.Download(context.Background(), "/home/deploy/.ssh/authorized_keys")
	core.RequireNoError(t, err)
	core.AssertEqual(t, testKey+"\n", string(content))
	core.AssertFalse(t, mock.hasExecuted(`grep -qF`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_CustomPath(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("/srv/keys/deploy_keys"))
	core.AssertTrue(t, mock.hasExecuted(`mkdir -p "/srv/keys"`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_ManageDirDisabled(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, mock.hasExecuted(`mkdir -p`))
}

func TestModulesAdv_ModuleAuthorizedKey_Good_RootUserFallback(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	// Should use /root/.ssh/authorized_keys
	core.AssertTrue(t, mock.containsSubstring("/root/.ssh"))
}

func TestModulesAdv_ModuleAuthorizedKey_Bad_MissingUserAndKey(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "user and key required")
}

func TestModulesAdv_ModuleAuthorizedKey_Bad_MissingKey(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"user": "deploy",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "user and key required")
}

func TestModulesAdv_ModuleAuthorizedKey_Bad_MissingUser(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleAuthorizedKeyWithClient(e, mock, map[string]any{
		"key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDcT...",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "user and key required")
}

// --- git module ---

func TestModulesAdv_ModuleGit_Good_FreshClone(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// .git does not exist → fresh clone
	mock.expectCommand(`git clone`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
		"dest": "/opt/app",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`git clone`))
	core.AssertTrue(t, mock.containsSubstring("https://github.com/example/app.git"))
	core.AssertTrue(t, mock.containsSubstring("/opt/app"))
	// Default version is HEAD
	core.AssertTrue(t, mock.containsSubstring("git checkout"))
}

func TestModulesAdv_ModuleGit_Good_UpdateExisting(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	// .git exists → fetch + checkout
	mock.addFile("/opt/app/.git", []byte("gitdir"))
	mock.expectCommand(`git fetch --all && git checkout`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
		"dest": "/opt/app",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`git fetch --all`))
	core.AssertTrue(t, mock.containsSubstring("git checkout --force"))
	// Should NOT contain git clone
	core.AssertFalse(t, mock.containsSubstring("git clone"))
}

func TestModulesAdv_ModuleGit_Good_CustomVersion(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`git clone`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo":    "https://github.com/example/app.git",
		"dest":    "/opt/app",
		"version": "v2.1.0",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("v2.1.0"))
}

func TestModulesAdv_ModuleGit_Good_UpdateWithBranch(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/srv/myapp/.git", []byte("gitdir"))
	mock.expectCommand(`git fetch --all && git checkout`, "", "", 0)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo":    "git@github.com:org/repo.git",
		"dest":    "/srv/myapp",
		"version": "develop",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.containsSubstring("develop"))
}

func TestModulesAdv_ModuleGit_Bad_MissingRepoAndDest(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "repo and dest required")
}

func TestModulesAdv_ModuleGit_Bad_MissingRepo(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{
		"dest": "/opt/app",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "repo and dest required")
}

func TestModulesAdv_ModuleGit_Bad_MissingDest(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/app.git",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "repo and dest required")
}

func TestModulesAdv_ModuleGit_Good_CloneFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`git clone`, "", "fatal: repository not found", 128)

	result, err := moduleGitWithClient(e, mock, map[string]any{
		"repo": "https://github.com/example/nonexistent.git",
		"dest": "/opt/app",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "repository not found")
}

// --- unarchive module ---

func TestModulesAdv_ModuleUnarchive_Good_ExtractTarGzLocal(t *core.T) {
	// Create a temporary "archive" file
	tmpDir := t.TempDir()
	archivePath := joinPath(tmpDir, "package.tar.gz")
	core.RequireNoError(t, writeTestFile(archivePath, []byte("fake-archive-content"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xzf`, "", "", 0)
	mock.expectCommand(`rm -f`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":  archivePath,
		"dest": "/opt/app",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	// Should have uploaded the file
	core.AssertEqual(t, 1, mock.uploadCount())
	core.AssertTrue(t, mock.containsSubstring("tar -xzf"))
	core.AssertTrue(t, mock.containsSubstring("/opt/app"))
}

func TestModulesAdv_ModuleUnarchive_Good_ExtractZipLocal(t *core.T) {
	tmpDir := t.TempDir()
	archivePath := joinPath(tmpDir, "release.zip")
	core.RequireNoError(t, writeTestFile(archivePath, []byte("fake-zip-content"), 0644))

	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`unzip -o`, "", "", 0)
	mock.expectCommand(`rm -f`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":  archivePath,
		"dest": "/opt/releases",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 1, mock.uploadCount())
	core.AssertTrue(t, mock.containsSubstring("unzip -o"))
}

func TestModulesAdv_ModuleUnarchive_Good_RemoteSource(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xzf`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":        "/tmp/remote-archive.tar.gz",
		"dest":       "/opt/app",
		"remote_src": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	// No upload should happen for remote sources
	core.AssertEqual(t, 0, mock.uploadCount())
	core.AssertTrue(t, mock.containsSubstring("tar -xzf"))
}

func TestModulesAdv_ModuleUnarchive_Good_TarXz(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xJf`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":        "/tmp/archive.tar.xz",
		"dest":       "/opt/extract",
		"remote_src": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.containsSubstring("tar -xJf"))
}

func TestModulesAdv_ModuleUnarchive_Good_TarBz2(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`mkdir -p`, "", "", 0)
	mock.expectCommand(`tar -xjf`, "", "", 0)

	result, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":        "/tmp/archive.tar.bz2",
		"dest":       "/opt/extract",
		"remote_src": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertTrue(t, mock.containsSubstring("tar -xjf"))
}

func TestModulesAdv_ModuleUnarchive_Bad_MissingSrcAndDest(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src and dest required")
}

func TestModulesAdv_ModuleUnarchive_Bad_MissingSrc(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"dest": "/opt/app",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "src and dest required")
}

func TestModulesAdv_ModuleUnarchive_Bad_LocalFileNotFound(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()
	mock.expectCommand(`mkdir -p`, "", "", 0)

	_, err := moduleUnarchiveWithClient(e, mock, map[string]any{
		"src":  "/nonexistent/archive.tar.gz",
		"dest": "/opt/app",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "read src")
}

// --- pause module ---

func TestModulesAdv_ModulePause_Good_WaitsForSeconds(t *core.T) {
	e := NewExecutor("/tmp")

	start := time.Now()
	result, err := e.modulePause(context.Background(), map[string]any{
		"seconds": 1,
	})
	elapsed := time.Since(start)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Changed)
	core.AssertGreaterOrEqual(t, elapsed, 900*time.Millisecond)
}

func TestModulesAdv_ModulePause_Good_PromptReturnsImmediatelyWithoutTTY(t *core.T) {
	e := NewExecutor("/tmp")

	start := time.Now()
	result, err := e.modulePause(context.Background(), map[string]any{
		"prompt": "Press enter to continue",
		"echo":   false,
	})
	elapsed := time.Since(start)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, "Press enter to continue", result.Msg)
	core.AssertLess(t, elapsed, 250*time.Millisecond)
}

// --- wait_for module ---

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPathPresent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/ready", []byte("ok"))

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path": "/tmp/ready",
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPathAbsent(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertGreaterOrEqual(t, elapsed, 150*time.Millisecond)
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPathRegexMatch(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertGreaterOrEqual(t, elapsed, 150*time.Millisecond)
}

func TestModulesAdv_ModuleWaitFor_Good_HonoursInitialDelay(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertGreaterOrEqual(t, elapsed, 1*time.Second)
}

func TestModulesAdv_ModuleWaitFor_Bad_CustomTimeoutMessage(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/config", []byte("ready=false\n"))

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path":         "/tmp/config",
		"search_regex": "ready=true",
		"timeout":      0,
		"msg":          "service never became ready",
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, "service never became ready", result.Msg)
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPortAbsent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 2 bash -c 'until ! nc -z 127.0.0.1 8080; do sleep 1; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "absent",
		"timeout": 2,
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`until ! nc -z 127.0.0.1 8080`))
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPortStopped(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 2 bash -c 'until ! nc -z 127.0.0.1 8080; do sleep 1; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "stopped",
		"timeout": 2,
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`until ! nc -z 127.0.0.1 8080`))
}

func TestModulesAdv_ModuleWaitFor_Good_WaitsForPortDrained(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 2 bash -c 'until ! ss -Htan state established`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "drained",
		"timeout": 2,
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`ss -Htan state established`))
}

func TestModulesAdv_ModuleWaitFor_Good_UsesCustomSleepInterval(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.addFile("/tmp/slow-ready", []byte("ready=false\n"))

	go func() {
		time.Sleep(150 * time.Millisecond)
		mock.mu.Lock()
		mock.files["/tmp/slow-ready"] = []byte("ready=true\n")
		mock.mu.Unlock()
	}()

	start := time.Now()
	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"path":         "/tmp/slow-ready",
		"search_regex": "ready=true",
		"sleep":        2,
		"timeout":      3,
	})
	elapsed := time.Since(start)

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertGreaterOrEqual(t, elapsed, 2*time.Second)
}

func TestModulesAdv_ModuleWaitFor_Good_UsesCustomSleepInPortLoop(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 5 bash -c 'until nc -z 127.0.0.1 8080; do sleep 3; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    8080,
		"state":   "started",
		"timeout": 5,
		"sleep":   3,
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`sleep 3`))
}

func TestModulesAdv_ModuleWaitFor_Good_AcceptsStringNumericArgs(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`timeout 0 bash -c 'until ! nc -z 127.0.0.1 8080; do sleep 1; done'`, "", "", 0)

	result, err := e.moduleWaitFor(context.Background(), mock, map[string]any{
		"host":    "127.0.0.1",
		"port":    "8080",
		"state":   "stopped",
		"timeout": "0",
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`until ! nc -z 127.0.0.1 8080`))
}

// --- wait_for_connection module ---

type deadlineAwareMockClient struct {
	*MockSSHClient
}

func (c *deadlineAwareMockClient) Run(ctx context.Context, cmd string) (string, string, int, error) {
	if _, ok := ctx.Deadline(); !ok {
		return "", "", 1, context.DeadlineExceeded
	}
	return c.MockSSHClient.Run(ctx, cmd)
}

func TestModulesAdv_ModuleWaitForConnection_Good_ReturnsWhenHostIsReachable(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`^true$`, "", "", 0)

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "wait_for_connection",
		Args: map[string]any{
			"timeout": 0,
		},
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, mock.hasExecuted(`^true$`))
}

func TestModulesAdv_ModuleWaitForConnection_Good_UsesConnectTimeout(t *core.T) {
	e := NewExecutor("/tmp")
	client := &deadlineAwareMockClient{MockSSHClient: NewMockSSHClient()}

	result, err := e.moduleWaitForConnection(context.Background(), client, map[string]any{
		"timeout":         0,
		"connect_timeout": 1,
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertTrue(t, client.hasExecuted(`^true$`))
}

func TestModulesAdv_ModuleWaitForConnection_Bad_ImmediateFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommandError(`^true$`, core.AnError)

	result, err := executeModuleWithMock(e, mock, "host1", &Task{
		Module: "ansible.builtin.wait_for_connection",
		Args: map[string]any{
			"timeout": 0,
		},
	})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, core.AnError.Error())
	core.AssertTrue(t, mock.hasExecuted(`^true$`))
}

// --- include_vars module ---

func TestModulesAdv_ModuleIncludeVars_Good_LoadSingleFile(t *core.T) {
	dir := t.TempDir()
	varsPath := joinPath(dir, "vars.yml")
	core.RequireNoError(t, writeTestFile(varsPath, []byte("app_name: demo\napp_port: 8080\nnested:\n  enabled: true\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"file": varsPath,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertContains(t, result.Msg, varsPath)
	core.AssertNotNil(t, result.Data)
	core.AssertContains(t, result.Data, "ansible_included_var_files")
	core.AssertEqual(t, []string{varsPath}, result.Data["ansible_included_var_files"])
	core.AssertEqual(t, "demo", e.vars["app_name"])
	core.AssertEqual(t, 8080, e.vars["app_port"])

	nested, ok := e.vars["nested"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, true, nested["enabled"])
}

func TestModulesAdv_ModuleIncludeVars_Good_LoadJSONFileByDefault(t *core.T) {
	dir := t.TempDir()
	varsPath := joinPath(dir, "vars.json")
	core.RequireNoError(t, writeTestFile(varsPath, []byte(`{"app_name":"demo","app_port":8080}`), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"file": varsPath,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "demo", e.vars["app_name"])
	core.AssertEqual(t, 8080, e.vars["app_port"])
}

func TestModulesAdv_ModuleIncludeVars_Good_CustomExtensionsFilter(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "01-ignored.yml"), []byte("ignored_value: false\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "02-selected.vars"), []byte("selected_value: included\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":        dir,
		"extensions": []any{"vars"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "included", e.vars["selected_value"])
	_, hasIgnored := e.vars["ignored_value"]
	core.AssertFalse(t, hasIgnored)
	core.AssertContains(t, result.Msg, joinPath(dir, "02-selected.vars"))
	core.AssertNotContains(t, result.Msg, joinPath(dir, "01-ignored.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_LoadExtensionlessFilesWhenRequested(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars"), []byte("app_name: demo\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "ignored.txt"), []byte("ignored_value: true\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":        dir,
		"extensions": []any{"", "yml", "yaml", "json"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "demo", e.vars["app_name"])
	_, hasIgnored := e.vars["ignored_value"]
	core.AssertFalse(t, hasIgnored)
	core.AssertContains(t, result.Msg, joinPath(dir, "vars"))
	core.AssertNotContains(t, result.Msg, joinPath(dir, "ignored.txt"))
}

func TestModulesAdv_ModuleIncludeVars_Good_LoadDirectoryWithMerge(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "01-base.yml"), []byte("app_name: demo\nnested:\n  a: 1\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "02-override.yaml"), []byte("app_port: 8080\nnested:\n  b: 2\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":            dir,
		"hash_behaviour": "merge",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertContains(t, result.Msg, joinPath(dir, "01-base.yml"))
	core.AssertContains(t, result.Msg, joinPath(dir, "02-override.yaml"))
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, []string{
		joinPath(dir, "01-base.yml"),
		joinPath(dir, "02-override.yaml"),
	}, result.Data["ansible_included_var_files"])
	core.AssertEqual(t, "demo", e.vars["app_name"])
	core.AssertEqual(t, 8080, e.vars["app_port"])

	nested, ok := e.vars["nested"].(map[string]any)
	core.RequireTrue(t, ok)
	core.AssertEqual(t, 1, nested["a"])
	core.AssertEqual(t, 2, nested["b"])
}

func TestModulesAdv_ModuleIncludeVars_Good_ResolvesRelativePathsAgainstBasePath(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars.yml"), []byte("app_name: demo\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "vars", "01-extra.yaml"), []byte("app_port: 8080\n"), 0644))

	e := NewExecutor(dir)

	result, err := e.moduleIncludeVars(map[string]any{
		"file": "vars.yml",
		"dir":  "vars",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertContains(t, result.Msg, "vars.yml")
	core.AssertContains(t, result.Msg, joinPath(dir, "vars", "01-extra.yaml"))
	core.AssertEqual(t, "demo", e.vars["app_name"])
	core.AssertEqual(t, 8080, e.vars["app_port"])
}

func TestModulesAdv_ModuleIncludeVars_Good_RecursesIntoNestedDirectories(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "01-root.yml"), []byte("root_value: root\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "nested", "02-child.yaml"), []byte("child_value: child\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "nested", "deep", "03-grandchild.yml"), []byte("grandchild_value: grandchild\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir": dir,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "root", e.vars["root_value"])
	core.AssertEqual(t, "child", e.vars["child_value"])
	core.AssertEqual(t, "grandchild", e.vars["grandchild_value"])
	core.AssertContains(t, result.Msg, joinPath(dir, "01-root.yml"))
	core.AssertContains(t, result.Msg, joinPath(dir, "nested", "02-child.yaml"))
	core.AssertContains(t, result.Msg, joinPath(dir, "nested", "deep", "03-grandchild.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_RespectsDepthLimit(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "01-root.yml"), []byte("root_value: root\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "nested", "02-child.yaml"), []byte("child_value: child\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "nested", "deep", "03-grandchild.yml"), []byte("grandchild_value: grandchild\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":   dir,
		"depth": 1,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "root", e.vars["root_value"])
	core.AssertEqual(t, "child", e.vars["child_value"])
	_, hasGrandchild := e.vars["grandchild_value"]
	core.AssertFalse(t, hasGrandchild)
	core.AssertNotContains(t, result.Msg, joinPath(dir, "nested", "deep", "03-grandchild.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_FiltersFilesMatching(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "01-base.yml"), []byte("base_value: base\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "02-extra.yaml"), []byte("extra_value: extra\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "notes.txt"), []byte("ignored: true\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":            dir,
		"files_matching": `^02-.*\.ya?ml$`,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "extra", e.vars["extra_value"])
	_, hasBase := e.vars["base_value"]
	core.AssertFalse(t, hasBase)
	core.AssertContains(t, result.Msg, joinPath(dir, "02-extra.yaml"))
	core.AssertNotContains(t, result.Msg, joinPath(dir, "01-base.yml"))
}

func TestModulesAdv_ModuleIncludeVars_Good_IgnoresNamedFiles(t *core.T) {
	dir := t.TempDir()
	core.RequireNoError(t, writeTestFile(joinPath(dir, "01-base.yml"), []byte("base_value: base\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "02-skip.yml"), []byte("skip_value: skipped\n"), 0644))
	core.RequireNoError(t, writeTestFile(joinPath(dir, "nested", "02-skip.yml"), []byte("nested_skip_value: skipped\n"), 0644))

	e := NewExecutor("/tmp")

	result, err := e.moduleIncludeVars(map[string]any{
		"dir":          dir,
		"ignore_files": []any{"02-skip.yml"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "base", e.vars["base_value"])
	_, hasSkip := e.vars["skip_value"]
	core.AssertFalse(t, hasSkip)
	_, hasNestedSkip := e.vars["nested_skip_value"]
	core.AssertFalse(t, hasNestedSkip)
	core.AssertContains(t, result.Msg, joinPath(dir, "01-base.yml"))
	core.AssertNotContains(t, result.Msg, joinPath(dir, "02-skip.yml"))
	core.AssertNotContains(t, result.Msg, joinPath(dir, "nested", "02-skip.yml"))
}

// --- sysctl module ---

func TestModulesAdv_ModuleSysctl_Good_ReloadsAfterPersisting(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sysctl -w net.ipv4.ip_forward=1`, "", "", 0)
	mock.expectCommand(`grep -q .*net.ipv4.ip_forward`, "", "", 0)
	mock.expectCommand(`sysctl -p`, "", "", 0)

	result, err := e.moduleSysctl(context.Background(), mock, map[string]any{
		"name":   "net.ipv4.ip_forward",
		"value":  "1",
		"reload": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`sysctl -w net.ipv4.ip_forward=1`))
	core.AssertTrue(t, mock.hasExecuted(`sysctl -p`))
}

func TestModulesAdv_ModuleSysctl_Good_IgnoreErrorsAddsSysctlFlag(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sysctl -e -w net.ipv4.ip_forward=1`, "", "", 0)
	mock.expectCommand(`grep -q .*net.ipv4.ip_forward`, "", "", 0)
	mock.expectCommand(`sysctl -e -p`, "", "", 0)

	result, err := e.moduleSysctl(context.Background(), mock, map[string]any{
		"name":         "net.ipv4.ip_forward",
		"value":        "1",
		"reload":       true,
		"ignoreerrors": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`sysctl -e -w net.ipv4.ip_forward=1`))
	core.AssertTrue(t, mock.hasExecuted(`sysctl -e -p`))
}

func TestModulesAdv_ModuleSysctl_Good_UsesCustomSysctlFile(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sed -i '/net\\.ipv4\\.ip_forward/d' .*custom\.conf`, "", "", 0)

	result, err := e.moduleSysctl(context.Background(), mock, map[string]any{
		"name":        "net.ipv4.ip_forward",
		"state":       "absent",
		"sysctl_file": "/etc/sysctl.d/custom.conf",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`sed -i '/net\\.ipv4\\.ip_forward/d' .*custom\.conf`))
}

// --- uri module ---

func TestModulesAdv_ModuleURI_Good_GetRequestDefault(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*https://example.com/api/health`, "OK\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://example.com/api/health",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed) // URI module does not set changed
	core.AssertEqual(t, 200, result.RC)
	core.AssertEqual(t, 200, result.Data["status"])
	core.AssertTrue(t, mock.containsSubstring("-L"))
}

func TestModulesAdv_ModuleURI_Good_DisablesRedirectFollowing(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*https://example.com/api/health`, "OK\n200", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":              "https://example.com/api/health",
		"follow_redirects": "none",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertFalse(t, result.Changed)
	core.AssertEqual(t, 200, result.RC)
	core.AssertEqual(t, 200, result.Data["status"])
	core.AssertTrue(t, mock.containsSubstring("--max-redirs 0"))
	core.AssertFalse(t, mock.containsSubstring("-L"))
}

func TestModulesAdv_ModuleURI_Good_PostWithBodyAndHeaders(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 201, result.RC)
	core.AssertTrue(t, mock.containsSubstring("-X POST"))
	core.AssertTrue(t, mock.containsSubstring("-d"))
	core.AssertTrue(t, mock.containsSubstring("Content-Type"))
	core.AssertTrue(t, mock.containsSubstring("Authorization"))
}

func TestModulesAdv_ModuleURI_Good_UsesBasicAuthFlags(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*secure\.example\.com`, "OK\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":              "https://secure.example.com/api",
		"url_username":     "apiuser",
		"url_password":     "apipass",
		"force_basic_auth": true,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 200, result.RC)
	core.AssertTrue(t, mock.hasExecuted(`-u .*apiuser:apipass`))
	core.AssertTrue(t, mock.hasExecuted(`--basic`))
}

func TestModulesAdv_ModuleURI_Good_UsesUnixSocket(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*http://localhost/_ping`, "OK\n200", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":         "http://localhost/_ping",
		"unix_socket": "/var/run/docker.sock",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 200, result.RC)
	core.AssertTrue(t, mock.hasExecuted(`--unix-socket '/var/run/docker.sock'`))
}

func TestModulesAdv_ModuleURI_Good_DisablesProxyUsage(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*https://example.com/api/health`, "OK\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":       "https://example.com/api/health",
		"use_proxy": false,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 200, result.RC)
	core.AssertTrue(t, mock.hasExecuted(`--noproxy`))
}

func TestModulesAdv_ModuleURI_Good_UsesSourceFileBody(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	core.RequireNoError(t, mock.Upload(context.Background(), newReader("alpha=1&beta=2"), "/tmp/request-body.txt", 0644))
	mock.expectCommand(`curl.*form\.example\.com`, "created\n201", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":         "https://form.example.com/submit",
		"method":      "POST",
		"src":         "/tmp/request-body.txt",
		"body_format": "json",
		"status_code": 201,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 201, result.RC)
	core.AssertTrue(t, mock.containsSubstring(`-d "alpha=1&beta=2"`))
	core.AssertFalse(t, mock.containsSubstring("Content-Type: application/json"))
}

func TestModulesAdv_ModuleURI_Good_FormURLEncodedBody(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 201, result.RC)
	core.AssertTrue(t, mock.containsSubstring(`-d "name=Alice+Example&scope=read&scope=write"`))
	core.AssertTrue(t, mock.containsSubstring("Content-Type: application/x-www-form-urlencoded"))
}

func TestModulesAdv_ModuleURI_Good_FormMultipartBody(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl.*form\.example\.com`, "created\n201", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":         "https://form.example.com/upload",
		"method":      "POST",
		"body_format": "form-multipart",
		"body": map[string]any{
			"name":  "Alice Example",
			"scope": []any{"read", "write"},
		},
		"status_code": 201,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 201, result.RC)
	core.AssertTrue(t, mock.containsSubstring(`-F "name=Alice Example"`))
	core.AssertTrue(t, mock.containsSubstring(`-F "scope=read"`))
	core.AssertTrue(t, mock.containsSubstring(`-F "scope=write"`))
	core.AssertFalse(t, mock.containsSubstring("Content-Type: application/json"))
	core.AssertFalse(t, mock.containsSubstring("Content-Type: application/x-www-form-urlencoded"))
}

func TestModulesAdv_ModuleURI_Good_WrongStatusCode(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "Not Found\n404", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://example.com/missing",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed) // Expected 200, got 404
	core.AssertEqual(t, 404, result.RC)
}

func TestModulesAdv_ModuleURI_Good_CurlCommandFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommandError(`curl`, core.AnError)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url": "https://unreachable.example.com",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, core.AnError.Error())
}

func TestModulesAdv_ModuleURI_Good_CustomExpectedStatus(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "\n204", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":         "https://api.example.com/resource/1",
		"method":      "DELETE",
		"status_code": 204,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 204, result.RC)
}

func TestModulesAdv_ModuleURI_Good_ReturnContent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "{\"ok\":true}\n200", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":            "https://example.com/api/status",
		"return_content": true,
		"status_code":    200,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "{\"ok\":true}", result.Data["content"])
	core.AssertEqual(t, 200, result.Data["status"])
}

func TestModulesAdv_ModuleURI_Good_WritesResponseToDest(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "{\"ok\":true}\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":  "https://example.com/api/status",
		"dest": "/tmp/api-status.json",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertNotNil(t, result.Data)
	core.AssertEqual(t, "/tmp/api-status.json", result.Data["dest"])

	content, err := mock.Download(context.Background(), "/tmp/api-status.json")
	core.RequireNoError(t, err)
	core.AssertEqual(t, []byte("{\"ok\":true}"), content)
}

func TestModulesAdv_ModuleURI_Good_JSONBodyFormat(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 201, result.RC)
	core.AssertTrue(t, mock.containsSubstring(`-d "{\"name\":\"test\"}"`))
	core.AssertTrue(t, mock.containsSubstring("Content-Type: application/json"))
}

func TestModulesAdv_ModuleURI_Good_TimeoutAndInsecureSkipVerify(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "OK\n200", "", 0)

	result, err := moduleURIWithClient(e, mock, map[string]any{
		"url":            "https://insecure.example.com/health",
		"timeout":        15,
		"validate_certs": false,
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 200, result.RC)
	core.AssertTrue(t, mock.containsSubstring("-k"))
	core.AssertTrue(t, mock.containsSubstring("--max-time 15"))
}

func TestModulesAdv_ModuleURI_Good_MultipleExpectedStatuses(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "\n202", "", 0)

	result, err := e.moduleURI(context.Background(), mock, map[string]any{
		"url":         "https://example.com/jobs/123",
		"status_code": []any{200, 202, 204},
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, 202, result.RC)
	core.AssertEqual(t, 202, result.Data["status"])
}

func TestModulesAdv_ModuleURI_Bad_MissingURL(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleURIWithClient(e, mock, map[string]any{
		"method": "GET",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "url required")
}

// --- ufw module ---

func TestModulesAdv_ModuleUFW_Good_AllowRuleWithPort(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw allow 443/tcp`, "Rule added", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule": "allow",
		"port": "443",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw allow 443/tcp`))
}

func TestModulesAdv_ModuleUFW_Good_EnableFirewall(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force enable`, "Firewall is active", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "enabled",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw --force enable`))
}

func TestModulesAdv_ModuleUFW_Good_DenyRuleWithProto(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw deny 53/udp`, "Rule added", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule":  "deny",
		"port":  "53",
		"proto": "udp",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw deny 53/udp`))
}

func TestModulesAdv_ModuleUFW_Good_ResetFirewall(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force reset`, "Resetting", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "reset",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw --force reset`))
}

func TestModulesAdv_ModuleUFW_Good_DisableFirewall(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw disable`, "Firewall stopped", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "disabled",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw disable`))
}

func TestModulesAdv_ModuleUFW_Good_ReloadFirewall(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw reload`, "Firewall reloaded", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "reloaded",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw reload`))
}

func TestModulesAdv_ModuleUFW_Good_LimitRule(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw limit 22/tcp`, "Rule added", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule": "limit",
		"port": "22",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw limit 22/tcp`))
}

func TestModulesAdv_ModuleUFW_Good_DeleteRule(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw delete allow 443/tcp`, "Rule deleted", "", 0)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"rule":   "allow",
		"port":   "443",
		"delete": true,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw delete allow 443/tcp`))
}

func TestModulesAdv_ModuleUFW_Good_LoggingMode(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw logging high`))
}

func TestModulesAdv_ModuleUFW_Good_BuiltinAliasDispatch(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`ufw --force enable`))
}

func TestModulesAdv_ModuleUFW_Good_StateCommandFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`ufw --force enable`, "", "ERROR: problem running ufw", 1)

	result, err := moduleUFWWithClient(e, mock, map[string]any{
		"state": "enabled",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
}

// --- docker_compose module ---

func TestModulesAdv_ModuleDockerCompose_Good_StatePresent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Creating container_1\nCreating container_2\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose up -d`))
	core.AssertTrue(t, mock.containsSubstring("/opt/myapp"))
}

func TestModulesAdv_ModuleDockerCompose_Good_StateAbsent(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose down`, "Removing container_1\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "absent",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose down`))
}

func TestModulesAdv_ModuleDockerCompose_Good_StateStopped(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose stop`, "Stopping container_1\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "stopped",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose stop`))
}

func TestModulesAdv_ModuleDockerCompose_Good_AlreadyUpToDate(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Container myapp-web-1  Up to date\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed) // "Up to date" in stdout → changed=false
	core.AssertFalse(t, result.Failed)
}

func TestModulesAdv_ModuleDockerCompose_Good_StateRestarted(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose restart`, "Restarting container_1\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/stack",
		"state":       "restarted",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose restart`))
}

func TestModulesAdv_ModuleDockerCompose_Bad_MissingProjectSrc(t *core.T) {
	e, _ := newTestExecutorWithMock("host1")
	mock := NewMockSSHClient()

	_, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"state": "present",
	})

	core.AssertError(t, err)
	core.AssertContains(t, err.Error(), "project_src required")
}

func TestModulesAdv_ModuleDockerCompose_Good_CommandFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "", "Error response from daemon", 1)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/broken",
		"state":       "present",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "Error response from daemon")
}

func TestModulesAdv_ModuleDockerCompose_Good_DefaultStateIsPresent(t *core.T) {
	// When no state is specified, default is "present"
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose up -d`, "Starting\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src": "/opt/app",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose up -d`))
}

func TestModulesAdv_ModuleDockerCompose_Good_ProjectNameAndFiles(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d`, "Starting\n", "", 0)

	result, err := moduleDockerComposeWithClient(e, mock, map[string]any{
		"project_src":  "/opt/app",
		"project_name": "demo-app",
		"files":        []any{"docker-compose.yml", "docker-compose.prod.yml"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.containsSubstring("docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d"))
}

func TestModulesAdv_ModuleDockerCompose_Production_Good_AlreadyUpToDate(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`docker compose up -d`, "Container myapp-web-1  Up to date\n", "", 0)

	result, err := e.moduleDockerCompose(context.Background(), mock, map[string]any{
		"project_src": "/opt/myapp",
		"state":       "present",
	})

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertEqual(t, "Container myapp-web-1  Up to date\n", result.Stdout)
}

func TestModulesAdv_ModuleDockerCompose_Production_Good_ProjectNameAndFiles(t *core.T) {
	e := NewExecutor("/tmp")
	mock := NewMockSSHClient()
	mock.expectCommand(`docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d`, "Starting\n", "", 0)

	result, err := e.moduleDockerCompose(context.Background(), mock, map[string]any{
		"project_src":  "/opt/app",
		"project_name": "demo-app",
		"files":        []any{"docker-compose.yml", "docker-compose.prod.yml"},
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose -p 'demo-app' -f 'docker-compose.yml' -f 'docker-compose.prod.yml' up -d`))
}

// --- Cross-module dispatch tests for advanced modules ---

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchUser(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchGroup(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`getent group|groupadd`, "", "", 0)

	task := &Task{
		Module: "group",
		Args: map[string]any{
			"name": "docker",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchCron(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchGit(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchURI(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`curl`, "OK\n200", "", 0)

	task := &Task{
		Module: "uri",
		Args: map[string]any{
			"url": "https://example.com",
		},
	}

	result, err := executeModuleWithMock(e, mock, "host1", task)

	core.RequireNoError(t, err)
	core.AssertFalse(t, result.Failed)
}

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchDockerCompose(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
}

func TestModulesAdv_ExecuteModuleWithMock_Good_DispatchDockerComposeV2(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
}

func TestModulesAdv_ExecuteModule_Good_DispatchBuiltinDockerCompose(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Changed)
	core.AssertFalse(t, result.Failed)
	core.AssertTrue(t, mock.hasExecuted(`docker compose up -d`))
}

// --- reboot module ---

func TestModulesAdv_ModuleReboot_Good_WaitsForTestCommand(t *core.T) {
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

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "Reboot initiated", result.Msg)
	core.AssertEqual(t, 3, mock.commandCount())
	core.AssertTrue(t, mock.hasExecuted(`sleep 2 && shutdown -r now 'Maintenance window' &`))
	core.AssertTrue(t, mock.hasExecuted(`sleep 3`))
	core.AssertTrue(t, mock.hasExecuted(`whoami`))
}

func TestModulesAdv_ModuleReboot_Good_CustomRebootCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`sleep 1 && /sbin/reboot`, "", "", 0)
	mock.expectCommand(`whoami`, "root\n", "", 0)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{
		"reboot_command":   "/sbin/reboot",
		"pre_reboot_delay": 1,
		"reboot_timeout":   5,
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Changed)
	core.AssertEqual(t, "Reboot initiated", result.Msg)
	core.AssertEqual(t, 2, mock.commandCount())
	core.AssertTrue(t, mock.hasExecuted(`sleep 1 && /sbin/reboot`))
	core.AssertTrue(t, mock.hasExecuted(`whoami`))
}

func TestModulesAdv_ModuleReboot_Bad_TimesOutWaitingForTestCommand(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`shutdown -r now 'Reboot initiated by Ansible' &`, "", "", 0)
	mock.expectCommand(`whoami`, "", "host unreachable", 1)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{
		"reboot_timeout": 0,
		"test_command":   "whoami",
	})

	core.RequireNoError(t, err)
	core.AssertTrue(t, result.Failed)
	core.AssertContains(t, result.Msg, "timed out")
	core.AssertEqual(t, "host unreachable", result.Stderr)
	core.AssertEqual(t, 1, result.RC)
}

func TestModulesAdv_ModuleReboot_Bad_ReportsInitialShutdownFailure(t *core.T) {
	e, mock := newTestExecutorWithMock("host1")
	mock.expectCommand(`shutdown -r now 'Reboot initiated by Ansible' &`, "", "permission denied", 1)

	result, err := e.moduleReboot(context.Background(), mock, map[string]any{})

	core.RequireNoError(t, err)
	core.AssertNotNil(t, result)
	core.AssertTrue(t, result.Failed)
	core.AssertEqual(t, "permission denied", result.Msg)
	core.AssertEqual(t, 1, result.RC)
	core.AssertEqual(t, 1, mock.commandCount())
	core.AssertFalse(t, mock.hasExecuted(`whoami`))
}
