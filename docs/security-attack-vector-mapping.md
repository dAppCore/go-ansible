# Security Attack Vector Mapping

Scope: "external input" here means CLI arguments, controller-side files, environment variables, playbook/inventory/task YAML, inventory-derived SSH settings, remote SSH stdout/stderr/facts, and remote/network responses consumed by modules.

Notes:
- All task module arguments originate in YAML via `(*Task).UnmarshalYAML` and are templated in `(*Executor).templateArgs` before handler dispatch.
- Unless noted otherwise, module handlers only check required-field presence and coerce values with `getStringArg`/`getBoolArg` (`modules.go:1095`, `modules.go:1105`). They do not enforce allow-lists, path containment, protocol restrictions, or sink-specific sanitisation.
- This document records current behaviour only. It does not propose fixes.

## Controller, CLI, Parsing, and Templating Boundaries

| Function | File:Line | Input source | Flows into | Current validation | Potential attack vector |
| --- | --- | --- | --- | --- | --- |
| `runAnsible` | `cmd/ansible/ansible.go:29` | Positional CLI playbook path | `filepath.Abs`, `coreio.Local.Exists`, `Executor.Run`, then YAML parsing and task execution | Existence check only; no allow-list or base-path restriction | Arbitrary controller-side file selection; execution of attacker-controlled playbook content |
| `runAnsible` | `cmd/ansible/ansible.go:29` | CLI `--extra-vars`, `--limit`, `--tags`, `--skip-tags` | `Executor.SetVar`, host filtering, tag filtering, later templating and module dispatch | Comma splitting only; no schema or escaping | Variable poisoning into commands/paths/URLs; host-scope expansion; task-selection bypass |
| `runAnsible` | `cmd/ansible/ansible.go:29` | CLI `--inventory` path | `Executor.SetInventory` -> `Parser.ParseInventory` | Abs path resolution, existence check, simple directory probing for fixed filenames | Arbitrary inventory load; malicious SSH target/credential redirection |
| `runAnsible` | `cmd/ansible/ansible.go:29` | Play names, task names, task results, remote `stdout`/`stderr` | `fmt.Printf` in callbacks | No output escaping or redaction beyond verbosity gates | Terminal-control-sequence injection, log injection, secret disclosure in verbose mode |
| `runAnsibleTest` | `cmd/ansible/ansible.go:163` | CLI host, user, password, key, port | `NewSSHClient`, `Connect`, remote fact probe commands | Defaults only | Outbound SSH to arbitrary hosts, credential misuse, internal-host probing |
| `runAnsibleTest` | `cmd/ansible/ansible.go:163` | Remote fact probe output | Printed directly to terminal | No output escaping | Terminal-control-sequence injection or misleading operator output from hostile host |
| `Parser.ParsePlaybook` | `parser.go:31` | Controller-side playbook file path and YAML content | `coreio.Local.Read`, `yaml.Unmarshal`, `processPlay` | File read and YAML parse only | Arbitrary local file read by API caller; attacker-controlled task graph and vars |
| `Parser.ParseInventory` | `parser.go:68` | Controller-side inventory path and YAML content | `coreio.Local.Read`, `yaml.Unmarshal` | File read and YAML parse only | Inventory poisoning of hosts, ports, users, passwords, key paths, become settings |
| `Parser.ParseTasks` | `parser.go:83` | Controller-side task include path and YAML content | `coreio.Local.Read`, `yaml.Unmarshal`, `extractModule` | File read and YAML parse only | Included-task poisoning; arbitrary local file read if caller controls path |
| `Parser.ParseRole` | `parser.go:119` | Role `name` and `tasks_from` from playbook YAML | `filepath.Join/Clean`, search-path traversal, reads role tasks/defaults/vars | Existence check only; no containment within a roles root | Path traversal out of intended roles tree; arbitrary controller-side YAML/vars load |
| `Task.UnmarshalYAML` | `parser.go:239` | Arbitrary task keys and module argument values from YAML | `task.Module`, `task.Args`, include/import fields, loop fields | Known-key skip list only; any dotted key is accepted as a module | Module confusion, unexpected dispatch, arbitrary argument injection into later sinks |
| `Executor.runIncludeTasks` | `executor.go:420` | `include_tasks` / `import_tasks` path, after templating | `templateString`, `Parser.ParseTasks` | No path canonicalisation or base-path enforcement | Variable-driven arbitrary task-file include and controller-side file read |
| `Executor.runIncludeRole` | `executor.go:444` | `include_role` / `import_role` name, `tasks_from`, vars | `runRole` -> `Parser.ParseRole` | No validation beyond downstream role lookup | Variable-driven role traversal and role var poisoning |
| `Executor.templateString` / `resolveExpr` | `executor.go:746` / `executor.go:757` | Templated strings from playbook vars, task vars, host vars, facts, and registered results | Module args, include paths, command fragments, URLs, file paths | Regex substitution only; unresolved expressions are left in place | Data-to-command/path/URL injection once templated values reach sink functions |
| `Executor.handleLookup` | `executor.go:870` | Template `lookup('env', ...)` and `lookup('file', ...)` expressions | `os.Getenv`, `coreio.Local.Read`, then back into templated args/content | Regex parse only; no path or variable allow-list | Controller secret exfiltration via environment variables or arbitrary local file reads |
| `Executor.TemplateFile` | `executor.go:973` | Template `src` path and template file content | Local file read, Go template parse/execute, later upload to remote host | `src` must exist; no path restrictions; parse failure falls back to string replacement | Arbitrary controller-side file read; template-driven secret interpolation into remote files |
| `Executor.evaluateWhen` / `evalCondition` | `executor.go:620` / `executor.go:652` | `when:` and assertion conditions from YAML, plus templated values | Task/role gating logic | Minimal parsing; unknown conditions default to `true` | Conditional-bypass and unexpected task execution if conditions are malformed or attacker-shaped |
| `Executor.getHosts` / `GetHosts` | `executor.go:468` / `parser.go:331` | Play `hosts` and CLI `--limit` patterns | Inventory host selection | Exact/group lookup plus `strings.Contains` fallback for `Limit` | Substring-based host-scope expansion to unintended inventory entries |
| `Executor.getClient` | `executor.go:499` | Inventory host vars such as `ansible_host`, `ansible_port`, `ansible_user`, passwords, key file, become password | `SSHConfig`, `NewSSHClient`, later network dial and sudo use | Type assertions only | Host redirection, credential injection, arbitrary key-path selection, password reuse for sudo |
| `Executor.gatherFacts` | `executor.go:565` | Remote command output from target host | `e.facts`, later templating and condition evaluation | `strings.TrimSpace` and simple field parsing only | Fact poisoning from hostile hosts into later command, file, and conditional paths |

## SSH and Remote Transport Boundaries

| Function | File:Line | Input source | Flows into | Current validation | Potential attack vector |
| --- | --- | --- | --- | --- | --- |
| `SSHClient.Connect` | `ssh.go:77` | SSH config from CLI/inventory: host, port, user, password, key file | Controller-side key reads, `known_hosts`, outbound TCP dial, SSH handshake | Defaults plus `known_hosts` verification; no host or key-path restrictions | Arbitrary key-file read on controller; outbound SSH to internal or attacker hosts; credential use against unintended targets |
| `SSHClient.Run` | `ssh.go:200` | Remote command string built by executor/modules | `session.Run`, optionally wrapped in `sudo ... bash -c` | Only escapes single quotes when `become` is active; otherwise trusts caller | Shell injection whenever a module builds a command from untrusted data; privileged execution under reused passwords |
| `SSHClient.RunScript` | `ssh.go:276` | Raw script text from task/module logic | Heredoc-fed `bash` on remote host | No sanitisation; static heredoc delimiter | Arbitrary remote script execution; delimiter collision if script embeds `ANSIBLE_SCRIPT_EOF` |
| `SSHClient.Upload` | `ssh.go:283` | Uploaded bytes, remote path, mode, become state | Remote `mkdir`, `cat >`, `chmod`, optional `sudo` | Remote path quoted; mode rendered numerically | Arbitrary remote file write/overwrite, including privileged paths when `become` is enabled |
| `SSHClient.Download` | `ssh.go:377` | Remote file path | Remote `cat`, returned bytes | Remote path quoted only | Arbitrary remote file read and exfiltration back to controller |
| `SSHClient.FileExists` | `ssh.go:396` | Remote file path | Remote `test -e` probe | Remote path quoted only | Remote path probing and presence disclosure |
| `SSHClient.Stat` | `ssh.go:410` | Remote file path | Inline remote shell script and parsed booleans | Remote path quoted only | Remote path probing and metadata disclosure |

## Module Dispatch and Command/Control Modules

| Function | File:Line | Input source | Flows into | Current validation | Potential attack vector |
| --- | --- | --- | --- | --- | --- |
| `executeModule` | `modules.go:17` | `task.Module` and templated `task.Args` from YAML | Module switch dispatch; unknown/empty modules with spaces fall back to `moduleShell` | Module normalisation only | Malformed or spoofed module names can degrade into shell execution |
| `moduleShell` | `modules.go:178` | `shell:` / `cmd:` / `_raw_params`, optional `chdir` | `SSHClient.RunScript` | Non-empty command required; `chdir` quoted | Arbitrary remote shell execution by design |
| `moduleCommand` | `modules.go:206` | `command:` / `cmd:` / `_raw_params`, optional `chdir` | `SSHClient.Run` | Non-empty command required; `chdir` quoted | Remote command injection because the whole string is passed to the remote shell |
| `moduleRaw` | `modules.go:234` | Raw task command string | `SSHClient.Run` | Non-empty string required | Arbitrary remote command execution by design |
| `moduleScript` | `modules.go:253` | Controller-side script path from task args | Local file read, then `SSHClient.RunScript` | Script path must exist | Arbitrary controller file read and remote execution of attacker-selected script |
| `moduleApt` | `modules.go:550` | Package `name`, `state`, `update_cache` | `apt-get` command strings | Required-field check is weak; package name inserted raw | Shell injection via package name; attacker-controlled package installation/removal |
| `moduleAptKey` | `modules.go:584` | Key URL, optional keyring path, state | Remote `curl | gpg --dearmor` or `curl | apt-key add -` | URL required for present state; no protocol/host checks | Trust-store poisoning from attacker URL; remote egress/SSRF-style fetches |
| `moduleAptRepository` | `modules.go:615` | Repository line, optional filename, `update_cache` | Writes `/etc/apt/sources.list.d/*.list`, then `apt-get update` | Repo required; filename only lightly normalised | Repository poisoning and package-source redirection |
| `modulePackage` | `modules.go:652` | Package args plus remote `which` output | Delegates to `moduleApt` | No extra validation | Same package-command risks as `moduleApt`; trusts remote detection output |
| `modulePip` | `modules.go:665` | Package `name`, `state`, `executable` | Remote `pip`/custom executable command | No allow-list; values inserted raw | Shell injection; arbitrary executable selection on remote host |
| `moduleService` | `modules.go:690` | Unit `name`, `state`, `enabled` | `systemctl` command strings | `name` required; values inserted raw | Shell injection and unauthorised service lifecycle manipulation |
| `moduleSystemd` | `modules.go:732` | `daemon_reload` plus service args | `systemctl daemon-reload`, then `moduleService` | No extra validation | Same service-control and command-injection risks as `moduleService` |
| `moduleUser` | `modules.go:743` | Username plus uid/group/groups/home/shell/system flags | `useradd`, `usermod`, `userdel` command strings | `name` required; most fields inserted raw | Shell injection and privileged account creation/modification/deletion |
| `moduleGroup` | `modules.go:800` | Group name, gid, system flag | `groupadd`, `groupdel` commands | `name` required; values inserted raw | Shell injection and privileged group manipulation |
| `moduleURI` | `modules.go:835` | URL, method, headers, body, expected status | Remote `curl`, stdout/stderr parsing into task results | URL required; headers/body are concatenated into command options | SSRF/egress from remote host, shell injection through headers/body, response-driven output injection |
| `moduleAssert` | `modules.go:915` | `that` conditions and `fail_msg` | `evalCondition`, task result | Only checks that `that` exists | Guard bypass because condition evaluator is permissive; attacker-controlled failure messages |
| `moduleSetFact` | `modules.go:932` | Arbitrary key/value pairs from playbook | Global executor vars used by later templating and conditions | Ignores only `cacheable` | Variable poisoning that changes later commands, paths, URLs, and condition outcomes |
| `modulePause` | `modules.go:941` | `seconds` | Sleep loop gated by context | Numeric parse only | Denial of service through long pauses or deliberately stalled runs |
| `moduleWaitFor` | `modules.go:989` | Target host, port, state, timeout | Remote `timeout ... bash -c 'until nc -z ...'` | Port/timeout integer checks only; host inserted raw | Shell injection through host; remote internal-port scanning / SSRF-like reachability probing |
| `moduleGit` | `modules.go:1013` | Repo URL/path, destination, version | `git clone`, `git fetch`, `git checkout --force` on remote host | `repo` and `dest` required; shell quoting used | Supply-chain/code provenance risk from attacker repo/version; forced checkout can destroy remote working tree state |
| `moduleHostname` | `modules.go:1120` | Hostname string | `hostnamectl` / `hostname`, then `sed -i` on `/etc/hosts` | `name` required; quoted in first command only | `/etc/hosts` corruption or `sed` injection via hostname content |
| `moduleSysctl` | `modules.go:1139` | Sysctl `name`, `value`, state | `sysctl -w`, `grep`, `sed`, config append | `name` required; values inserted raw | Shell injection and persistent kernel-parameter tampering |
| `moduleCron` | `modules.go:1172` | Cron `name`, `job`, `user`, schedule fields, state | `crontab` pipelines with `grep` and `echo` | No allow-list; several fields inserted raw | Shell injection and persistence via cron jobs |
| `moduleReboot` | `modules.go:1288` | Reboot delay and message | `shutdown -r now 'msg'` | Delay integer only; message is single-quoted without escaping | Shell injection via reboot message; high-impact denial of service |
| `moduleUFW` | `modules.go:1306` | Firewall rule, port, protocol, state | `ufw` commands | No allow-list; port/proto inserted raw | Shell injection and unauthorised firewall reconfiguration |
| `moduleDockerCompose` | `modules.go:1408` | `project_src`, `state` | `docker compose` in selected directory | `project_src` required; path quoted | Execution of attacker-controlled compose project, image pull/deploy supply-chain risk |

## File, Transfer, and Data-Exposure Modules

| Function | File:Line | Input source | Flows into | Current validation | Potential attack vector |
| --- | --- | --- | --- | --- | --- |
| `moduleCopy` | `modules.go:281` | Local `src` or inline `content`, remote `dest`, mode, owner, group | Controller file read or inline content -> `SSHClient.Upload` -> `chown`/`chgrp` | `dest` required; mode parse is best-effort; owner/group inserted raw | Arbitrary controller file read, remote file overwrite, ownership-command injection |
| `moduleTemplate` | `modules.go:324` | Template `src`, remote `dest`, mode, templated vars/facts | `TemplateFile` -> `SSHClient.Upload` | `src` and `dest` required | Arbitrary controller template read, secret interpolation into remote files, remote overwrite |
| `moduleFile` | `modules.go:352` | Remote path/dest, state, mode, src, owner, group, recurse | `mkdir`, `chmod`, `rm -rf`, `touch`, `ln -sf`, `chown`, `chgrp` | Path required; mode/owner/group inserted raw | Destructive deletion, symlink manipulation, permission/ownership command injection |
| `moduleLineinfile` | `modules.go:420` | Remote path, line text, regexp, state | `sed`, `grep`, `echo` on remote host | Path required; regexp/line not sanitised for `sed` | Regex/`sed` injection, file corruption, uncontrolled line insertion/removal |
| `moduleStat` | `modules.go:463` | Remote path | `SSHClient.Stat`, then result data | Path required | Remote path probing and metadata disclosure |
| `moduleSlurp` | `modules.go:480` | Remote path/src | `SSHClient.Download`, then base64-encoded result | Path required | Arbitrary remote file exfiltration into task results/registered vars |
| `moduleFetch` | `modules.go:502` | Remote `src`, controller `dest` | `SSHClient.Download`, `EnsureDir`, `coreio.Local.Write` | Both paths required; no destination root restriction | Arbitrary controller-side file overwrite from remote-controlled content |
| `moduleGetURL` | `modules.go:526` | URL, remote `dest`, mode | Remote `curl`/`wget`, optional `chmod` | URL and `dest` required; mode inserted raw | Remote SSRF/egress, arbitrary file write, permission command injection |
| `moduleUnarchive` | `modules.go:1042` | Local or remote archive `src`, remote `dest`, `remote_src` | Optional controller file read/upload, then remote `tar`/`unzip` | `src` and `dest` required; archive type guessed from suffix | Arbitrary controller file read, extraction of attacker archives, path-traversal/symlink effects during extraction |
| `moduleBlockinfile` | `modules.go:1209` | Remote path, block text, marker, state, create flag | Remote `sed` and heredoc append script | Path required; marker only partly escaped; block inserted into heredoc script | `sed` injection, file corruption, persistent content insertion |
| `moduleIncludeVars` | `modules.go:1262` | File path or raw parameter from task args | Currently only returned in result message | No validation | Information disclosure/log injection via reflected path; function is a stub, not a parser |
| `moduleDebug` | `modules.go:895` | `msg` or variable name/value | Task result message, later terminal output | No redaction or escaping | Secret leakage and terminal/log injection when untrusted values are displayed |
| `moduleFail` | `modules.go:907` | Failure message | Task result message, later terminal output | No escaping | Log/terminal injection via reflected failure text |
| `moduleMeta` | `modules.go:1277` | Meta action args from playbook | No-op result | Args ignored | No direct sink today; low immediate impact unless future behaviour is added without review |
| `moduleSetup` | `modules.go:1283` | Module invocation from playbook | Currently returns a fixed result without using remote input | No task-arg handling and no remote call in current implementation | No direct sink today; future fact-gathering work would need a fresh boundary review |
| `moduleAuthorizedKey` | `modules.go:1357` | User name, SSH public key, state | Remote `getent`, `mkdir`, `chown`, `grep`, `echo`, `sed` on `authorized_keys` | `user` and `key` required; key matching uses only first 40 chars; user inserted raw | Command injection through user, persistent SSH access, key-prefix collisions or incorrect key removal |
