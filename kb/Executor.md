# Executor

Module: `dappco.re/go/ansible`

The `Executor` is the main playbook runner. It manages SSH connections, variable resolution, conditional evaluation, loops, blocks, roles, handlers, and module execution.

## Execution Flow

1. Parse playbook YAML into `[]Play`
2. For each play:
   - Resolve target hosts from inventory (apply `Limit` filter)
   - Merge play variables
   - Gather facts (SSH into hosts, collect OS/hostname/kernel info)
   - Execute `pre_tasks`, `roles`, `tasks`, `post_tasks`
   - Run notified handlers
3. Each task goes through:
   - Tag matching (`Tags`, `SkipTags`)
   - Block/rescue/always handling
   - Include/import resolution
   - `when` condition evaluation
   - Loop expansion
   - Module execution via SSH
   - Result registration and handler notification

## Templating

Jinja2-like `{{ var }}` syntax is supported:

- Variable resolution from play vars, task vars, host vars, facts, registered results
- Dotted access: `{{ result.stdout }}`, `{{ result.rc }}`
- Filters: `| default(value)`, `| bool`, `| trim`
- Lookups: `lookup('env', 'HOME')`, `lookup('file', '/path')`

## Conditionals

`when` supports:

- Boolean literals: `true`, `false`
- Inline boolean expressions with `and`, `or`, and parentheses
- Registered variable checks: `result is success`, `result is failed`, `result is changed`, `result is defined`
- Negation: `not condition`
- Variable truthiness checks

## SSH Client Features

- Key-based and password authentication
- Known hosts verification
- Privilege escalation (`become`/`sudo`) with password support
- File upload via `cat` (no SCP dependency)
- File download, stat, exists checks
- Context-based timeout and cancellation
