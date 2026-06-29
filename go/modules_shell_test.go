package ansible

import (
	core "dappco.re/go"
)

// --- shellQuote ---

func TestModulesShell_ShellQuote_Good_WrapsAndEscapes(t *core.T) {
	core.AssertEqual(t, "'plain'", shellQuote("plain"))
	core.AssertEqual(t, "'it'\\''s'", shellQuote("it's"))
}

// --- wrapLocalBecomeCommand ---

func TestModulesShell_WrapLocalBecomeCommand_Good_DefaultRootNoPassword(t *core.T) {
	got := wrapLocalBecomeCommand("ls -l", "", "")
	core.AssertContains(t, got, "sudo -n -u root bash -lc")
	core.AssertContains(t, got, "ls -l")
}

func TestModulesShell_WrapLocalBecomeCommand_Ugly_PasswordAndQuoteEscape(t *core.T) {
	got := wrapLocalBecomeCommand("echo 'hi'", "deploy", "secret")
	core.AssertContains(t, got, "sudo -S -u deploy bash -lc")
	// The embedded single-quote is escaped for the wrapping shell.
	core.AssertContains(t, got, "'\\''")
}

// --- sedExactLinePattern ---

func TestModulesShell_SedExactLinePattern_Good_EscapesRegexAndPipe(t *core.T) {
	got := sedExactLinePattern("a.b|c")
	// QuoteMeta escapes the dot; the pipe is additionally backslash-escaped.
	core.AssertContains(t, got, "a\\.b")
	core.AssertContains(t, got, "\\|c")
}

// --- isAuthorizedKeyType / authorizedKeyBase ---

func TestModulesShell_IsAuthorizedKeyType_Good_KnownPrefixes(t *core.T) {
	core.AssertTrue(t, isAuthorizedKeyType("ssh-rsa"))
	core.AssertTrue(t, isAuthorizedKeyType("ecdsa-sha2-nistp256"))
	core.AssertTrue(t, isAuthorizedKeyType("sk-ssh-ed25519@openssh.com"))
	core.AssertFalse(t, isAuthorizedKeyType("comment"))
}

func TestModulesShell_AuthorizedKeyBase_Good_TypeAndKey(t *core.T) {
	core.AssertEqual(t, "ssh-rsa AAAAB3", authorizedKeyBase("ssh-rsa AAAAB3 user@host"))
}

func TestModulesShell_AuthorizedKeyBase_Bad_EmptyAndNoType(t *core.T) {
	core.AssertEqual(t, "", authorizedKeyBase(""))
	// A line without a recognised key-type returns the (trimmed) line as-is.
	core.AssertEqual(t, "just a comment line", authorizedKeyBase("  just a comment line  "))
}

func TestModulesShell_AuthorizedKeyBase_Ugly_TypeWithoutKey(t *core.T) {
	core.AssertEqual(t, "ssh-rsa", authorizedKeyBase("ssh-rsa"))
}

// --- rewriteAuthorizedKeyContent ---

func TestModulesShell_RewriteAuthorizedKeyContent_Good_AppendsNewKey(t *core.T) {
	content := "ssh-rsa AAAA existing@host\n"
	line := "ssh-ed25519 BBBB new@host"
	out, changed := rewriteAuthorizedKeyContent(content, "", line)
	core.AssertTrue(t, changed)
	core.AssertContains(t, out, "ssh-rsa AAAA existing@host")
	core.AssertContains(t, out, line)
}

func TestModulesShell_RewriteAuthorizedKeyContent_Bad_AlreadyPresentNoChange(t *core.T) {
	line := "ssh-rsa AAAA user@host"
	out, changed := rewriteAuthorizedKeyContent(line+"\n", "", line)
	core.AssertFalse(t, changed)
	core.AssertEqual(t, line+"\n", out)
}

func TestModulesShell_RewriteAuthorizedKeyContent_Ugly_RemovalByEmptyLine(t *core.T) {
	// Empty target line with a matching base removes that key entirely.
	content := "ssh-rsa AAAA user@host\n"
	out, changed := rewriteAuthorizedKeyContent(content, "ssh-rsa AAAA", "")
	core.AssertTrue(t, changed)
	core.AssertEqual(t, "", out)
}

// --- resolveSerialBatchSizes / resolveSerialBatchSize ---

func TestModulesShell_ResolveSerialBatchSizes_Good_IntAndNil(t *core.T) {
	core.AssertElementsMatch(t, []int{10}, resolveSerialBatchSizes(nil, 10))
	core.AssertElementsMatch(t, []int{3}, resolveSerialBatchSizes(3, 10))
}

func TestModulesShell_ResolveSerialBatchSizes_Bad_NonPositiveTotal(t *core.T) {
	core.AssertNil(t, resolveSerialBatchSizes(5, 0))
}

func TestModulesShell_ResolveSerialBatchSizes_Ugly_PercentAndSlice(t *core.T) {
	// 30% of 10 hosts, rounded up.
	core.AssertElementsMatch(t, []int{3}, resolveSerialBatchSizes("30%", 10))
	// A mixed list of absolute and percentage batches.
	core.AssertElementsMatch(t, []int{1, 5}, resolveSerialBatchSizes([]any{1, "50%"}, 10))
	// Empty string falls back to the whole batch.
	core.AssertElementsMatch(t, []int{10}, resolveSerialBatchSizes("", 10))
}

func TestModulesShell_ResolveSerialBatchSize_Good_FirstSize(t *core.T) {
	core.AssertEqual(t, 1, resolveSerialBatchSize([]any{1, "50%"}, 10))
	core.AssertEqual(t, 10, resolveSerialBatchSize(nil, 10))
}
