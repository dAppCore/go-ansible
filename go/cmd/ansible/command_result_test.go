package ansiblecmd

import (
	core "dappco.re/go"
)

// --- commandResultStdout ---

func TestCommandResult_Stdout_Good_ExtractsString(t *core.T) {
	r := core.Ok(map[string]any{"stdout": "hello"})
	core.AssertEqual(t, "hello", commandResultStdout(r))
}

func TestCommandResult_Stdout_Bad_MissingOrWrongType(t *core.T) {
	core.AssertEqual(t, "", commandResultStdout(core.Ok(map[string]any{})))
	core.AssertEqual(t, "", commandResultStdout(core.Ok(map[string]any{"stdout": 42})))
}

func TestCommandResult_Stdout_Ugly_NonMapValue(t *core.T) {
	core.AssertEqual(t, "", commandResultStdout(core.Ok("not-a-map")))
}

// --- commandResultExitCode ---

func TestCommandResult_ExitCode_Good_ExtractsInt(t *core.T) {
	r := core.Ok(map[string]any{"exitCode": 0})
	core.AssertEqual(t, 0, commandResultExitCode(r))
	r = core.Ok(map[string]any{"exitCode": 2})
	core.AssertEqual(t, 2, commandResultExitCode(r))
}

func TestCommandResult_ExitCode_Bad_MissingDefaultsNegative(t *core.T) {
	core.AssertEqual(t, -1, commandResultExitCode(core.Ok(map[string]any{})))
	core.AssertEqual(t, -1, commandResultExitCode(core.Ok(map[string]any{"exitCode": "2"})))
}

func TestCommandResult_ExitCode_Ugly_NonMapValue(t *core.T) {
	core.AssertEqual(t, -1, commandResultExitCode(core.Ok(nil)))
}
