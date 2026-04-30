package ansible

import (
	core "dappco.re/go"
	coreerr "dappco.re/go/log"
)

type commandRunResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
	Err      error
}

type parserRoleDataResult struct {
	Tasks    []Task
	Defaults map[string]any
	Vars     map[string]any
	Path     string
	Handlers []Task
}

type backupRemoteFileResult struct {
	Path      string
	HadBefore bool
}

func commandRunOK(stdout, stderr string, exitCode int) core.Result {
	return core.Ok(map[string]any{
		"stdout":   stdout,
		"stderr":   stderr,
		"exitCode": exitCode,
	})
}

func commandRunFail(stdout, stderr string, exitCode int, cause any) core.Result {
	err, _ := cause.(error)
	if err == nil {
		err = coreerr.E("commandRunFail", sprintf("command failed with exit code %d: %s%s", exitCode, stdout, stderr), nil)
	}
	return core.Fail(err)
}

func commandRunValue(r core.Result) commandRunResult {
	if out, ok := r.Value.(commandRunResult); ok {
		return out
	}
	if out, ok := r.Value.(map[string]any); ok {
		result := commandRunResult{}
		if stdout, ok := out["stdout"].(string); ok {
			result.Stdout = stdout
		}
		if stderr, ok := out["stderr"].(string); ok {
			result.Stderr = stderr
		}
		if exitCode, ok := out["exitCode"].(int); ok {
			result.ExitCode = exitCode
		}
		if err, ok := out["err"].(error); ok {
			result.Err = err
		}
		return result
	}
	return commandRunResult{}
}

func wrapFailure(r core.Result, operation, message string) core.Result {
	if r.OK {
		return r
	}
	if err, ok := r.Value.(error); ok {
		return core.Fail(coreerr.E(operation, message, err))
	}
	return core.Fail(coreerr.E(operation, message+": "+r.Error(), nil))
}

func resultErrorMessage(r core.Result) string {
	if r.OK {
		return ""
	}
	return r.Error()
}
