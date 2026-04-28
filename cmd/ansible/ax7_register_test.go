package ansiblecmd

import core "dappco.re/go"

func TestAX7_Register_Good(t *core.T) {
	app := core.New()
	Register(app)
	ansible := app.Command("ansible")
	core.AssertTrue(t, ansible.OK)
	core.AssertNotNil(t, ansible.Value)
}

func TestAX7_Register_Bad(t *core.T) {
	var app *core.Core
	core.AssertPanics(t, func() {
		Register(app)
	})
	core.AssertNil(t, app)
}

func TestAX7_Register_Ugly(t *core.T) {
	app := core.New()
	Register(app)
	Register(app)
	testCommand := app.Command("ansible/test")
	core.AssertTrue(t, testCommand.OK)
	core.AssertNotNil(t, testCommand.Value)
}
