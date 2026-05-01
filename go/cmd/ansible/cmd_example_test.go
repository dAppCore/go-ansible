package ansiblecmd

import core "dappco.re/go"

func ExampleRegister() {
	app := core.New()
	Register(app)
	ansible := app.Command("ansible")
	test := app.Command("ansible/test")
	core.Println(ansible.OK, test.OK)
	// Output: true true
}
