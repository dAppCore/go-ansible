package ansible

import (
	core "dappco.re/go"
)

func ExampleNewService() {
	c := core.New(core.WithService(NewService(Options{BasePath: "/srv/playbooks"})))
	svc := core.MustServiceFor[*Service](c, "ansible")
	core.Println(svc.Parser() != nil)
	// Output: true
}

func ExampleRegister() {
	c := core.New(core.WithService(Register))
	r := c.Service("ansible")
	core.Println(r.OK)
	// Output: true
}

func ExampleService_Parser() {
	c := core.New(core.WithService(NewService(Options{})))
	svc := core.MustServiceFor[*Service](c, "ansible")
	core.Println(svc.Parser() != nil)
	// Output: true
}
