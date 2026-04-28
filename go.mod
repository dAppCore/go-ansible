module dappco.re/go/ansible

go 1.26.0

require (
	dappco.re/go v0.9.0
	dappco.re/go/io v0.8.0-alpha.1
	dappco.re/go/log v0.8.0-alpha.1
	golang.org/x/crypto v0.50.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	dappco.re/go/core v0.8.0-alpha.1 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace (
	dappco.re/go/io => github.com/dAppCore/go-io v0.8.0-alpha.1
	dappco.re/go/log => github.com/dAppCore/go-log v0.8.0-alpha.1
)
