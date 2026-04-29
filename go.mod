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
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/sys v0.43.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace (
	dappco.re/go/io => ../go-io
	dappco.re/go/log => ../go-log
)
