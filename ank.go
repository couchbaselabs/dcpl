package main

import (
	"bufio"
	"fmt"
	"github.com/mattn/anko/core"
	"github.com/mattn/anko/packages"
	"github.com/mattn/anko/parser"
	"github.com/mattn/anko/vm"
	"os"
	"strings"
)

func NewAnkEnv() *vm.Env {
	env := vm.NewEnv()
	core.Import(env)
	packages.DefineImport(env)
	return env
}

func runREPL() {
	fmt.Fprintf(os.Stderr, `# For more info about .ank scripts, see https://github.com/mattn/anko.
# In filtering script, DCPL defines variable 'event' which has following structure:
#
#     type Event struct {
#       Type  string
#       Key   string
#       Value []byte
#     }
#
# This REPL has preloaded event in 'event' variable, where Value is JSON encoded object. So that
# the simples filtering script might look like this:
#
#     json = import("encoding/json")
#     time = import("time")
#     func filter(event) {
#       if event.Type != "mutation" {
#         return false
#       }
#       releaseDate, err = time.Parse(time.RFC3339, "2015-08-15T01:01:42+08:00")
#       if err != nil {
#         return false
#       }
#       val = ""
#       err = json.Unmarshal(event.Value, &val)
#       if err != nil {
#         return false
#       }
#       documentDate, err = time.Parse(time.RFC3339, toString(val.LastViewDate))
#       if err != nil {
#         return false
#       }
#       return val.Type == "ad-view" && documentDate.After(releaseDate)
#     }
#     filter(event)
#
# Enter quit() to close REPL.
#


`)
	var (
		source    string
		following bool
	)
	parser.EnableErrorVerbose()

	event := Event{
		Type: "mutation",
		Key:  "foo",
		Value: []byte(`{
  "Type": "ad-view",
  "ObjectId": 43827,
  "ProfileId": 39524,
  "Count": 5,
  "Referrers": ["http://example.com", "http://google.com/search"],
  "LastViewDate": "2015-09-27T12:01:31+03:00"
}`),
	}

	env := NewAnkEnv()
	env.Define("event", event)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		if following {
			source += "\n"
			fmt.Print("  ")
		} else {
			fmt.Print("> ")
		}

		if !scanner.Scan() {
			break
		}
		source += scanner.Text()
		if source == "" {
			continue
		}
		if source == "quit()" {
			break
		}

		stmts, err := parser.ParseSrc(source)

		if e, ok := err.(*parser.Error); ok {
			es := e.Error()
			if strings.HasPrefix(es, "syntax error: unexpected") {
				if strings.HasPrefix(es, "syntax error: unexpected $end,") {
					following = true
					continue
				}
			} else {
				if e.Pos.Column == len(source) && !e.Fatal {
					fmt.Fprintln(os.Stderr, e)
					following = true
					continue
				}
				if e.Error() == "unexpected EOF" {
					following = true
					continue
				}
			}
		}

		following = false
		source = ""
		var v interface{}

		if err == nil {
			v, err = vm.Run(stmts, env)
		}
		if err != nil {
			if e, ok := err.(*vm.Error); ok {
				fmt.Fprintf(os.Stderr, "%d:%d %s\n", e.Pos.Line, e.Pos.Column, err)
			} else if e, ok := err.(*parser.Error); ok {
				fmt.Fprintf(os.Stderr, "%d:%d %s\n", e.Pos.Line, e.Pos.Column, err)
			} else {
				fmt.Fprintln(os.Stderr, err)
			}
			continue
		}

		fmt.Printf("StatementReturn: %#v\n\n", v)
	}
}
