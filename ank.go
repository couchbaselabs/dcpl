package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	anko_math "github.com/mattn/anko/builtins/math"
	anko_regexp "github.com/mattn/anko/builtins/regexp"
	anko_sort "github.com/mattn/anko/builtins/sort"
	anko_strings "github.com/mattn/anko/builtins/strings"
	"github.com/mattn/anko/parser"

	"github.com/mattn/anko/vm"
)

func jsonUnmarshal(input []byte, out ...interface{}) (interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	var output interface{}
	if len(out) > 0 {
		output = out[0]
	}
	err := decoder.Decode(&output)
	return output, err
}

func jsonImport(env *vm.Env) *vm.Env {
	m := env.NewEnv()
	m.Define("Marshal", reflect.ValueOf(json.Marshal))
	m.Define("Unmarshal", reflect.ValueOf(jsonUnmarshal))
	return m
}

func timeImport(env *vm.Env) *vm.Env {
	m := env.NewEnv()
	m.Define("ANSIC", reflect.ValueOf(time.ANSIC))
	m.Define("Date", reflect.ValueOf(time.Date))
	m.Define("FixedZone", reflect.ValueOf(time.FixedZone))
	m.Define("Hour", reflect.ValueOf(time.Hour))
	m.Define("Kitchen", reflect.ValueOf(time.Kitchen))
	m.Define("LoadLocation", reflect.ValueOf(time.LoadLocation))
	m.Define("Local", reflect.ValueOf(time.Local))
	m.Define("Microsecond", reflect.ValueOf(time.Microsecond))
	m.Define("Millisecond", reflect.ValueOf(time.Millisecond))
	m.Define("Minute", reflect.ValueOf(time.Minute))
	m.Define("Nanosecond", reflect.ValueOf(time.Nanosecond))
	m.Define("Now", reflect.ValueOf(time.Now))
	m.Define("Parse", reflect.ValueOf(time.Parse))
	m.Define("ParseDuration", reflect.ValueOf(time.ParseDuration))
	m.Define("ParseInLocation", reflect.ValueOf(time.ParseInLocation))
	m.Define("RFC1123", reflect.ValueOf(time.RFC1123))
	m.Define("RFC1123Z", reflect.ValueOf(time.RFC1123Z))
	m.Define("RFC3339", reflect.ValueOf(time.RFC3339))
	m.Define("RFC3339Nano", reflect.ValueOf(time.RFC3339Nano))
	m.Define("RFC822", reflect.ValueOf(time.RFC822))
	m.Define("RFC822Z", reflect.ValueOf(time.RFC822Z))
	m.Define("RFC850", reflect.ValueOf(time.RFC850))
	m.Define("RubyDate", reflect.ValueOf(time.RubyDate))
	m.Define("Second", reflect.ValueOf(time.Second))
	m.Define("Since", reflect.ValueOf(time.Since))
	m.Define("Stamp", reflect.ValueOf(time.Stamp))
	m.Define("StampMicro", reflect.ValueOf(time.StampMicro))
	m.Define("StampMilli", reflect.ValueOf(time.StampMilli))
	m.Define("StampNano", reflect.ValueOf(time.StampNano))
	m.Define("UTC", reflect.ValueOf(time.UTC))
	m.Define("Unix", reflect.ValueOf(time.Unix))
	m.Define("UnixDate", reflect.ValueOf(time.UnixDate))

	return m
}

func NewAnkEnv() *vm.Env {
	env := vm.NewEnv()
	tbl := map[string]func(env *vm.Env) *vm.Env{
		"json":    jsonImport,
		"time":    timeImport,
		"math":    anko_math.Import,
		"regexp":  anko_regexp.Import,
		"sort":    anko_sort.Import,
		"strings": anko_strings.Import,
	}

	env.Define("import", reflect.ValueOf(func(s string) interface{} {
		if loader, ok := tbl[s]; ok {
			return loader(env)
		}
		panic(fmt.Sprintf("package '%s' not found", s))
	}))

	env.Define("len", reflect.ValueOf(func(v interface{}) int64 {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Interface {
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.String {
			return int64(len([]byte(rv.String())))
		}
		if rv.Kind() != reflect.Array && rv.Kind() != reflect.Slice {
			panic("Argument #1 should be array")
		}
		return int64(rv.Len())
	}))

	env.Define("keys", reflect.ValueOf(func(v interface{}) []string {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Interface {
			rv = rv.Elem()
		}
		if rv.Kind() != reflect.Map {
			panic("Argument #1 should be map")
		}
		keys := []string{}
		mk := rv.MapKeys()
		for _, key := range mk {
			keys = append(keys, key.String())
		}
		return keys
	}))

	env.Define("range", reflect.ValueOf(func(args ...int64) []int64 {
		if len(args) < 1 {
			panic("Missing arguments")
		}
		if len(args) > 2 {
			panic("Too many arguments")
		}
		var min, max int64
		if len(args) == 1 {
			min = 0
			max = args[0] - 1
		} else {
			min = args[0]
			max = args[1]
		}
		arr := []int64{}
		for i := min; i <= max; i++ {
			arr = append(arr, i)
		}
		return arr
	}))

	env.Define("toBytes", reflect.ValueOf(func(s string) []byte {
		return []byte(s)
	}))

	env.Define("toRunes", reflect.ValueOf(func(s string) []rune {
		return []rune(s)
	}))

	env.Define("toString", reflect.ValueOf(func(v interface{}) string {
		return fmt.Sprint(v)
	}))

	env.Define("toInt", reflect.ValueOf(func(v interface{}) int64 {
		nt := reflect.TypeOf(1)
		rv := reflect.ValueOf(v)
		if rv.Type().ConvertibleTo(nt) {
			return 0
		}
		return rv.Convert(nt).Int()
	}))

	env.Define("toFloat", reflect.ValueOf(func(v interface{}) float64 {
		nt := reflect.TypeOf(1.0)
		rv := reflect.ValueOf(v)
		if rv.Type().ConvertibleTo(nt) {
			return 0.0
		}
		return rv.Convert(nt).Float()
	}))

	env.Define("toBool", reflect.ValueOf(func(v interface{}) bool {
		nt := reflect.TypeOf(true)
		rv := reflect.ValueOf(v)
		if rv.Type().ConvertibleTo(nt) {
			return false
		}
		return rv.Convert(nt).Bool()
	}))

	env.Define("toChar", reflect.ValueOf(func(s rune) string {
		return string(s)
	}))

	env.Define("toRune", reflect.ValueOf(func(s string) rune {
		if len(s) == 0 {
			return 0
		}
		return []rune(s)[0]
	}))

	env.Define("string", reflect.ValueOf(func(b []byte) string {
		return string(b)
	}))

	env.Define("typeof", reflect.ValueOf(func(v interface{}) string {
		return reflect.TypeOf(v).String()
	}))

	env.Define("defined", reflect.ValueOf(func(s string) bool {
		_, err := env.Get(s)
		return err == nil
	}))
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
#     json = import("json")
#     time = import("time")
#
#     func filter(event) {
#       if event.Type != "mutation" {
#         return false
#       }
#       releaseDate, err = time.Parse(time.RFC3339, "2015-08-15T01:01:42+08:00")
#       if err != nil {
#         return false
#       }
#       val, err = json.Unmarshal(event.Value)
#       if err != nil {
#         return false
#       }
#       documentDate, err = time.Parse(time.RFC3339, toString(val.LastViewDate))
#       if err != nil {
#         return false
#       }
#       return val.Type == "ad-view" && documentDate.After(releaseDate)
#     }
#
#     filter(event)

`)
	var (
		code      string
		following bool
	)

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
	env.Define("event", reflect.ValueOf(event))
	reader := bufio.NewReader(os.Stdin)
	for {
		if following {
			fmt.Print("  ")
		} else {
			fmt.Print("> ")
		}
		b, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		if len(b) == 0 {
			continue
		}
		if code != "" {
			code += "\n"
		}
		code += string(b)

		scanner := new(parser.Scanner)
		scanner.Init(code)
		stmts, err := parser.Parse(scanner)

		if e, ok := err.(*parser.Error); ok {
			if e.Pos.Column == len(b) && !e.Fatal {
				following = true
				continue
			}
			if e.Error() == "unexpected EOF" || strings.Contains(e.Error(), "unexpected $end") {
				following = true
				continue
			}
		}
		following = false
		code = ""
		v := vm.NilValue
		if err == nil {
			v, err = vm.Run(stmts, env)
		}
		if err != nil {
			if e, ok := err.(*vm.Error); ok {
				fmt.Fprintf(os.Stderr, "VM error at %d: %s\n", e.Pos.Line, err)
			} else if e, ok := err.(*parser.Error); ok {
				fmt.Fprintf(os.Stderr, "Parser error at %d: %s\n", e.Pos.Line, err)
			} else {
				fmt.Fprintln(os.Stderr, err)
			}
		} else {
			if v == vm.NilValue || !v.IsValid() {
				fmt.Println("nil")
			} else {
				s, ok := v.Interface().(fmt.Stringer)
				if v.Kind() != reflect.String && ok {
					fmt.Println(s)
				} else {
					fmt.Printf("%#v\n", v.Interface())
				}
			}
		}

	}
}
