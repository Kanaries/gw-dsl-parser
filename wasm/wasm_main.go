//go:build js && wasm
// +build js,wasm

package main

import (
	_ "crypto/sha512"
	"encoding/json"
	"syscall/js"

	"github.com/foghorn-tech/kanaries-dsl/parser"
)

func dslToSql(this js.Value, args []js.Value) interface{} {
	DatasetStr := []byte(args[0].String())
	PayloadStr := []byte(args[1].String())

	var dataset parser.Dataset
	var payload parser.GraphicWalkerDSL
	json.Unmarshal(DatasetStr, &dataset)
	json.Unmarshal(PayloadStr, &payload)

	sql, _ := parser.BaseParser{}.Parse(dataset, payload)
	return sql
}

// export wasm file: `GOOS=js GOARCH=wasm go build -o main.wasm wasm_main.go`
func main() {
	done := make(chan struct{}, 0)
	js.Global().Set("dslToSql", js.FuncOf(dslToSql))
	<-done
}
