package main

import (
	/*
		#include<stdbool.h>
	*/
	"C"
)
import (
	"encoding/json"

	"github.com/kanaries/gw-dsl-parser/parser"
)

func parseByString(DatasetStr string, PayloadStr string) string {
	var dataset parser.Dataset
	var payload parser.GraphicWalkerDSL
	json.Unmarshal([]byte(DatasetStr), &dataset)
	json.Unmarshal([]byte(PayloadStr), &payload)

	sql, _ := parser.BaseParser{}.Parse(dataset, payload)
	return sql
}

//export ParseByString
func ParseByString(DatasetStr *C.char, PayloadStr *C.char) *C.char {
	s1 := C.GoString(DatasetStr)
	s2 := C.GoString(PayloadStr)
	result := parseByString(s1, s2)
	return C.CString(result)
}

// go build -buildmode=c-shared -o dslToSql.so ./so/so_main.go
func main() {
}
