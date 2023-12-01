package parser

import (
	"encoding/json"
	"github.com/bytecodealliance/wasmtime-go"
)

type DuckDBParser struct {
}

var engine *wasmtime.Engine
var store *wasmtime.Store
var module *wasmtime.Module
var linker *wasmtime.Linker

func InitDuckDBParser() error {
	var err error
	engine = wasmtime.NewEngine()
	store = wasmtime.NewStore(engine)
	module, err = wasmtime.NewModuleFromFile(engine, "gw_dsl_parser.wasm")
	if err != nil {
		return err
	}
	for _, o := range module.Imports() {
		println(*o.Name())
	}

	linker = wasmtime.NewLinker(engine)

	e := linker.DefineWasi()
	if e != nil {
		return err
	}
	return nil
}
func NewDuckDBParser() DuckDBParser {
	return DuckDBParser{}
}

func (p DuckDBParser) Parse(dataset Dataset, dsl GraphicWalkerDSL, meta string) (string, error) {
	instance, err := linker.Instantiate(store, module)
	if err != nil {
		return "", err
	}
	dslStr, err := json.Marshal(dsl)
	if err != nil {
		return "", err
	}

	allocate := instance.GetExport(store, "allocate").Func()
	deallocate := instance.GetExport(store, "deallocate").Func()
	tableAddress, err := writeStringToWasm(instance, store, allocate, dataset.Source)
	if err != nil {
		return "", err
	}
	dslAddress, err := writeStringToWasm(instance, store, allocate, string(dslStr))
	if err != nil {
		return "", err
	}
	metaAddress, err := writeStringToWasm(instance, store, allocate, string(meta))
	if err != nil {
		return "", err
	}
	var wasmFunc *wasmtime.Func
	if dataset.Type == "table" {
		wasmFunc = instance.GetExport(store, "parser_dsl_with_table").Func()
	} else {
		wasmFunc = instance.GetExport(store, "parser_dsl_with_view").Func()
	}
	val, err := wasmFunc.Call(store, tableAddress, dslAddress, metaAddress)
	if err != nil {
		return "", err
	}

	res := readStringFromWasm(instance, store, val.(int32))
	//deallocate
	deallocate.Call(store, tableAddress)
	deallocate.Call(store, dslAddress)
	deallocate.Call(store, val)
	return res, nil
}

const wasmMemory = "memory"

// a string is serialized as 4 byte length + content + trailing zero
func writeStringToWasm(inst *wasmtime.Instance, store *wasmtime.Store, fn *wasmtime.Func, s string) (int32, error) {
	vaddr, e := fn.Call(store, int32(len(s)+1))
	if e != nil {
		return 0, e
	}
	mem := inst.GetExport(store, wasmMemory).Memory().UnsafeData(store)
	addr := vaddr.(int32)
	copy(mem[addr:], s)
	mem[addr+int32(len(s))] = 0
	return addr, e
}

func readStringFromWasm(inst *wasmtime.Instance, store *wasmtime.Store, addr int32) string {
	mem := inst.GetExport(store, wasmMemory).Memory().UnsafeData(store)
	for i := 0; ; i++ {
		if mem[addr+int32(i)] == 0 {
			return string(mem[addr : addr+int32(i)])
		}
	}
	return ""
}
