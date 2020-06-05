package tarantoolapi

import (
	"fmt"

	"github.com/tarantool/go-tarantool"
)
const _DebugEnabled = false

// tuple is used to store any number of incoming variables and pass them to TarantoolDB
type tuple = []interface{}

// TupleID is just for flex
type TupleID = string

// TarantoolIndex too
type TarantoolIndex = string

// Tarantool represents tarantool db connection
type Tarantool struct {
	options    tarantool.Opts
	connection *tarantool.Connection
	//schema     *tarantool.Connection.Schema
}

// Connect allows to initialize Tarantool with connection
func (t *Tarantool) connect(address string, options tarantool.Opts) {
	t.options = options
	connection, err := tarantool.Connect(address, t.options)
	if err != nil && _DebugEnabled {
		fmt.Println("Debug> Tarantool.go>\nDebug> Tarantool.go> Error: Tarantool connection refused with error = ", err, "\n")
	} else {
		t.connection = connection
	}
}

// printSpacesList: Debug function only works with debug enabled - prints list of spaces
func (t *Tarantool) printSpacesList() {
	if _DebugEnabled {
		fmt.Printf("Debug> Tarantool.go>\nDebug> Tarantool.go> List of spaces in connection:\n")
		for _, values := range t.connection.Schema.Spaces {
			print("Debug> Tarantool.go> id: ", values.Id, ", name: ", values.Name, "\n")
		}
	}
}

// executeLua executes plain lua code
func (t *Tarantool) executeLua(code string) tuple {
	resp, err := t.connection.Eval(code, tuple{})

	if _DebugEnabled {
		fmt.Println("Debug> Tarantool.go> Exectuted :`", code, "`")
		fmt.Println("Debug> Tarantool.go> Tarantool response> Error = ", err)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Code = ", resp.Code)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Data = ", resp.Data)
	}

	return resp.Data
}

// TarantoolSpace is a table in Tarantool
type TarantoolSpace struct {
	name   string
	server *Tarantool
	schema string
}

func (s *TarantoolSpace) connectTarantool(t *Tarantool) {
	s.server = t
}

func (s *TarantoolSpace) initializeSpace(spaceName string, schema string) bool {
	s.name = spaceName
	s.schema = schema
	resp, err := s.server.connection.Eval(`box.schema.create_space('`+spaceName+`')`, tuple{})

	if _DebugEnabled {
		fmt.Println("Debug> Tarantool.go> Added space")
		fmt.Println("Debug> Tarantool.go> Tarantool response> Error = ", err)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Code = ", resp.Code)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Data = ", resp.Data)
	}

	resp, err2 := s.server.connection.Eval(`box.space.`+s.name+`:format({`+s.schema+`})`, tuple{})

	if _DebugEnabled {
		fmt.Println("Debug> Tarantool.go> Added space format")
		fmt.Println("Debug> Tarantool.go> Tarantool response> Error = ", err2)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Code = ", resp.Code)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Data = ", resp.Data)
	}

	return (err == nil) && (err2 == nil)
}

func (s *TarantoolSpace) initializeSpaceWDB(t *Tarantool, spaceName string, format string) {
	s.connectTarantool(t)
	s.initializeSpace(spaceName, format)
}

//newIndex adds index to space
func (s *TarantoolSpace) newIndex(indexName string, structure string) TarantoolIndex {
	resp, err := s.server.connection.Eval(`box.space.`+s.name+`:create_index ('`+indexName+`', {
		`+structure+`
		})`, tuple{})

	if _DebugEnabled {
		fmt.Println("Debug> Tarantool.go> Added index")
		fmt.Println("Debug> Tarantool.go> Tarantool response> Error = ", err)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Code = ", resp.Code)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Data = ", resp.Data)
	}
	return indexName
}

//insert inserts line into space
func (s *TarantoolSpace) add(line tuple) bool {
	resp, err := s.server.connection.Insert(s.name, line)

	if _DebugEnabled {
		fmt.Println("Debug> Tarantool.go> Inserted ", line)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Error = ", err)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Code = ", resp.Code)
		fmt.Println("Debug> Tarantool.go> Tarantool response> Data = ", resp.Data)
	}

	return err == nil
}

//get selects line with query
func (s *TarantoolSpace) get(start uint32, offset uint32, iterator uint32, index TarantoolIndex, query tuple) tuple {
	resp, err := s.server.connection.Select(s.name, index, start, offset, iterator, query)
	if _DebugEnabled {
		fmt.Println("Debug>\nDebug> Select")
		fmt.Println("Debug> Tarantool.go> Error", err)
		fmt.Println("Debug> Tarantool.go> Code", resp.Code)
		fmt.Println("Debug> Tarantool.go> Data", resp.Data)
	}
	return resp.Data
}

// getAllGq selects everything greater
func (s *TarantoolSpace) getAll(index TarantoolIndex) tuple {
	return s.get(1, 4294967295, tarantool.IterGt, index, tuple{""})
}

// getElementByID allows you to get element by unique string id, like in random_id.go
func (s *TarantoolSpace) getElementByID(id TupleID, index TarantoolIndex) tuple {
	return s.get(0, 1, tarantool.IterEq, index, tuple{id})
}

func (s *TarantoolSpace) delete(id TupleID, index TarantoolIndex) bool {
	resp, err := s.server.connection.Delete(s.name, index, tuple{id})
	if _DebugEnabled {
		fmt.Println("Debug>\nDebug> Delete")
		fmt.Println("Debug> Tarantool.go> Error", err)
		fmt.Println("Debug> Tarantool.go> Code", resp.Code)
		fmt.Println("Debug> Tarantool.go> Data", resp.Data)
	}

	return err == nil
}

func (s *TarantoolSpace) replace(line tuple) bool {

	resp, err := s.server.connection.Replace(s.name, line)
	if _DebugEnabled {
		fmt.Println("Debug>\nDebug> Delete")
		fmt.Println("Debug> Tarantool.go> Error", err)
		fmt.Println("Debug> Tarantool.go> Code", resp.Code)
		fmt.Println("Debug> Tarantool.go> Data", resp.Data)
	}

	return err == nil
}

//resp, err = client.Update(spaceNo, indexNo, []interface{}{uint(13)}, []interface{}{[]interface{}{"+", 1, 3}})
//resp, err = client.Upsert(spaceNo, []interface{}{uint(15), 1}, []interface{}{[]interface{}{"+", 1, 1}}) // insert 1 or do 2
//resp, err = client.Call("func_name", []interface{}{1, 2, 3})
