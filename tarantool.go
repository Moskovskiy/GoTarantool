package tarantoolapi

import (
	"fmt"

	"github.com/tarantool/go-tarantool"
)

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
	if err == nil {
		t.connection = connection
	}
}

// printSpacesList: Debug function only works with debug enabled - prints list of spaces
func (t *Tarantool) printSpacesList() {

}

// executeLua executes plain lua code
func (t *Tarantool) executeLua(code string) tuple {
	resp, err := t.connection.Eval(code, tuple{})
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
	resp, err2 := s.server.connection.Eval(`box.space.`+s.name+`:format({`+s.schema+`})`, tuple{})
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
	return indexName
}

//insert inserts line into space
func (s *TarantoolSpace) add(line tuple) bool {
	resp, err := s.server.connection.Insert(s.name, line)
	return err == nil
}

//get selects line with query
func (s *TarantoolSpace) get(start uint32, offset uint32, iterator uint32, index TarantoolIndex, query tuple) tuple {
	resp, err := s.server.connection.Select(s.name, index, start, offset, iterator, query)
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
	return err == nil
}

func (s *TarantoolSpace) replace(line tuple) bool {
	resp, err := s.server.connection.Replace(s.name, line)
	return err == nil
}
