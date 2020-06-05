# tarantool
This is a simple Tarantool connector library that uses go-tarantool

Examples

## Connect
```
	tarantoolDB := new(Tarantool)
  
	tarantoolDB.connect("127.0.0.1:3301", tarantool.Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
		User:          "guest",
		Pass:          "",
	})
```

## Initialize new space
```
laptops := new(TarantoolSpace)
laptops.initializeSpaceWDB(tarantoolDB, "Laptops", `
		{name = 'guid', type = 'string'},
		{name = 'name', type = 'string'},
		{name = 'serial', type = 'string'}
`)
```

## Creating index
```
secondary := laptops.newIndex("secondary", `type = 'tree', parts = {'id'}`)
```

## Adding element
```
newid := serial.generateTupleID("")
laptops.add(tuple{newid, "Apple MacBook Pro 13", "HX3Y3H78DE6G32"})
```

## Getting by ID
```
laptopOne := laptops.getElementByID(newid, secondary)
```

## Getting everything
```
allLaptops := laptops.getAll(secondary)

for index, laptop := range allLaptops {
  current := laptop.([]interface{})
  fmt.Printf("Found laptop # %d: {id = %s, name = %s, serial = %s}\n", index, current[0], current[1], current[2])
}
```

## Deleting element
```
laptops.delete(newid, secondary)
```
