# DB Listener - Listen to data inserts, updates and deletes

## Features:
- Install listeners for data inserts, updates and deletes to any number of tables in a database using pq (github.com/pq).
- Provide a DB connection string to connect to a DB instance, then register to listen for data inserts, updates and deletes for any number of tables.
- Listener is notified when any row is inserted, updated or deleted in any registered table.

## Example:
- Connect to a database and listen to two tables name *oprequests* and *opaborts*:

```
./dblisten -connect="dbname=qctldb user=postgres host=localhost" -tables=oprequests,opaborts
```

Changes are notified as follows (in this example, we're printing notifications to ```stdout```)

```
{"table" : "oprequests", "action" : "INSERT", "data" : {"id":"5392bba2-075c-49da-a330-56e4645f0227","data":{"id": "5392bba2-075c-49da-a330-56e4645f0227", "op": "bmc", "etag": "a199f92b-ab1d-49cf-a9f8-d11dfc5ceacb", "name": "op-5392bba2-075c-49da-a330-56e4645f0227-poweron-poweron", "opcmd": "POWERON", "state": "pending", "pod_id": "e4bc10fa-c974-43ba-a39a-026822fc9c91", "result": "", "status": "", "created": "2022-02-21T00:26:29.37Z", "rack_id": "13657826-c544-43f5-9298-461be793cc86", "modified": "2022-02-21T00:26:29.37Z", "completed": false, "hoster_id": "a95fe9e9-0200-4b60-b2e6-a6549f291a1b", "portal_id": "TestPortal", "machine_id": "187a0f00-3636-4825-936f-6dfcb79743d3", "result_msg": "", "result_info": "", "resource_type": "op", "schema_version": "v1"}}}
```

## Build dblistener

Build ```dblistener``` by changing to the ```main``` directory and running ```go build```

## Functionality

Start listening to data changes in a named set of tables in a DB:

```
dbListener, err := trigger.ListenAndNotify(connect, changeHandler, tables...)
```

where ```connectStr``` is a "connection string" to connect to a DB (e.g. ```dbname=postgres user=postgres host=localhost```) and tables is a comma-separated list of table names (e.g. ```oprequests,opaborts```).

```changeHandler``` is a user defined callback function with this signature:

```type DataChangeHandler func(data *DataChange) bool```

The main program uses this changeHandler which simply prints any received data-change events to STDOUT.

```
func changeHandler(dataChange *trigger.DataChange) bool {
	// nolint
	fmt.Printf("%s\t%s\t%s\n", dataChange.Table, dataChange.Type, dataChange.Data)

	return false
}
```