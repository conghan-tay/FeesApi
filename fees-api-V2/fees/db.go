package fees

import "encore.dev/storage/sqldb"

var db = sqldb.NewDatabase("feesdb", sqldb.DatabaseConfig{
	Migrations: "migrations",
})
