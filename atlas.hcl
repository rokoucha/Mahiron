env "local" {
  src = "file://internal/db/schema.sql"
  dev = "sqlite://file?mode=memory&_fk=1"

  migration {
    dir = "file://internal/db/migrations"
  }
}

env "data_broadcast_cache" {
  src = "file://internal/stream/databroadcast/cachedb/schema.sql"
  dev = "sqlite://file?mode=memory&_fk=1"

  migration {
    dir = "file://internal/stream/databroadcast/cachedb/migrations"
  }
}
