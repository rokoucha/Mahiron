env "local" {
  src = "file://internal/db/schema.sql"
  dev = "sqlite://file?mode=memory&_fk=1"

  migration {
    dir = "file://internal/db/migrations"
  }
}

env "data_broadcast_cache" {
  src = "file://internal/bml/cache/cachedb/schema.sql"
  dev = "sqlite://file?mode=memory&_fk=1"

  migration {
    dir = "file://internal/bml/cache/cachedb/migrations"
  }
}
