module e2e

go 1.25.6

require github.com/norma-core/normfs/normfs_go v0.0.0

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/norma-core/norma-core/shared/gremlin_go v0.0.0-20260213171720-ca7c2eed0f31 // indirect
)

replace github.com/norma-core/normfs/normfs_go => ../../../normfs-src/normfs_go
