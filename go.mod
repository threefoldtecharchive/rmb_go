module github.com/threefoldtech/go-rmb

go 1.16

require (
	github.com/ChainSafe/go-schnorrkel v1.0.0
	github.com/go-redis/redis/v8 v8.11.1
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/gtank/merlin v0.1.1
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.26.0
	github.com/stretchr/testify v1.7.0
	github.com/threefoldtech/substrate-client v0.0.0-20220629072807-544d41e5b81a
)

replace github.com/centrifuge/go-substrate-rpc-client/v4 v4.0.2 => github.com/threefoldtech/go-substrate-rpc-client/v4 v4.0.3-0.20220629062423-919d421680f4
