package main

import (
	"flag"
	"net/http"

	"github.com/dmahmouali/rmb-go/pkg/rmb"
	"github.com/gorilla/mux"
	"github.com/rs/zerolog/log"
)

type flags struct {
	twin      int
	substrate string
	debug     string
	redis     string
}

func main() {
	f := flags{}
	flag.IntVar(&f.twin, "twin", -1, "the twin id")
	flag.StringVar(&f.substrate, "substrate", "https://explorer.devnet.grid.tf/", "substrate url")
	flag.StringVar(&f.debug, "debug", "none", "debug [all or none]")
	flag.StringVar(&f.redis, "redis", "127.0.0.1:6379", "redis url")

	flag.Parse()

	if f.twin == -1 {
		println("twin flag is required")
		return
	}

	s, err := createServer(f)
	if err != nil {
		panic(err)
	}

	if err := s.ListenAndServe(); err != nil {
		if err == http.ErrServerClosed {
			log.Info().Msg("server stopped gracefully")
		} else {
			log.Error().Err(err).Msg("server stopped unexpectedly")
		}
	}
}

func createServer(f flags) (*http.Server, error) {
	router := mux.NewRouter()
	debug := false
	substrate := f.substrate
	redis := f.redis
	twin := f.twin
	if f.debug == "all" {
		debug = true
	}

	rmb.Setup(router, debug, substrate, redis, twin)
	return &http.Server{
		Handler: router,
		Addr:    "0.0.0.0:8051",
	}, nil
}
