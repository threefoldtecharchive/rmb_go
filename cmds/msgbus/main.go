package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/rmb"
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
	flag.StringVar(&f.substrate, "substrate", "wss://explorer.devnet.grid.tf/ws", "substrate url")
	flag.StringVar(&f.debug, "log-level", "debug", "log level [debug|info|warn|error|fatal|panic]")
	flag.StringVar(&f.redis, "redis", "127.0.0.1:6379", "redis url")

	flag.Parse()
	setupLogging(f.debug)
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

const (
	colorBlack = iota + 30
	colorRed
	colorGreen
	colorYellow
	colorBlue
	colorMagenta
	colorCyan
	colorWhite

	colorBold     = 1
	colorDarkGray = 90
)

// colorize returns the string s wrapped in ANSI code c, unless disabled is true.
func colorize(s interface{}, c int) string {
	return fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
}

func formatLevel(i interface{}) string {
	var l string
	if ll, ok := i.(string); ok {
		switch ll {
		case "debug":
			l = colorize(ll, colorBlue)
		case "info":
			l = colorize(ll, colorGreen)
		case "warn":
			l = colorize(ll, colorYellow)
		case "error":
			l = colorize(colorize(ll, colorRed), colorBold)
		case "fatal":
			l = colorize(colorize(ll, colorRed), colorBold)
		case "panic":
			l = colorize(colorize(ll, colorRed), colorBold)
		default:
			l = colorize("???", colorBold)
		}
	} else {
		if i == nil {
			l = colorize("???", colorBold)
		} else {
			l = strings.ToUpper(fmt.Sprintf("%s", i))[0:3]
		}
	}
	return l
}

func setupLogging(level string) {
	if level == "debug" {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else if level == "info" {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else if level == "warn" {
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	} else if level == "error" {
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	} else if level == "fatal" {
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	} else if level == "panic" {
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{
		TimeFormat:  time.RFC3339,
		Out:         os.Stdout,
		FormatLevel: formatLevel,
	})
}
