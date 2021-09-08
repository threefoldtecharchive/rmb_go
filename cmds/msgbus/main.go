package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/threefoldtech/rmb"
)

type flags struct {
	twin      int
	substrate string
	debug     string
	redis     string
	http      bool
}

func (f *flags) Valid() error {
	if f.twin == -1 {
		return fmt.Errorf("twin id is required")
	}
	return nil
}

func main() {
	var f flags
	flag.IntVar(&f.twin, "twin", -1, "the twin id")
	flag.StringVar(&f.substrate, "substrate", "wss://explorer.devnet.grid.tf/ws", "substrate url")
	flag.StringVar(&f.debug, "log-level", "debug", "log level [debug|info|warn|error|fatal|panic]")
	flag.StringVar(&f.redis, "redis", "127.0.0.1:6379", "redis url")
	flag.BoolVar(&f.http, "http", false, "Allow rmb to accept http requests")

	flag.Parse()

	if err := f.Valid(); err != nil {
		flag.PrintDefaults()
		log.Fatal().Err(err).Msg("invalid arguments")
	}

	setupLogging(f.debug)

	if err := app(f); err != nil {
		log.Fatal().Msg(err.Error())
	}
}

func app(f flags) error {
	s, err := rmb.NewServer(f.substrate, f.redis, f.twin, f.http)
	if err != nil {
		return errors.Wrap(err, "failed to create server")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-ch
		log.Info().Msg("shutting down...")
		cancel()
	}()

	if err := s.Serve(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return errors.Wrap(err, "server exited unexpectedly")
	}

	return nil
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
