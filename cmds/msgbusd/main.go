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
	"github.com/threefoldtech/go-rmb"
	"github.com/threefoldtech/substrate-client"
)

type flags struct {
	localConfig bool
  substrate string
	debug     string
	redis     string
	mnemonics string
	key_type  string
	workers   int
  publish bool
}

func (f *flags) Valid() error {
	if f.mnemonics == "" {
		return fmt.Errorf("mnemonics id is required")
	}
	return nil
}

var Flags *flags

func main() {
	var f flags
	flag.StringVar(&f.substrate, "substrate", "wss://tfchain.grid.tf", "substrate url")
	flag.StringVar(&f.debug, "log-level", "info", "log level [debug|info|warn|error|fatal|panic]")
	flag.StringVar(&f.mnemonics, "mnemonics", "", "mnemonics")
	flag.StringVar(&f.key_type, "key-type", "sr25519", "key type")
	flag.IntVar(&f.workers, "workers", 1000, "workers is number of active channels that communicate with the backend")
  flag.BoolVar(&Flags.localConfig, "localconfig", true, "Local endpoint that overrides substrate lookup")
	flag.BoolVar(&Flags.publish, "publish", false, "Enable publish instead of push on redis")

	flag.Parse()

	if err := Flags.Valid(); err != nil {
		flag.PrintDefaults()
		log.Fatal().Err(err).Msg("invalid arguments")
	}

	setupLogging(Flags.debug)
	log.Debug().Bool("flags", Flags.publish).Msg("huts")

	if err := app(); err != nil {
		log.Fatal().Msg(err.Error())
	}
}


func constructSigner(mnemonics string, key_type string) (substrate.Identity, error) {
	if key_type == "ed25519" {
		return substrate.NewIdentityFromEd25519Phrase(mnemonics)
	} else if key_type == "sr25519" {
		return substrate.NewIdentityFromSr25519Phrase(mnemonics)
	} else {
		return nil, fmt.Errorf("unrecognized key type %s", key_type)
	}
}

func app() error {
	identity, err := constructSigner(f.mnemonics, f.key_type)
	if err != nil {
		return err
	}
  backend := rmb.NewRedisBackend(Flags.redis, Flags.publish)
	var res rmb.TwinResolver
	var err error

	if Flags.localConfig {
		res, err = rmb.NewLocalTwinResolver()
	} else {
		res, err = rmb.NewSubstrateResolver(Flags.substrate)
	}

	if err != nil {
		return errors.Wrap(err, "couldn't get a client to explorer resolver")
	}
  
	s, err := rmb.NewServer(res, *backend, Flags.workers, identity)

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
