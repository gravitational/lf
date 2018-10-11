package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gravitational/lf/lf"

	"github.com/gravitational/trace"
	"github.com/gravitational/version"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var exitCode int
	var err error

	if err = run(); err != nil {
		fmt.Println(err.Error())
		exitCode = 255
	}
	os.Exit(exitCode)
}

func run() error {
	var (
		app   = kingpin.New("lf", "LF is a log format key value database tool")
		debug = app.Flag("debug", "Enable debug mode").Short('d').Bool()
		dir   = app.Flag("dir", "Directory with data").Default(".").String()

		// commands
		cversion = app.Command("version", "Print version information")

		cget    = app.Command("get", "Get value by key")
		cgetkey = cget.Arg("key", "Key to fetch").Required().String()

		cset    = app.Command("set", "Set key and value")
		csetkey = cset.Arg("key", "Key to set").Required().String()
		csetval = cset.Arg("val", "Value to set").Required().String()
		csetttl = cset.Flag("ttl", "TTL to set").Duration()
	)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed parsing command line arguments: %s.\nTry lf --help.\n", err.Error())
		return err
	}

	if *debug {
		log.SetOutput(os.Stderr)
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetOutput(os.Stderr)
		log.SetLevel(log.WarnLevel)
	}

	switch cmd {

	// "version" command
	case cversion.FullCommand():
		version.Print()
	case cget.FullCommand():
		return get(*dir, *cgetkey)
	case cset.FullCommand():
		var expires time.Time
		if *csetttl > 0 {
			expires = time.Now().UTC().Add(*csetttl)
		}
		return set(*dir, lf.Item{
			Key:     []byte(*csetkey),
			Val:     []byte(*csetval),
			Expires: expires,
		})
	default:
		return trace.BadParameter("unknown command")
	}
	return nil
}

func get(dir string, key string) error {
	l, err := lf.NewDirLog(lf.DirLogConfig{
		Dir: dir,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l.Close()

	item, err := l.Get([]byte(key))
	if err != nil {
		return trace.Wrap(err)
	}
	_, err = os.Stdout.Write(item.Val)
	return trace.ConvertSystemError(err)
}

func set(dir string, item lf.Item) error {
	log.Debugf("Setting item key: %v val:%v expires: %v", string(item.Key), string(item.Val), item.Expires)
	l, err := lf.NewDirLog(lf.DirLogConfig{
		Dir: dir,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l.Close()

	err = l.Put(item)
	if err != nil {
		return trace.Wrap(err)
	}
	return nil
}
