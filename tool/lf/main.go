package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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
		cgetKey = cget.Arg("key", "Key to fetch").Required().String()

		cset    = app.Command("set", "Set key and value")
		csetKey = cset.Arg("key", "Key to set").Required().String()
		csetVal = cset.Arg("val", "Value to set").Required().String()
		csetTTL = cset.Flag("ttl", "TTL to set").Duration()

		cls         = app.Command("ls", "List keys")
		clsPrefix   = cls.Arg("prefix", "Prefix to list").Required().String()
		clsWithIDs  = cls.Flag("ids", "Show record ids").Default("false").Bool()
		clsWithVals = cls.Flag("vals", "Show values").Default("false").Bool()

		cwatch         = app.Command("watch", "Watch prefix")
		cwatchPrefix   = cwatch.Arg("prefix", "Prefix to watch").String()
		cwatchRecordID = cwatch.Flag("id", "Record it to start watch at").Int64()

		ccompact = app.Command("compact", "Compact database")
		crepair  = app.Command("repair", "Repair database")
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
		return get(*dir, *cgetKey)
	case cls.FullCommand():
		return ls(*dir, *clsPrefix, *clsWithIDs, *clsWithVals)
	case cwatch.FullCommand():
		return watch(setupSignalHandlers(), *dir, *cwatchPrefix, *cwatchRecordID)
	case ccompact.FullCommand():
		return compact(setupSignalHandlers(), *dir)
	case crepair.FullCommand():
		return repair(setupSignalHandlers(), *dir)
	case cset.FullCommand():
		var expires time.Time
		if *csetTTL > 0 {
			expires = time.Now().UTC().Add(*csetTTL)
		}
		return set(*dir, lf.Item{
			Key:     []byte(*csetKey),
			Val:     []byte(*csetVal),
			Expires: expires,
		})
	default:
		return trace.BadParameter("unknown command")
	}
	return nil
}

// setupSignalHandlers sets up a handler to handle common unix process signal traps.
// Some signals are handled to avoid the default handling which might be termination (SIGPIPE, SIGHUP, etc)
// The rest are considered as termination signals and the handler initiates shutdown upon receiving
// such a signal.
func setupSignalHandlers() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	go func() {
		defer cancel()
		for sig := range c {
			log.Debugf("Received a %s signal, exiting...", sig)
			return
		}
	}()
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	return ctx
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

func ls(dir string, prefix string, showIDs, showVals bool) error {
	l, err := lf.NewDirLog(lf.DirLogConfig{
		Dir: dir,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l.Close()

	result, err := l.GetRange([]byte(prefix), lf.Range{MatchPrefix: true})
	if err != nil {
		return trace.Wrap(err)
	}
	for _, item := range result.Items {
		fmt.Fprintln(os.Stdout, itemToString(item, showIDs, showVals))
	}
	return nil
}

func watch(ctx context.Context, dir string, prefix string, recordID int64) error {
	l, err := lf.NewDirLog(lf.DirLogConfig{
		Dir:     dir,
		Context: ctx,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l.Close()

	var offset *lf.Offset
	if recordID != 0 {
		offset = &lf.Offset{
			SchemaVersion: lf.V1,
			RecordID:      uint64(recordID),
		}
	}
	watcher, err := l.NewWatcher([]byte(prefix), offset)
	if err != nil {
		return trace.Wrap(err)
	}
	defer watcher.Close()

	for {
		select {
		case event := <-watcher.Events():
			fmt.Fprintln(os.Stdout, eventToString(event))
		case <-watcher.Done():
			fmt.Println("Watcher has exited.")
			return nil
		case <-ctx.Done():
			fmt.Println("Interrupted.")
			return nil
		}
	}
}

func compact(ctx context.Context, dir string) error {
	l, err := lf.NewDirLog(lf.DirLogConfig{
		Context: ctx,
		Dir:     dir,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l.Close()

	if err := l.Compact(ctx); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func repair(ctx context.Context, dir string) error {
	l, err := lf.NewDirLog(lf.DirLogConfig{
		Context: ctx,
		Dir:     dir,
		Repair:  true,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	defer l.Close()
	return nil
}

func itemToString(item lf.Item, showID, showVal bool) string {
	out := &bytes.Buffer{}
	if showID {
		fmt.Fprintf(out, "%v\t", item.ID)
	}
	fmt.Fprintf(out, "%v", string(item.Key))
	if showVal {
		if item.Val != nil {
			fmt.Fprintf(out, "\t%v ", string(item.Val))
		} else {
			fmt.Fprintf(out, "<empty>")
		}
	}
	return out.String()
}

func eventToString(event lf.Record) string {
	out := &bytes.Buffer{}
	fmt.Fprintf(out, "%v %v ", event.ID, event.Type)
	if event.Key != nil {
		fmt.Fprintf(out, "%v ", string(event.Key))
	}
	if event.Val != nil {
		fmt.Fprintf(out, "%v ", string(event.Val))
	}
	return out.String()
}
