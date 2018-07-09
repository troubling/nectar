package nectar

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtext"
)

type CLIInstance struct {
	Arg0 string

	fatal    func(cli *CLIInstance, err error)
	fatalf   func(cli *CLIInstance, frmt string, args ...interface{})
	verbosef func(cli *CLIInstance, frmt string, args ...interface{})

	GlobalFlags               *flag.FlagSet
	globalFlagAuthURL         *string
	globalFlagAuthTenant      *string
	globalFlagAuthUser        *string
	globalFlagAuthKey         *string
	globalFlagAuthPassword    *string
	globalFlagOverrideURLs    *string
	globalFlagStorageRegion   *string
	GlobalFlagVerbose         *bool
	globalFlagContinueOnError *bool
	globalFlagConcurrency     *int
	globalFlagInternalStorage *bool
	globalFlagHeaders         stringListFlag

	BenchDeleteFlags          *flag.FlagSet
	benchDeleteFlagContainers *int
	benchDeleteFlagCount      *int
	benchDeleteFlagCSV        *string
	benchDeleteFlagCSVOT      *string

	BenchGetFlags          *flag.FlagSet
	benchGetFlagContainers *int
	benchGetFlagCount      *int
	benchGetFlagCSV        *string
	benchGetFlagCSVOT      *string
	benchGetFlagIterations *int

	BenchHeadFlags          *flag.FlagSet
	benchHeadFlagContainers *int
	benchHeadFlagCount      *int
	benchHeadFlagCSV        *string
	benchHeadFlagCSVOT      *string
	benchHeadFlagIterations *int

	BenchMixedFlags          *flag.FlagSet
	benchMixedFlagContainers *int
	benchMixedFlagCSV        *string
	benchMixedFlagCSVOT      *string
	benchMixedFlagSize       *int
	benchMixedFlagTime       *string

	BenchPostFlags          *flag.FlagSet
	benchPostFlagContainers *int
	benchPostFlagCount      *int
	benchPostFlagCSV        *string
	benchPostFlagCSVOT      *string

	BenchPutFlags          *flag.FlagSet
	benchPutFlagContainers *int
	benchPutFlagCount      *int
	benchPutFlagCSV        *string
	benchPutFlagCSVOT      *string
	benchPutFlagSize       *int
	benchPutFlagMaxSize    *int

	DownloadFlags       *flag.FlagSet
	downloadFlagAccount *bool

	GetFlags         *flag.FlagSet
	getFlagRaw       *bool
	getFlagNameOnly  *bool
	getFlagMarker    *string
	getFlagEndMarker *string
	getFlagReverse   *bool
	getFlagLimit     *int
	getFlagPrefix    *string
	getFlagDelimiter *string
}

// CLI runs a nectar command-line-interface with the given args (args[0] should
// have the name of the executable). The fatal, fatalf, and verbosef parameters
// may be nil for the defaults. The default fatal and fatalf functions will
// call os.Exit(1) after emitting error (or help) text.
func CLI(args []string, fatal func(cli *CLIInstance, err error), fatalf func(cli *CLIInstance, frmt string, args ...interface{}), verbosef func(cli *CLIInstance, frmt string, args ...interface{})) {
	if fatal == nil {
		fatal = cliFatal
	}
	if fatalf == nil {
		fatalf = cliFatalf
	}
	if verbosef == nil {
		verbosef = cliVerbosef
	}
	cli := &CLIInstance{Arg0: args[0], fatal: fatal, fatalf: fatalf, verbosef: verbosef}
	var flagbuf bytes.Buffer

	cli.GlobalFlags = flag.NewFlagSet(cli.Arg0, flag.ContinueOnError)
	cli.GlobalFlags.SetOutput(&flagbuf)
	cli.globalFlagAuthURL = cli.GlobalFlags.String("A", os.Getenv("AUTH_URL"), "|<url>| URL to auth system, example: http://127.0.0.1:8080/auth/v1.0 - Env: AUTH_URL")
	cli.globalFlagAuthTenant = cli.GlobalFlags.String("T", os.Getenv("AUTH_TENANT"), "|<tenant>| Tenant name for auth system, example: test - Not all auth systems need this. Env: AUTH_TENANT")
	cli.globalFlagAuthUser = cli.GlobalFlags.String("U", os.Getenv("AUTH_USER"), "|<user>| User name for auth system, example: tester - Some auth systems allow tenant:user format here, example: test:tester - Env: AUTH_USER")
	cli.globalFlagAuthKey = cli.GlobalFlags.String("K", os.Getenv("AUTH_KEY"), "|<key>| Key for auth system, example: testing - Some auth systems use passwords instead, see -P - Env: AUTH_KEY")
	cli.globalFlagAuthPassword = cli.GlobalFlags.String("P", os.Getenv("AUTH_PASSWORD"), "|<password>| Password for auth system, example: testing - Some auth system use keys instead, see -K - Env: AUTH_PASSWORD")
	cli.globalFlagOverrideURLs = cli.GlobalFlags.String("O", os.Getenv("OVERRIDE_URLS"), "|<url> [url] ...| Override URLs for service endpoint(s); the service endpoint given by auth will be ignored - Env: OVERRIDE_URLS")
	cli.globalFlagStorageRegion = cli.GlobalFlags.String("R", os.Getenv("STORAGE_REGION"), "|<region>| Storage region to use if set, otherwise uses the default. Env: STORAGE_REGION")
	cli.GlobalFlagVerbose = cli.GlobalFlags.Bool("v", false, "Will activate verbose output.")
	cli.globalFlagContinueOnError = cli.GlobalFlags.Bool("continue-on-error", false, "When possible, continue with additional operations even if one or more fail.")
	i32, _ := strconv.ParseInt(os.Getenv("CONCURRENCY"), 10, 32)
	cli.globalFlagConcurrency = cli.GlobalFlags.Int("C", int(i32), "|<number>| The maximum number of concurrent operations to perform; default is 1. Env: CONCURRENCY")
	b, _ := strconv.ParseBool(os.Getenv("STORAGE_INTERNAL"))
	cli.globalFlagInternalStorage = cli.GlobalFlags.Bool("I", b, "Internal storage URL resolution, such as Rackspace ServiceNet. Env: STORAGE_INTERNAL")
	cli.GlobalFlags.Var(&cli.globalFlagHeaders, "H", "|<name>:[value]| Sets a header to be sent with the request. Useful mostly for PUTs and POSTs, allowing you to set metadata. This option can be specified multiple times for additional headers.")

	cli.BenchDeleteFlags = flag.NewFlagSet("bench-delete", flag.ContinueOnError)
	cli.BenchDeleteFlags.SetOutput(&flagbuf)
	cli.benchDeleteFlagContainers = cli.BenchDeleteFlags.Int("containers", 1, "|<number>| Number of containers in use.")
	cli.benchDeleteFlagCount = cli.BenchDeleteFlags.Int("count", 1000, "|<number>| Number of objects to delete, distributed across containers.")
	cli.benchDeleteFlagCSV = cli.BenchDeleteFlags.String("csv", "", "|<filename>| Store the timing of each delete into a CSV file.")
	cli.benchDeleteFlagCSVOT = cli.BenchDeleteFlags.String("csvot", "", "|<filename>| Store the number of deletes performed over time into a CSV file.")

	cli.BenchGetFlags = flag.NewFlagSet("bench-get", flag.ContinueOnError)
	cli.BenchGetFlags.SetOutput(&flagbuf)
	cli.benchGetFlagContainers = cli.BenchGetFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	cli.benchGetFlagCount = cli.BenchGetFlags.Int("count", 1000, "|<number>| Number of objects to get, distributed across containers.")
	cli.benchGetFlagCSV = cli.BenchGetFlags.String("csv", "", "|<filename>| Store the timing of each get into a CSV file.")
	cli.benchGetFlagCSVOT = cli.BenchGetFlags.String("csvot", "", "|<filename>| Store the number of gets performed over time into a CSV file.")
	cli.benchGetFlagIterations = cli.BenchGetFlags.Int("iterations", 1, "|<number>| Number of iterations to perform.")

	cli.BenchHeadFlags = flag.NewFlagSet("bench-head", flag.ContinueOnError)
	cli.BenchHeadFlags.SetOutput(&flagbuf)
	cli.benchHeadFlagContainers = cli.BenchHeadFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	cli.benchHeadFlagCount = cli.BenchHeadFlags.Int("count", 1000, "|<number>| Number of objects to head, distributed across containers.")
	cli.benchHeadFlagCSV = cli.BenchHeadFlags.String("csv", "", "|<filename>| Store the timing of each head into a CSV file.")
	cli.benchHeadFlagCSVOT = cli.BenchHeadFlags.String("csvot", "", "|<filename>| Store the number of heads performed over time into a CSV file.")
	cli.benchHeadFlagIterations = cli.BenchHeadFlags.Int("iterations", 1, "|<number>| Number of iterations to perform.")

	cli.BenchMixedFlags = flag.NewFlagSet("bench-mixed", flag.ContinueOnError)
	cli.BenchMixedFlags.SetOutput(&flagbuf)
	cli.benchMixedFlagContainers = cli.BenchMixedFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	cli.benchMixedFlagCSV = cli.BenchMixedFlags.String("csv", "", "|<filename>| Store the timing of each request into a CSV file.")
	cli.benchMixedFlagCSVOT = cli.BenchMixedFlags.String("csvot", "", "|<filename>| Store the number of requests performed over time into a CSV file.")
	cli.benchMixedFlagSize = cli.BenchMixedFlags.Int("size", 4096, "|<bytes>| Number of bytes for each object.")
	cli.benchMixedFlagTime = cli.BenchMixedFlags.String("time", "10m", "|<timespan>| Amount of time to run the test, such as 10m or 1h.")

	cli.BenchPostFlags = flag.NewFlagSet("bench-post", flag.ContinueOnError)
	cli.BenchPostFlags.SetOutput(&flagbuf)
	cli.benchPostFlagContainers = cli.BenchPostFlags.Int("containers", 1, "|<number>| Number of containers in use.")
	cli.benchPostFlagCount = cli.BenchPostFlags.Int("count", 1000, "|<number>| Number of objects to post, distributed across containers.")
	cli.benchPostFlagCSV = cli.BenchPostFlags.String("csv", "", "|<filename>| Store the timing of each post into a CSV file.")
	cli.benchPostFlagCSVOT = cli.BenchPostFlags.String("csvot", "", "|<filename>| Store the number of posts performed over time into a CSV file.")

	cli.BenchPutFlags = flag.NewFlagSet("bench-put", flag.ContinueOnError)
	cli.BenchPutFlags.SetOutput(&flagbuf)
	cli.benchPutFlagContainers = cli.BenchPutFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	cli.benchPutFlagCount = cli.BenchPutFlags.Int("count", 1000, "|<number>| Number of objects to PUT, distributed across containers.")
	cli.benchPutFlagCSV = cli.BenchPutFlags.String("csv", "", "|<filename>| Store the timing of each PUT into a CSV file.")
	cli.benchPutFlagCSVOT = cli.BenchPutFlags.String("csvot", "", "|<filename>| Store the number of PUTs performed over time into a CSV file.")
	cli.benchPutFlagSize = cli.BenchPutFlags.Int("size", 4096, "|<bytes>| Number of bytes for each object.")
	cli.benchPutFlagMaxSize = cli.BenchPutFlags.Int("maxsize", 0, "|<bytes>| This option will vary object sizes randomly between -size and -maxsize")

	cli.DownloadFlags = flag.NewFlagSet("download", flag.ContinueOnError)
	cli.DownloadFlags.SetOutput(&flagbuf)
	cli.downloadFlagAccount = cli.DownloadFlags.Bool("a", false, "Indicates you truly wish to download the entire account; this is to prevent accidentally doing so when giving a single parameter to download.")

	cli.GetFlags = flag.NewFlagSet("get", flag.ContinueOnError)
	cli.GetFlags.SetOutput(&flagbuf)
	cli.getFlagRaw = cli.GetFlags.Bool("r", false, "Emit raw results")
	cli.getFlagNameOnly = cli.GetFlags.Bool("n", false, "In listings, emits the names only")
	cli.getFlagMarker = cli.GetFlags.String("marker", "", "|<text>| In listings, sets the start marker")
	cli.getFlagEndMarker = cli.GetFlags.String("endmarker", "", "|<text>| In listings, sets the stop marker")
	cli.getFlagReverse = cli.GetFlags.Bool("reverse", false, "In listings, reverses the order")
	cli.getFlagLimit = cli.GetFlags.Int("limit", 0, "|<number>| In listings, limits the results")
	cli.getFlagPrefix = cli.GetFlags.String("prefix", "", "|<text>| In listings, returns only those matching the prefix")
	cli.getFlagDelimiter = cli.GetFlags.String("delimiter", "", "|<text>| In listings, sets the delimiter and activates delimiter listings")

	if err := cli.GlobalFlags.Parse(args[1:]); err != nil || len(cli.GlobalFlags.Args()) == 0 {
		cli.fatal(cli, err)
	}
	if *cli.globalFlagAuthURL == "" {
		cli.fatalf(cli, "No Auth URL set; use -A\n")
	}
	if *cli.globalFlagAuthUser == "" {
		cli.fatalf(cli, "No Auth User set; use -U\n")
	}
	if *cli.globalFlagAuthKey == "" && *cli.globalFlagAuthPassword == "" {
		cli.fatalf(cli, "No Auth Key or Password set; use -K or -P\n")
	}
	c, resp := NewClient(*cli.globalFlagAuthTenant, *cli.globalFlagAuthUser, *cli.globalFlagAuthPassword, *cli.globalFlagAuthKey, *cli.globalFlagStorageRegion, *cli.globalFlagAuthURL, *cli.globalFlagInternalStorage, strings.Split(*cli.globalFlagOverrideURLs, " "))
	if resp != nil {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		cli.fatalf(cli, "Auth responded with %d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	cmd := ""
	args = append([]string{}, cli.GlobalFlags.Args()...)
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "auth":
		cli.auth(c, args)
	case "bench-delete":
		cli.benchDelete(c, args)
	case "bench-get":
		cli.benchGet(c, args)
	case "bench-head":
		cli.benchHead(c, args)
	case "bench-mixed":
		cli.benchMixed(c, args)
	case "bench-post":
		cli.benchPost(c, args)
	case "bench-put":
		cli.benchPut(c, args)
	case "delete":
		cli.delet(c, args)
	case "download":
		cli.download(c, args)
	case "get":
		cli.get(c, args)
	case "head":
		cli.head(c, args)
	case "post":
		cli.post(c, args)
	case "put":
		cli.put(c, args)
	case "upload":
		cli.upload(c, args)
	default:
		cli.fatalf(cli, "Unknown command: %s\n", cmd)
	}
}

func cliFatal(cli *CLIInstance, err error) {
	if err == flag.ErrHelp || err == nil {
		fmt.Println(cli.Arg0, `[options] <subcommand> ...`)
		fmt.Println(brimtext.Wrap(`
Tool for accessing a Hummingbird/Swift cluster. Some global options can also be set via environment variables. These will be noted at the end of the description with Env: NAME. The following global options are available:
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.GlobalFlags))
		fmt.Println()
		fmt.Println(brimtext.Wrap(`
The following subcommands are available:`, 0, "", ""))
		fmt.Println("\nauth")
		fmt.Println(brimtext.Wrap(`
Displays information retrieved after authentication, such as the Account URL.
        `, 0, "  ", "  "))
		fmt.Println("\nbench-delete [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests DELETEs. By default, 1000 DELETEs are done against the named <container>. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-delete with the same options to test the deletions.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.BenchDeleteFlags))
		fmt.Println("\nbench-get [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests GETs. By default, 1000 GETs are done from the named <container>. If you specify [object] it will be used as the prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-get with the same options with the possible addition of -iterations to lengthen the test time.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.BenchGetFlags))
		fmt.Println("\nbench-head [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests HEADs. By default, 1000 HEADs are done from the named <container>. If you specify [object] it will be used as the prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-head with the same options with the possible addition of -iterations to lengthen the test time.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.BenchHeadFlags))
		fmt.Println("\nbench-mixed [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests mixed request workloads. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used. This test is made to be run for a specific span of time (10 minutes by default). You probably want to run with the -continue-on-error global flag; due to the eventual consistency model of Swift|Hummingbird, a few requests may 404.

Note: The concurrency setting for this test will be used for each request type separately. So, with five request types (PUT, POST, GET, HEAD, DELETE), this means five times the concurrency value specified.
        `, 0, "  ", "  "))
		fmt.Println()
		fmt.Print(cli.HelpFlags(cli.BenchMixedFlags))
		fmt.Println("\nbench-post [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests POSTs. By default, 1000 POSTs are done against the named <container>. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-post with the same options to test POSTing.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.BenchPostFlags))
		fmt.Println("\nbench-put [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests PUTs. By default, 1000 PUTs are done into the named <container>. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.BenchPutFlags))
		fmt.Println("\ndelete [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a DELETE request. A DELETE, as probably expected, is used to remove the target.
        `, 0, "  ", "  "))
		fmt.Println("\ndownload [options] [container] [object] <destpath>")
		fmt.Println(brimtext.Wrap(`
Downloads an object or objects to a local file or files. The <destpath> indicates where you want the file or files to be created. If you don't give [container] [object] the entire account will be downloaded (requires -a for confirmation). If you just give [container] that entire container will be downloaded. Perhaps obviously, if you give [container] [object] just that object will be downloaded.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.DownloadFlags))
		fmt.Println("\nget [options] [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a GET request. A GET on an account or container will output the listing of containers or objects, respectively. A GET on an object will output the content of the object to standard output.
        `, 0, "  ", "  "))
		fmt.Print(cli.HelpFlags(cli.GetFlags))
		fmt.Println("\nhead [options] [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a HEAD request, giving overall information about the account, container, or object.
        `, 0, "  ", "  "))
		fmt.Println("\npost [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a POST request. POSTs allow you to update the metadata for the target.
        `, 0, "  ", "  "))
		fmt.Println("\nput [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a PUT request. A PUT to an account or container will create them. A PUT to an object will create it using the content from standard input.
        `, 0, "  ", "  "))
		fmt.Println("\nupload [options] <sourcepath> [container] [object]")
		fmt.Println(brimtext.Wrap(`
Uploads local files as objects. If you don't specify [container] the name of the current directory will be used. If you don't specify [object] the relative path name from the current directory will be used. If you do specify [object] while uploading a directory, [object] will be used as a prefix to the resulting object names. Note that when uploading a directory, only regular files will be uploaded.
        `, 0, "  ", "  "))
		fmt.Println("\n[container] [object] can also be specified as [container]/[object]")
	} else {
		msg := err.Error()
		if strings.HasPrefix(msg, "flag provided but not defined: ") {
			msg = "No such option: " + msg[len("flag provided but not defined: "):]
		}
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(1)
}

func cliFatalf(cli *CLIInstance, frmt string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, frmt, args...)
	os.Exit(1)
}

func cliVerbosef(cli *CLIInstance, frmt string, args ...interface{}) {
	if *cli.GlobalFlagVerbose {
		fmt.Fprintf(os.Stderr, frmt, args...)
	}
}

// HelpFlags returns the formatted help text for the FlagSet given.
func (cli *CLIInstance) HelpFlags(flags *flag.FlagSet) string {
	var data [][]string
	firstWidth := 0
	flags.VisitAll(func(f *flag.Flag) {
		n := "    -" + f.Name
		u := strings.TrimSpace(f.Usage)
		if u != "" && u[0] == '|' {
			s := strings.SplitN(u, "|", 3)
			if len(s) == 3 {
				n += " " + strings.TrimSpace(s[1])
				u = strings.TrimSpace(s[2])
			}
		}
		if len(n) > firstWidth {
			firstWidth = len(n)
		}
		data = append(data, []string{n, u})
	})
	opts := brimtext.NewDefaultAlignOptions()
	opts.Widths = []int{0, brimtext.GetTTYWidth() - firstWidth - 2}
	return brimtext.Align(data, opts)
}

func (cli *CLIInstance) auth(c Client, args []string) {
	uc, ok := c.(*userClient)
	if ok {
		surls := uc.GetURLs()
		if len(surls) == 0 {
			fmt.Println("Account URL:")
		} else if len(surls) == 1 {
			fmt.Println("Account URL:", surls[0])
		} else {
			fmt.Println("Account URLs:", strings.Join(surls, " "))
		}
	} else {
		fmt.Println("Account URL:", c.GetURL())
	}
	if ct, ok := c.(ClientToken); ok {
		fmt.Println("Token:", ct.GetToken())
	}
}

func (cli *CLIInstance) benchDelete(c Client, args []string) {
	if err := cli.BenchDeleteFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.BenchDeleteFlags.Args())
	if container == "" {
		cli.fatalf(cli, "bench-delete requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *cli.benchDeleteFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *cli.benchDeleteFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *cli.benchDeleteFlagCSV != "" {
		csvf, err := os.Create(*cli.benchDeleteFlagCSV)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
		csvw.Flush()
	}
	var csvotw *csv.Writer
	if *cli.benchDeleteFlagCSVOT != "" {
		csvotf, err := os.Create(*cli.benchDeleteFlagCSVOT)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
		csvotw.Flush()
	}
	concurrency := *cli.globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for x := 0; x < concurrency; x++ {
		go func() {
			var start time.Time
			for {
				i := <-benchChan
				if i == 0 {
					break
				}
				i--
				deleteContainer := container
				if containers > 1 {
					deleteContainer = fmt.Sprintf("%s%d", deleteContainer, i%containers)
				}
				deleteObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "DELETE %s/%s\n", deleteContainer, deleteObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.DeleteObject(deleteContainer, deleteObject, cli.globalFlagHeaders.Headers())
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						deleteContainer + "/" + deleteObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "DELETE %s/%s - %d %s - %s\n", deleteContainer, deleteObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "DELETE %s/%s - %d %s - %s\n", deleteContainer, deleteObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				resp.Body.Close()
			}
			wg.Done()
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-DELETE of %d objects from 1 container, at %d concurrency...", count, concurrency)
	} else {
		fmt.Printf("Bench-DELETE of %d objects, distributed across %d containers, at %d concurrency...", count, containers, concurrency)
	}
	ticker := time.NewTicker(time.Minute)
	start := time.Now()
	lastSoFar := 0
	for i := 1; i <= count; i++ {
		waiting := true
		for waiting {
			select {
			case <-ticker.C:
				soFar := i - concurrency
				now := time.Now()
				elapsed := now.Sub(start)
				fmt.Printf("\n%.05fs for %d DELETEs so far, %.05f DELETEs per second...", float64(elapsed)/float64(time.Second), soFar, float64(soFar)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						fmt.Sprintf("%d", soFar-lastSoFar),
					})
					csvotw.Flush()
					lastSoFar = soFar
				}
			case benchChan <- i:
				waiting = false
			}
		}
	}
	close(benchChan)
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	ticker.Stop()
	fmt.Println()
	if containers == 1 {
		fmt.Printf("Attempting to delete container...")
		cli.verbosef(cli, "DELETE %s\n", container)
		resp := c.DeleteContainer(container, cli.globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Fprintf(os.Stderr, "DELETE %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		resp.Body.Close()
	} else {
		fmt.Printf("Attempting to delete the %d containers...", containers)
		for x := 0; x < containers; x++ {
			deleteContainer := fmt.Sprintf("%s%d", container, x)
			cli.verbosef(cli, "DELETE %s\n", deleteContainer)
			resp := c.DeleteContainer(deleteContainer, cli.globalFlagHeaders.Headers())
			if resp.StatusCode/100 != 2 {
				bodyBytes, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				fmt.Fprintf(os.Stderr, "DELETE %s - %d %s - %s\n", deleteContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			}
			resp.Body.Close()
		}
	}
	fmt.Println()
	fmt.Printf("%.05fs total time, %.05f DELETEs per second.\n", float64(elapsed)/float64(time.Second), float64(count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", count-lastSoFar),
		})
		csvotw.Flush()
	}
}

func (cli *CLIInstance) benchGet(c Client, args []string) {
	if err := cli.BenchGetFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.BenchGetFlags.Args())
	if container == "" {
		cli.fatalf(cli, "bench-get requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *cli.benchGetFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *cli.benchGetFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *cli.benchGetFlagCSV != "" {
		csvf, err := os.Create(*cli.benchGetFlagCSV)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "headers_elapsed_nanoseconds", "elapsed_nanoseconds"})
		csvw.Flush()
	}
	var csvotw *csv.Writer
	if *cli.benchGetFlagCSVOT != "" {
		csvotf, err := os.Create(*cli.benchGetFlagCSVOT)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
		csvotw.Flush()
	}
	iterations := *cli.benchGetFlagIterations
	if iterations < 1 {
		iterations = 1
	}
	concurrency := *cli.globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for x := 0; x < concurrency; x++ {
		go func() {
			var start time.Time
			var headers_elapsed int64
			for {
				i := <-benchChan
				if i == 0 {
					break
				}
				i--
				getContainer := container
				if containers > 1 {
					getContainer = fmt.Sprintf("%s%d", getContainer, i%containers)
				}
				getObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "GET %s/%s\n", getContainer, getObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.GetObject(getContainer, getObject, cli.globalFlagHeaders.Headers())
				if csvw != nil {
					headers_elapsed = time.Now().Sub(start).Nanoseconds()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "GET %s/%s - %d %s - %s\n", getContainer, getObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "GET %s/%s - %d %s - %s\n", getContainer, getObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						getContainer + "/" + getObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", headers_elapsed),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
			}
			wg.Done()
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-GET of %d (%d distinct) objects, from 1 container, at %d concurrency...", iterations*count, count, concurrency)
	} else {
		fmt.Printf("Bench-GET of %d (%d distinct) objects, distributed across %d containers, at %d concurrency...", iterations*count, count, containers, concurrency)
	}
	ticker := time.NewTicker(time.Minute)
	start := time.Now()
	lastSoFar := 0
	for iteration := 0; iteration < iterations; iteration++ {
		for i := 1; i <= count; i++ {
			waiting := true
			for waiting {
				select {
				case <-ticker.C:
					soFar := iteration*count + i - concurrency
					now := time.Now()
					elapsed := now.Sub(start)
					fmt.Printf("\n%.05fs for %d GETs so far, %.05f GETs per second...", float64(elapsed)/float64(time.Second), soFar, float64(soFar)/float64(elapsed/time.Second))
					if csvotw != nil {
						csvotw.Write([]string{
							fmt.Sprintf("%d", now.UnixNano()),
							fmt.Sprintf("%d", soFar-lastSoFar),
						})
						csvotw.Flush()
						lastSoFar = soFar
					}
				case benchChan <- i:
					waiting = false
				}
			}
		}
	}
	close(benchChan)
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	ticker.Stop()
	fmt.Println()
	fmt.Printf("%.05fs total time, %.05f GETs per second.\n", float64(elapsed)/float64(time.Second), float64(iterations*count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", iterations*count-lastSoFar),
		})
		csvotw.Flush()
	}
}

func (cli *CLIInstance) benchHead(c Client, args []string) {
	if err := cli.BenchHeadFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.BenchHeadFlags.Args())
	if container == "" {
		cli.fatalf(cli, "bench-head requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *cli.benchHeadFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *cli.benchHeadFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *cli.benchHeadFlagCSV != "" {
		csvf, err := os.Create(*cli.benchHeadFlagCSV)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "headers_elapsed_nanoseconds", "elapsed_nanoseconds"})
		csvw.Flush()
	}
	var csvotw *csv.Writer
	if *cli.benchHeadFlagCSVOT != "" {
		csvotf, err := os.Create(*cli.benchHeadFlagCSVOT)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
		csvotw.Flush()
	}
	iterations := *cli.benchHeadFlagIterations
	if iterations < 1 {
		iterations = 1
	}
	concurrency := *cli.globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for x := 0; x < concurrency; x++ {
		go func() {
			var start time.Time
			var headers_elapsed int64
			for {
				i := <-benchChan
				if i == 0 {
					break
				}
				i--
				headContainer := container
				if containers > 1 {
					headContainer = fmt.Sprintf("%s%d", headContainer, i%containers)
				}
				headObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "HEAD %s/%s\n", headContainer, headObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.HeadObject(headContainer, headObject, cli.globalFlagHeaders.Headers())
				if csvw != nil {
					headers_elapsed = time.Now().Sub(start).Nanoseconds()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "HEAD %s/%s - %d %s - %s\n", headContainer, headObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "HEAD %s/%s - %d %s - %s\n", headContainer, headObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						headContainer + "/" + headObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", headers_elapsed),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
			}
			wg.Done()
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-HEAD of %d (%d distinct) objects, from 1 container, at %d concurrency...", iterations*count, count, concurrency)
	} else {
		fmt.Printf("Bench-HEAD of %d (%d distinct) objects, distributed across %d containers, at %d concurrency...", iterations*count, count, containers, concurrency)
	}
	ticker := time.NewTicker(time.Minute)
	start := time.Now()
	lastSoFar := 0
	for iteration := 0; iteration < iterations; iteration++ {
		for i := 1; i <= count; i++ {
			waiting := true
			for waiting {
				select {
				case <-ticker.C:
					soFar := iteration*count + i - concurrency
					now := time.Now()
					elapsed := now.Sub(start)
					fmt.Printf("\n%.05fs for %d HEADs so far, %.05f HEADs per second...", float64(elapsed)/float64(time.Second), soFar, float64(soFar)/float64(elapsed/time.Second))
					if csvotw != nil {
						csvotw.Write([]string{
							fmt.Sprintf("%d", now.UnixNano()),
							fmt.Sprintf("%d", soFar-lastSoFar),
						})
						csvotw.Flush()
						lastSoFar = soFar
					}
				case benchChan <- i:
					waiting = false
				}
			}
		}
	}
	close(benchChan)
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	ticker.Stop()
	fmt.Println()
	fmt.Printf("%.05fs total time, %.05f HEADs per second.\n", float64(elapsed)/float64(time.Second), float64(iterations*count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", iterations*count-lastSoFar),
		})
		csvotw.Flush()
	}
}

func (cli *CLIInstance) benchMixed(c Client, args []string) {
	if err := cli.BenchMixedFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.BenchMixedFlags.Args())
	if container == "" {
		cli.fatalf(cli, "bench-mixed requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *cli.benchMixedFlagContainers
	if containers < 1 {
		containers = 1
	}
	size := int64(*cli.benchMixedFlagSize)
	if size < 0 {
		size = 4096
	}
	timespan, err := time.ParseDuration(*cli.benchMixedFlagTime)
	if err != nil {
		cli.fatal(cli, err)
	}
	const (
		delet = iota
		get
		head
		post
		put
	)
	methods := []string{
		"DELETE",
		"GET",
		"HEAD",
		"POST",
		"PUT",
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *cli.benchMixedFlagCSV != "" {
		csvf, err := os.Create(*cli.benchMixedFlagCSV)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "method", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
		csvw.Flush()
	}
	var csvotw *csv.Writer
	if *cli.benchMixedFlagCSVOT != "" {
		csvotf, err := os.Create(*cli.benchMixedFlagCSVOT)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "DELETE", "GET", "HEAD", "POST", "PUT"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0", "0", "0", "0", "0", "0"})
		csvotw.Flush()
	}
	if containers == 1 {
		fmt.Printf("Ensuring container exists...")
		cli.verbosef(cli, "PUT %s\n", container)
		resp := c.PutContainer(container, cli.globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if *cli.globalFlagContinueOnError {
				fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			} else {
				cli.fatalf(cli, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			}
		}
		resp.Body.Close()
	} else {
		fmt.Printf("Ensuring %d containers exist...", containers)
		for x := 0; x < containers; x++ {
			putContainer := fmt.Sprintf("%s%d", container, x)
			cli.verbosef(cli, "PUT %s\n", putContainer)
			resp := c.PutContainer(putContainer, cli.globalFlagHeaders.Headers())
			if resp.StatusCode/100 != 2 {
				bodyBytes, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if *cli.globalFlagContinueOnError {
					fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					continue
				} else {
					cli.fatalf(cli, "PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
				}
			}
			resp.Body.Close()
		}
	}
	fmt.Println()
	concurrency := *cli.globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	timespanTicker := time.NewTicker(timespan)
	doneChan := make(chan bool)
	go func() {
		<-timespanTicker.C
		close(doneChan)
	}()
	deleteChan := make(chan int, concurrency)
	getChan := make(chan int, concurrency)
	headChan := make(chan int, concurrency)
	postChan := make(chan int, concurrency)
	putChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	var deletes int64
	var gets int64
	var heads int64
	var posts int64
	var puts int64
	for x := 0; x < concurrency; x++ {
		wg.Add(1)
		go func() {
			var start time.Time
			op := delet
			var i int
			for {
				select {
				case <-doneChan:
					wg.Done()
					return
				case i = <-deleteChan:
				}
				opContainer := container
				if containers > 1 {
					opContainer = fmt.Sprintf("%s%d", opContainer, i%containers)
				}
				opObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "%s %s/%s\n", methods[op], opContainer, opObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.DeleteObject(opContainer, opObject, cli.globalFlagHeaders.Headers())
				atomic.AddInt64(&deletes, 1)
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						methods[op],
						opContainer + "/" + opObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
			}
		}()
		wg.Add(1)
		go func() {
			var start time.Time
			op := get
			var i int
			for {
				select {
				case <-doneChan:
					wg.Done()
					return
				case i = <-getChan:
				}
				opContainer := container
				if containers > 1 {
					opContainer = fmt.Sprintf("%s%d", opContainer, i%containers)
				}
				opObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "%s %s/%s\n", methods[op], opContainer, opObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.GetObject(opContainer, opObject, cli.globalFlagHeaders.Headers())
				atomic.AddInt64(&gets, 1)
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						methods[op],
						opContainer + "/" + opObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
			}
		}()
		wg.Add(1)
		go func() {
			var start time.Time
			op := head
			var i int
			for {
				select {
				case <-doneChan:
					wg.Done()
					return
				case i = <-headChan:
				}
				opContainer := container
				if containers > 1 {
					opContainer = fmt.Sprintf("%s%d", opContainer, i%containers)
				}
				opObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "%s %s/%s\n", methods[op], opContainer, opObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.HeadObject(opContainer, opObject, cli.globalFlagHeaders.Headers())
				atomic.AddInt64(&heads, 1)
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						methods[op],
						opContainer + "/" + opObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
			}
		}()
		wg.Add(1)
		go func() {
			var start time.Time
			op := post
			var i int
			for {
				select {
				case <-doneChan:
					wg.Done()
					return
				case i = <-postChan:
				}
				opContainer := container
				if containers > 1 {
					opContainer = fmt.Sprintf("%s%d", opContainer, i%containers)
				}
				opObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "%s %s/%s\n", methods[op], opContainer, opObject)
				if csvw != nil {
					start = time.Now()
				}
				headers := cli.globalFlagHeaders.Headers()
				headers["X-Object-Meta-Bench-Mixed"] = strconv.Itoa(i)
				resp := c.PostObject(opContainer, opObject, headers)
				atomic.AddInt64(&posts, 1)
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						methods[op],
						opContainer + "/" + opObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
			}
		}()
		wg.Add(1)
		go func() {
			rnd := NewRand(time.Now().UnixNano())
			var start time.Time
			op := put
			var i int
			for {
				select {
				case <-doneChan:
					wg.Done()
					return
				case i = <-putChan:
				}
				opContainer := container
				if containers > 1 {
					opContainer = fmt.Sprintf("%s%d", opContainer, i%containers)
				}
				opObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "%s %s/%s\n", methods[op], opContainer, opObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.PutObject(opContainer, opObject, cli.globalFlagHeaders.Headers(), &io.LimitedReader{R: rnd, N: size})
				atomic.AddInt64(&puts, 1)
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						methods[op],
						opContainer + "/" + opObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				} else {
					io.Copy(ioutil.Discard, resp.Body)
				}
				resp.Body.Close()
			}
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-Mixed for %s, each object is %d bytes, into 1 container, at %d concurrency...", timespan, size, concurrency)
	} else {
		fmt.Printf("Bench-Mixed for %s, each object is %d bytes, distributed across %d containers, at %d concurrency...", timespan, size, containers, concurrency)
	}
	updateTicker := time.NewTicker(time.Minute)
	start := time.Now()
	var lastDeletes int64
	var lastGets int64
	var lastHeads int64
	var lastPosts int64
	var lastPuts int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-doneChan:
				return
			case <-updateTicker.C:
				now := time.Now()
				elapsed := now.Sub(start)
				snapshotDeletes := atomic.LoadInt64(&deletes)
				snapshotGets := atomic.LoadInt64(&gets)
				snapshotHeads := atomic.LoadInt64(&heads)
				snapshotPosts := atomic.LoadInt64(&posts)
				snapshotPuts := atomic.LoadInt64(&puts)
				total := snapshotDeletes + snapshotGets + snapshotHeads + snapshotPosts + snapshotPuts
				fmt.Printf("\n%.05fs for %d requests so far, %.05f requests per second...", float64(elapsed)/float64(time.Second), total, float64(total)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						fmt.Sprintf("%d", snapshotDeletes-lastDeletes),
						fmt.Sprintf("%d", snapshotGets-lastGets),
						fmt.Sprintf("%d", snapshotHeads-lastHeads),
						fmt.Sprintf("%d", snapshotPosts-lastPosts),
						fmt.Sprintf("%d", snapshotPuts-lastPuts),
					})
					csvotw.Flush()
					lastDeletes = snapshotDeletes
					lastGets = snapshotGets
					lastHeads = snapshotHeads
					lastPosts = snapshotPosts
					lastPuts = snapshotPuts
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			hi := int(atomic.LoadInt64(&puts)) - 10000 // TODO: CLI option
			if i > hi {
				select {
				case <-doneChan:
					return
				default:
				}
				time.Sleep(time.Second)
				continue
			}
			select {
			case <-doneChan:
				return
			case deleteChan <- i:
				i++
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			lo := int(atomic.LoadInt64(&deletes)) + concurrency*2
			if i < lo {
				i = lo
			}
			hi := int(atomic.LoadInt64(&puts)) - concurrency*2
			if i > hi {
				i = lo
			}
			if i > hi {
				select {
				case <-doneChan:
					return
				default:
				}
				time.Sleep(time.Second)
				continue
			}
			select {
			case <-doneChan:
				return
			case getChan <- i:
				i++
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			lo := int(atomic.LoadInt64(&deletes)) + concurrency*2
			if i < lo {
				i = lo
			}
			hi := int(atomic.LoadInt64(&puts)) - concurrency*2
			if i > hi {
				i = lo
			}
			if i > hi {
				select {
				case <-doneChan:
					return
				default:
				}
				time.Sleep(time.Second)
				continue
			}
			select {
			case <-doneChan:
				return
			case headChan <- i:
				i++
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			lo := int(atomic.LoadInt64(&deletes)) + concurrency*2
			if i < lo {
				i = lo
			}
			hi := int(atomic.LoadInt64(&puts)) - concurrency*2
			if i > hi {
				i = lo
			}
			if i > hi {
				select {
				case <-doneChan:
					return
				default:
				}
				time.Sleep(time.Second)
				continue
			}
			select {
			case <-doneChan:
				return
			case postChan <- i:
				i++
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			select {
			case <-doneChan:
				return
			case putChan <- i:
				i++
			}
		}
	}()
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	timespanTicker.Stop()
	updateTicker.Stop()
	fmt.Println()
	total := deletes + gets + heads + posts + puts
	fmt.Printf("%.05fs for %d requests, %.05f requests per second.\n", float64(elapsed)/float64(time.Second), total, float64(total)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", deletes-lastDeletes),
			fmt.Sprintf("%d", gets-lastGets),
			fmt.Sprintf("%d", heads-lastHeads),
			fmt.Sprintf("%d", posts-lastPosts),
			fmt.Sprintf("%d", puts-lastPuts),
		})
		csvotw.Flush()
	}
}

func (cli *CLIInstance) benchPost(c Client, args []string) {
	if err := cli.BenchPostFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.BenchPostFlags.Args())
	if container == "" {
		cli.fatalf(cli, "bench-post requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *cli.benchPostFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *cli.benchPostFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *cli.benchPostFlagCSV != "" {
		csvf, err := os.Create(*cli.benchPostFlagCSV)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
		csvw.Flush()
	}
	var csvotw *csv.Writer
	if *cli.benchPostFlagCSVOT != "" {
		csvotf, err := os.Create(*cli.benchPostFlagCSVOT)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
		csvotw.Flush()
	}
	concurrency := *cli.globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for x := 0; x < concurrency; x++ {
		go func() {
			var start time.Time
			for {
				i := <-benchChan
				if i == 0 {
					break
				}
				i--
				postContainer := container
				if containers > 1 {
					postContainer = fmt.Sprintf("%s%d", postContainer, i%containers)
				}
				postObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "POST %s/%s\n", postContainer, postObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.PostObject(postContainer, postObject, cli.globalFlagHeaders.Headers())
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						postContainer + "/" + postObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "POST %s/%s - %d %s - %s\n", postContainer, postObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "POST %s/%s - %d %s - %s\n", postContainer, postObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				resp.Body.Close()
			}
			wg.Done()
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-POST of %d objects in 1 container, at %d concurrency...", count, concurrency)
	} else {
		fmt.Printf("Bench-POST of %d objects, distributed across %d containers, at %d concurrency...", count, containers, concurrency)
	}
	ticker := time.NewTicker(time.Minute)
	start := time.Now()
	lastSoFar := 0
	for i := 1; i <= count; i++ {
		waiting := true
		for waiting {
			select {
			case <-ticker.C:
				soFar := i - concurrency
				now := time.Now()
				elapsed := now.Sub(start)
				fmt.Printf("\n%.05fs for %d POSTs so far, %.05f POSTs per second...", float64(elapsed)/float64(time.Second), soFar, float64(soFar)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						fmt.Sprintf("%d", soFar-lastSoFar),
					})
					csvotw.Flush()
					lastSoFar = soFar
				}
			case benchChan <- i:
				waiting = false
			}
		}
	}
	close(benchChan)
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	ticker.Stop()
	fmt.Println()
	fmt.Printf("%.05fs total time, %.05f POSTs per second.\n", float64(elapsed)/float64(time.Second), float64(count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", count-lastSoFar),
		})
		csvotw.Flush()
	}
}

func (cli *CLIInstance) benchPut(c Client, args []string) {
	if err := cli.BenchPutFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.BenchPutFlags.Args())
	if container == "" {
		cli.fatalf(cli, "bench-put requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *cli.benchPutFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *cli.benchPutFlagCount
	if count < 1 {
		count = 1000
	}
	size := int64(*cli.benchPutFlagSize)
	if size < 0 {
		size = 4096
	}
	maxsize := int64(*cli.benchPutFlagMaxSize)
	if maxsize < size {
		maxsize = size
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *cli.benchPutFlagCSV != "" {
		csvf, err := os.Create(*cli.benchPutFlagCSV)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
		csvw.Flush()
	}
	var csvotw *csv.Writer
	if *cli.benchPutFlagCSVOT != "" {
		csvotf, err := os.Create(*cli.benchPutFlagCSVOT)
		if err != nil {
			cli.fatal(cli, err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
		csvotw.Flush()
	}
	if containers == 1 {
		fmt.Printf("Ensuring container exists...")
		cli.verbosef(cli, "PUT %s\n", container)
		resp := c.PutContainer(container, cli.globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if *cli.globalFlagContinueOnError {
				fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			} else {
				cli.fatalf(cli, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			}
		}
		resp.Body.Close()
	} else {
		fmt.Printf("Ensuring %d containers exist...", containers)
		for x := 0; x < containers; x++ {
			putContainer := fmt.Sprintf("%s%d", container, x)
			cli.verbosef(cli, "PUT %s\n", putContainer)
			resp := c.PutContainer(putContainer, cli.globalFlagHeaders.Headers())
			if resp.StatusCode/100 != 2 {
				bodyBytes, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if *cli.globalFlagContinueOnError {
					fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					continue
				} else {
					cli.fatalf(cli, "PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
				}
			}
			resp.Body.Close()
		}
	}
	fmt.Println()
	concurrency := *cli.globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for x := 0; x < concurrency; x++ {
		go func() {
			rnd := NewRand(time.Now().UnixNano())
			var start time.Time
			for {
				i := <-benchChan
				if i == 0 {
					break
				}
				i--
				putContainer := container
				if containers > 1 {
					putContainer = fmt.Sprintf("%s%d", putContainer, i%containers)
				}
				putObject := fmt.Sprintf("%s%d", object, i)
				cli.verbosef(cli, "PUT %s/%s\n", putContainer, putObject)
				if csvw != nil {
					start = time.Now()
				}
				sz := size
				if maxsize > size {
					sz += int64(rnd.Intn(int(maxsize - size)))
				}
				resp := c.PutObject(putContainer, putObject, cli.globalFlagHeaders.Headers(), &io.LimitedReader{R: rnd, N: sz})
				if csvw != nil {
					stop := time.Now()
					elapsed := stop.Sub(start).Nanoseconds()
					csvlk.Lock()
					csvw.Write([]string{
						fmt.Sprintf("%d", stop.UnixNano()),
						putContainer + "/" + putObject,
						resp.Header.Get("X-Trans-Id"),
						fmt.Sprintf("%d", resp.StatusCode),
						fmt.Sprintf("%d", elapsed),
					})
					csvw.Flush()
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "PUT %s/%s - %d %s - %s\n", putContainer, putObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "PUT %s/%s - %d %s - %s\n", putContainer, putObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				resp.Body.Close()
			}
			wg.Done()
		}()
	}
	var sz string
	if maxsize > size {
		sz = fmt.Sprintf("%d-%d", size, maxsize)
	} else {
		sz = fmt.Sprintf("%d", size)
	}
	if containers == 1 {
		fmt.Printf("Bench-PUT of %d objects, each %s bytes, into 1 container, at %d concurrency...", count, sz, concurrency)
	} else {
		fmt.Printf("Bench-PUT of %d objects, each %s bytes, distributed across %d containers, at %d concurrency...", count, sz, containers, concurrency)
	}
	ticker := time.NewTicker(time.Minute)
	start := time.Now()
	lastSoFar := 0
	for i := 1; i <= count; i++ {
		waiting := true
		for waiting {
			select {
			case <-ticker.C:
				soFar := i - concurrency
				now := time.Now()
				elapsed := now.Sub(start)
				fmt.Printf("\n%.05fs for %d PUTs so far, %.05f PUTs per second...", float64(elapsed)/float64(time.Second), soFar, float64(soFar)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						fmt.Sprintf("%d", soFar-lastSoFar),
					})
					csvotw.Flush()
					lastSoFar = soFar
				}
			case benchChan <- i:
				waiting = false
			}
		}
	}
	close(benchChan)
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	ticker.Stop()
	fmt.Println()
	fmt.Printf("%.05fs total time, %.05f PUTs per second.\n", float64(elapsed)/float64(time.Second), float64(count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", count-lastSoFar),
		})
		csvotw.Flush()
	}
}

func (cli *CLIInstance) delet(c Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.DeleteObject(container, object, cli.globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.DeleteContainer(container, cli.globalFlagHeaders.Headers())
	} else {
		resp = c.DeleteAccount(cli.globalFlagHeaders.Headers())
	}
	cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func (cli *CLIInstance) get(c Client, args []string) {
	if err := cli.GetFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	container, object := parsePath(cli.GetFlags.Args())
	if *cli.getFlagRaw || object != "" {
		var resp *http.Response
		if object != "" {
			resp = c.GetObject(container, object, cli.globalFlagHeaders.Headers())
		} else if container != "" {
			resp = c.GetContainerRaw(container, *cli.getFlagMarker, *cli.getFlagEndMarker, *cli.getFlagLimit, *cli.getFlagPrefix, *cli.getFlagDelimiter, *cli.getFlagReverse, cli.globalFlagHeaders.Headers())
		} else {
			resp = c.GetAccountRaw(*cli.getFlagMarker, *cli.getFlagEndMarker, *cli.getFlagLimit, *cli.getFlagPrefix, *cli.getFlagDelimiter, *cli.getFlagReverse, cli.globalFlagHeaders.Headers())
		}
		cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		if *cli.getFlagRaw || object == "" {
			data := [][]string{}
			ks := []string{}
			for k := range resp.Header {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			for _, k := range ks {
				for _, v := range resp.Header[k] {
					data = append(data, []string{k + ":", v})
				}
			}
			fmt.Println(resp.StatusCode, http.StatusText(resp.StatusCode))
			opts := brimtext.NewDefaultAlignOptions()
			fmt.Print(brimtext.Align(data, opts))
		}
		if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
			cli.fatal(cli, err)
		}
		return
	}
	if container != "" {
		entries, resp := c.GetContainer(container, *cli.getFlagMarker, *cli.getFlagEndMarker, *cli.getFlagLimit, *cli.getFlagPrefix, *cli.getFlagDelimiter, *cli.getFlagReverse, cli.globalFlagHeaders.Headers())
		cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		if *cli.getFlagNameOnly {
			for _, entry := range entries {
				if entry.Subdir != "" {
					fmt.Println(entry.Subdir)
				} else {
					fmt.Println(entry.Name)
				}
			}
		} else {
			var data [][]string
			data = [][]string{{"Name", "Bytes", "Content Type", "Last Modified", "Hash"}}
			for _, entry := range entries {
				if entry.Subdir != "" {
					data = append(data, []string{entry.Subdir, "", "", "", ""})
				} else {
					data = append(data, []string{entry.Name, fmt.Sprintf("%d", entry.Bytes), entry.ContentType, entry.LastModified, entry.Hash})
				}
			}
			fmt.Print(brimtext.Align(data, nil))
		}
		return
	}
	entries, resp := c.GetAccount(*cli.getFlagMarker, *cli.getFlagEndMarker, *cli.getFlagLimit, *cli.getFlagPrefix, *cli.getFlagDelimiter, *cli.getFlagReverse, cli.globalFlagHeaders.Headers())
	cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	if *cli.getFlagNameOnly {
		for _, entry := range entries {
			fmt.Println(entry.Name)
		}
	} else {
		var data [][]string
		data = [][]string{{"Name", "Count", "Bytes"}}
		for _, entry := range entries {
			data = append(data, []string{entry.Name, fmt.Sprintf("%d", entry.Count), fmt.Sprintf("%d", entry.Bytes)})
		}
		fmt.Print(brimtext.Align(data, nil))
	}
	return
}

func (cli *CLIInstance) head(c Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.HeadObject(container, object, cli.globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.HeadContainer(container, cli.globalFlagHeaders.Headers())
	} else {
		resp = c.HeadAccount(cli.globalFlagHeaders.Headers())
	}
	cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	data := [][]string{}
	ks := []string{}
	kls := map[string]string{}
	for k := range resp.Header {
		ks = append(ks, k)
		kls[k] = k
	}
	sort.Strings(ks)
	for _, k := range ks {
		for _, v := range resp.Header[kls[k]] {
			data = append(data, []string{k + ":", v})
		}
	}
	fmt.Println(resp.StatusCode, http.StatusText(resp.StatusCode))
	fmt.Print(brimtext.Align(data, brimtext.NewDefaultAlignOptions()))
}

func (cli *CLIInstance) put(c Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.PutObject(container, object, cli.globalFlagHeaders.Headers(), os.Stdin)
	} else if container != "" {
		resp = c.PutContainer(container, cli.globalFlagHeaders.Headers())
	} else {
		resp = c.PutAccount(cli.globalFlagHeaders.Headers())
	}
	cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func (cli *CLIInstance) post(c Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.PostObject(container, object, cli.globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.PostContainer(container, cli.globalFlagHeaders.Headers())
	} else {
		resp = c.PostAccount(cli.globalFlagHeaders.Headers())
	}
	cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		cli.fatalf(cli, "%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func (cli *CLIInstance) upload(c Client, args []string) {
	if len(args) == 0 {
		cli.fatalf(cli, "<sourcepath> is required for upload.\n")
	}
	sourcepath := args[0]
	container, object := parsePath(args[1:])
	if container == "" {
		abscwd, err := filepath.Abs(".")
		if err != nil {
			cli.fatalf(cli, "Could not determine current working directory: %s\n", err)
		}
		container = filepath.Base(abscwd)
	}
	cli.verbosef(cli, "Ensuring container %q exists.\n", container)
	resp := c.PutContainer(container, cli.globalFlagHeaders.Headers())
	cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		cli.fatalf(cli, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
	uploadfn := func(path string, appendPath bool) {
		opath := object
		if appendPath {
			opath += path
		}
		cli.verbosef(cli, "Uploading %q to %q %q.\n", path, container, opath)
		f, err := os.Open(path)
		if err != nil {
			if *cli.globalFlagContinueOnError {
				fmt.Fprintf(os.Stderr, "Cannot open %s while attempting to upload to %s/%s: %s\n", path, container, opath, err)
				return
			} else {
				cli.fatalf(cli, "Cannot open %s while attempting to upload to %s/%s: %s\n", path, container, opath, err)
			}
		}
		resp := c.PutObject(container, opath, cli.globalFlagHeaders.Headers(), f)
		cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			f.Close()
			if *cli.globalFlagContinueOnError {
				fmt.Fprintf(os.Stderr, "PUT %s/%s - %d %s - %s\n", container, opath, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
				return
			} else {
				cli.fatalf(cli, "PUT %s/%s - %d %s - %s\n", container, opath, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			}
		}
		resp.Body.Close()
		f.Close()
	}
	fi, err := os.Stat(sourcepath)
	if err != nil {
		cli.fatalf(cli, "Could not stat %s: %s\n", sourcepath, err)
	}
	// This "if" is so a single file upload that happens to be a symlink will work.
	if fi.Mode().IsRegular() {
		uploadfn(sourcepath, false)
	} else {
		concurrency := *cli.globalFlagConcurrency
		if concurrency < 1 {
			concurrency = 1
		}
		uploadChan := make(chan string, concurrency-1)
		wg := sync.WaitGroup{}
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func() {
				for {
					path := <-uploadChan
					if path == "" {
						break
					}
					uploadfn(path, true)
				}
				wg.Done()
			}()
		}
		// This "if" is to handle when the user-given path is a symlink to a directory; we normally want to skip symlinks, but not in this initial case.
		if !strings.HasSuffix(sourcepath, string(os.PathSeparator)) {
			sourcepath += string(os.PathSeparator)
		}
		filepath.Walk(sourcepath, func(path string, info os.FileInfo, err error) error {
			if err != nil || !info.Mode().IsRegular() {
				return nil
			}
			uploadChan <- path
			return nil
		})
		close(uploadChan)
		wg.Wait()
	}
}

func (cli *CLIInstance) download(c Client, args []string) {
	if err := cli.DownloadFlags.Parse(args); err != nil {
		cli.fatal(cli, err)
	}
	args = cli.DownloadFlags.Args()
	if len(args) == 0 {
		cli.fatalf(cli, "<destpath> is required for download.\n")
	}
	destpath := args[len(args)-1]
	container, object := parsePath(args[:len(args)-1])
	concurrency := *cli.globalFlagConcurrency
	// Need at least 2 to queue object downloads while reading a container listing.
	if concurrency < 2 {
		concurrency = 2
	}
	type downloadTask struct {
		container string
		object    string
		destpath  string
	}
	downloadChan := make(chan *downloadTask, concurrency-1)
	var dirExistsLock sync.Mutex
	dirExists := map[string]bool{}
	taskWG := sync.WaitGroup{}
	taskWG.Add(concurrency)
	containerWG := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				task := <-downloadChan
				if task == nil {
					break
				}
				if task.object == "" {
					entries, resp := c.GetContainer(task.container, "", "", 0, "", "", false, cli.globalFlagHeaders.Headers())
					cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
					if resp.StatusCode/100 != 2 {
						bodyBytes, _ := ioutil.ReadAll(resp.Body)
						resp.Body.Close()
						containerWG.Done()
						if *cli.globalFlagContinueOnError {
							fmt.Fprintf(os.Stderr, "GET %s - %d %s - %s\n", task.container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
							continue
						} else {
							cli.fatalf(cli, "GET %s - %d %s - %s\n", task.container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						}
					}
					resp.Body.Close()
					for _, entry := range entries {
						if entry.Name != "" {
							downloadChan <- &downloadTask{container: task.container, object: entry.Name, destpath: filepath.Join(task.destpath, filepath.FromSlash(entry.Name))}
						}
					}
					containerWG.Done()
					continue
				}
				cli.verbosef(cli, "Downloading %s/%s to %s.\n", task.container, task.object, task.destpath)
				if dstdr := filepath.Dir(task.destpath); dstdr != "." {
					dirExistsLock.Lock()
					if !dirExists[dstdr] {
						if err := os.MkdirAll(dstdr, 0755); err != nil {
							if *cli.globalFlagContinueOnError {
								fmt.Fprintf(os.Stderr, "Could not make directory path %s: %s\n", dstdr, err)
							} else {
								cli.fatalf(cli, "Could not make directory path %s: %s\n", dstdr, err)
							}
						}
						dirExists[dstdr] = true
					}
					dirExistsLock.Unlock()
				}
				f, err := os.Create(task.destpath)
				if err != nil {
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Could not create %s: %s\n", task.destpath, err)
						continue
					} else {
						cli.fatalf(cli, "Could not create %s: %s\n", task.destpath, err)
					}
				}
				resp := c.GetObject(task.container, task.object, cli.globalFlagHeaders.Headers())
				cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					f.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "GET %s/%s - %d %s - %s\n", task.container, task.object, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						cli.fatalf(cli, "GET %s/%s - %d %s - %s\n", task.container, task.object, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				if _, err = io.Copy(f, resp.Body); err != nil {
					resp.Body.Close()
					f.Close()
					if *cli.globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Could not complete content transfer from %s/%s to %s: %s\n", task.container, task.object, task.destpath, err)
						continue
					} else {
						cli.fatalf(cli, "Could not complete content transfer from %s/%s to %s: %s\n", task.container, task.object, task.destpath, err)
					}
				}
				resp.Body.Close()
				f.Close()
			}
			taskWG.Done()
		}()
	}
	if object != "" {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				cli.fatalf(cli, "Could not stat %s: %s\n", destpath, err)
			}
		} else if fi.IsDir() {
			destpath = filepath.Join(destpath, object)
		}
		downloadChan <- &downloadTask{container: container, object: object, destpath: destpath}
	} else if container != "" {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				cli.fatalf(cli, "Could not stat %s: %s\n", destpath, err)
			}
		} else if !fi.IsDir() {
			cli.fatalf(cli, "Cannot download a container to a single file: %s\n", destpath)
		}
		containerWG.Add(1)
		downloadChan <- &downloadTask{container: container, object: "", destpath: destpath}
	} else if !*cli.downloadFlagAccount {
		cli.fatalf(cli, "You must specify -a if you wish to download the entire account.\n")
	} else {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				cli.fatalf(cli, "Could not stat %s: %s\n", destpath, err)
			}
		} else if !fi.IsDir() {
			cli.fatalf(cli, "Cannot download an account to a single file: %s\n", destpath)
		}
		entries, resp := c.GetAccount("", "", 0, "", "", false, cli.globalFlagHeaders.Headers())
		cli.verbosef(cli, "X-Trans-Id: %q\n", resp.Header.Get("X-Trans-Id"))
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			cli.fatalf(cli, "GET - %d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		resp.Body.Close()
		for _, entry := range entries {
			if entry.Name != "" {
				containerWG.Add(1)
				downloadChan <- &downloadTask{container: entry.Name, object: "", destpath: filepath.Join(destpath, entry.Name)}
			}
		}
	}
	containerWG.Wait()
	close(downloadChan)
	taskWG.Wait()
}

func parsePath(args []string) (string, string) {
	if len(args) == 0 {
		return "", ""
	}
	path := ""
	for _, arg := range args {
		if path == "" {
			path = arg
			continue
		}
		if strings.HasSuffix(path, "/") {
			path += arg
		} else {
			path += "/" + arg
		}
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

type stringListFlag []string

func (slf *stringListFlag) Set(value string) error {
	*slf = append(*slf, value)
	return nil
}

func (slf *stringListFlag) String() string {
	return strings.Join(*slf, " ")
}

func (slf *stringListFlag) Headers() map[string]string {
	headers := map[string]string{}
	for _, parameter := range *slf {
		splitParameters := strings.SplitN(parameter, ":", 2)
		if len(splitParameters) == 2 {
			headers[strings.TrimSpace(splitParameters[0])] = strings.TrimSpace(splitParameters[1])
		} else {
			headers[strings.TrimSpace(splitParameters[0])] = ""
		}
	}
	return headers
}

// lockedSource allows a random number generator to be used by multiple goroutines concurrently.
// The code is very similar to math/rand.lockedSource.
type lockedSource struct {
	mut sync.Mutex
	src rand.Source64
}

// NewRand returns a rand.Rand that is threadsafe.
func NewRand(seed int64) *rand.Rand {
	return rand.New(&lockedSource{src: rand.NewSource(seed).(rand.Source64)})
}

func (r *lockedSource) Int63() (n int64) {
	r.mut.Lock()
	n = r.src.Int63()
	r.mut.Unlock()
	return
}

func (r *lockedSource) Uint64() (n uint64) {
	r.mut.Lock()
	n = r.src.Uint64()
	r.mut.Unlock()
	return
}

// Seed implements Seed() of Source
func (r *lockedSource) Seed(seed int64) {
	r.mut.Lock()
	r.src.Seed(seed)
	r.mut.Unlock()
}
