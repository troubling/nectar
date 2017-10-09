// Package main defines the nectar command line tool.
package main

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
	"github.com/troubling/nectar"
)

var (
	globalFlags               = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	globalFlagAuthURL         = globalFlags.String("A", os.Getenv("AUTH_URL"), "|<url>| URL to auth system, example: http://127.0.0.1:8080/auth/v1.0 - Env: AUTH_URL")
	globalFlagAuthTenant      = globalFlags.String("T", os.Getenv("AUTH_TENANT"), "|<tenant>| Tenant name for auth system, example: test - Not all auth systems need this. Env: AUTH_TENANT")
	globalFlagAuthUser        = globalFlags.String("U", os.Getenv("AUTH_USER"), "|<user>| User name for auth system, example: tester - Some auth systems allow tenant:user format here, example: test:tester - Env: AUTH_USER")
	globalFlagAuthKey         = globalFlags.String("K", os.Getenv("AUTH_KEY"), "|<key>| Key for auth system, example: testing - Some auth systems use passwords instead, see -P - Env: AUTH_KEY")
	globalFlagAuthPassword    = globalFlags.String("P", os.Getenv("AUTH_PASSWORD"), "|<password>| Password for auth system, example: testing - Some auth system use keys instead, see -K - Env: AUTH_PASSWORD")
	globalFlagStorageRegion   = globalFlags.String("R", os.Getenv("STORAGE_REGION"), "|<region>| Storage region to use if set, otherwise uses the default. Env: STORAGE_REGION")
	globalFlagVerbose         = globalFlags.Bool("v", false, "Will activate verbose output.")
	globalFlagContinueOnError = globalFlags.Bool("continue-on-error", false, "When possible, continue with additional operations even if one or more fail.")
	globalFlagConcurrency     *int               // defined in init()
	globalFlagInternalStorage *bool              // defined in init()
	globalFlagHeaders         = stringListFlag{} // defined in init()
)

var (
	benchGetFlags          = flag.NewFlagSet("bench-get", flag.ContinueOnError)
	benchGetFlagContainers = benchGetFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	benchGetFlagCount      = benchGetFlags.Int("count", 1000, "|<number>| Number of objects to get, distributed across containers.")
	benchGetFlagCSV        = benchGetFlags.String("csv", "", "|<filename>| Store the timing of each get into a CSV file.")
	benchGetFlagCSVOT      = benchGetFlags.String("csvot", "", "|<filename>| Store the number of gets performed over time into a CSV file.")
	benchGetFlagIterations = benchGetFlags.Int("iterations", 1, "|<number>| Number of iterations to perform.")
)

var (
	benchHeadFlags          = flag.NewFlagSet("bench-head", flag.ContinueOnError)
	benchHeadFlagContainers = benchHeadFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	benchHeadFlagCount      = benchHeadFlags.Int("count", 1000, "|<number>| Number of objects to head, distributed across containers.")
	benchHeadFlagCSV        = benchHeadFlags.String("csv", "", "|<filename>| Store the timing of each head into a CSV file.")
	benchHeadFlagCSVOT      = benchHeadFlags.String("csvot", "", "|<filename>| Store the number of heads performed over time into a CSV file.")
	benchHeadFlagIterations = benchHeadFlags.Int("iterations", 1, "|<number>| Number of iterations to perform.")
)

var (
	benchMixedFlags          = flag.NewFlagSet("bench-mixed", flag.ContinueOnError)
	benchMixedFlagContainers = benchMixedFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	benchMixedFlagCSV        = benchMixedFlags.String("csv", "", "|<filename>| Store the timing of each request into a CSV file.")
	benchMixedFlagCSVOT      = benchMixedFlags.String("csvot", "", "|<filename>| Store the number of requests performed over time into a CSV file.")
	benchMixedFlagSize       = benchMixedFlags.Int("size", 4096, "|<bytes>| Number of bytes for each object.")
	benchMixedFlagRatios     = benchMixedFlags.String("ratios", "1,2,2,2,2", "|<deletes>,<gets>,<heads>,<posts>,<puts>| Specifies the number of each type of request in relation to other requests. The default is 1,2,2,2,2 so that two of every other type of request will happen for each DELETE request.")
	benchMixedFlagTime       = benchMixedFlags.String("time", "10m", "|<timespan>| Amount of time to run the test, such as 10m or 1h.")
)

var (
	benchPostFlags          = flag.NewFlagSet("bench-post", flag.ContinueOnError)
	benchPostFlagContainers = benchPostFlags.Int("containers", 1, "|<number>| Number of containers in use.")
	benchPostFlagCount      = benchPostFlags.Int("count", 1000, "|<number>| Number of objects to post, distributed across containers.")
	benchPostFlagCSV        = benchPostFlags.String("csv", "", "|<filename>| Store the timing of each post into a CSV file.")
	benchPostFlagCSVOT      = benchPostFlags.String("csvot", "", "|<filename>| Store the number of posts performed over time into a CSV file.")
)

var (
	benchPutFlags          = flag.NewFlagSet("bench-put", flag.ContinueOnError)
	benchPutFlagContainers = benchPutFlags.Int("containers", 1, "|<number>| Number of containers to use.")
	benchPutFlagCount      = benchPutFlags.Int("count", 1000, "|<number>| Number of objects to PUT, distributed across containers.")
	benchPutFlagCSV        = benchPutFlags.String("csv", "", "|<filename>| Store the timing of each PUT into a CSV file.")
	benchPutFlagCSVOT      = benchPutFlags.String("csvot", "", "|<filename>| Store the number of PUTs performed over time into a CSV file.")
	benchPutFlagSize       = benchPutFlags.Int("size", 4096, "|<bytes>| Number of bytes for each object.")
)

var (
	downloadFlags       = flag.NewFlagSet("download", flag.ContinueOnError)
	downloadFlagAccount = downloadFlags.Bool("a", false, "Indicates you truly wish to download the entire account; this is to prevent accidentally doing so when giving a single parameter to download.")
)

var (
	getFlags         = flag.NewFlagSet("get", flag.ContinueOnError)
	getFlagRaw       = getFlags.Bool("r", false, "Emit raw results")
	getFlagNameOnly  = getFlags.Bool("n", false, "In listings, emits the names only")
	getFlagMarker    = getFlags.String("marker", "", "|<text>| In listings, sets the start marker")
	getFlagEndMarker = getFlags.String("endmarker", "", "|<text>| In listings, sets the stop marker")
	getFlagReverse   = getFlags.Bool("reverse", false, "In listings, reverses the order")
	getFlagLimit     = getFlags.Int("limit", 0, "|<number>| In listings, limits the results")
	getFlagPrefix    = getFlags.String("prefix", "", "|<text>| In listings, returns only those matching the prefix")
	getFlagDelimiter = getFlags.String("delimiter", "", "|<text>| In listings, sets the delimiter and activates delimiter listings")
)

var (
	headFlags = flag.NewFlagSet("head", flag.ContinueOnError)
)

func init() {
	i32, _ := strconv.ParseInt(os.Getenv("CONCURRENCY"), 10, 32)
	globalFlagConcurrency = globalFlags.Int("C", int(i32), "|<number>| The maximum number of concurrent operations to perform; default is 1. Env: CONCURRENCY")
	b, _ := strconv.ParseBool(os.Getenv("STORAGE_INTERNAL"))
	globalFlagInternalStorage = globalFlags.Bool("I", b, "Internal storage URL resolution, such as Rackspace ServiceNet. Env: STORAGE_INTERNAL")
	globalFlags.Var(&globalFlagHeaders, "H", "|<name>:[value]| Sets a header to be sent with the request. Useful mostly for PUTs and POSTs, allowing you to set metadata. This option can be specified multiple times for additional headers.")
	var flagbuf bytes.Buffer
	globalFlags.SetOutput(&flagbuf)
	benchGetFlags.SetOutput(&flagbuf)
	benchHeadFlags.SetOutput(&flagbuf)
	benchMixedFlags.SetOutput(&flagbuf)
	benchPostFlags.SetOutput(&flagbuf)
	benchPutFlags.SetOutput(&flagbuf)
	downloadFlags.SetOutput(&flagbuf)
	getFlags.SetOutput(&flagbuf)
	headFlags.SetOutput(&flagbuf)
}

func fatal(err error) {
	if err == flag.ErrHelp || err == nil {
		fmt.Println(os.Args[0], `[options] <subcommand> ...`)
		fmt.Println(brimtext.Wrap(`
Tool for accessing a Hummingbird/Swift cluster. Some global options can also be set via environment variables. These will be noted at the end of the description with Env: NAME. The following global options are available:
        `, 0, "  ", "  "))
		helpFlags(globalFlags)
		fmt.Println()
		fmt.Println(brimtext.Wrap(`
The following subcommands are available:`, 0, "", ""))
		fmt.Println("\nbench-get [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests GETs. By default, 1000 GETs are done from the named <container>. If you specify [object] it will be used as the prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-get with the same options with the possible addition of -iterations to lengthen the test time.
        `, 0, "  ", "  "))
		helpFlags(benchGetFlags)
		fmt.Println("\nbench-head [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests HEADs. By default, 1000 HEADs are done from the named <container>. If you specify [object] it will be used as the prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-head with the same options with the possible addition of -iterations to lengthen the test time.
        `, 0, "  ", "  "))
		helpFlags(benchHeadFlags)
		fmt.Println("\nbench-mixed [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests mixed request workloads. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used. This test is made to be run for a specific span of time (10 minutes by default). You probably want to run with the -continue-on-error global flag; due to the eventual consistency model of Swift|Hummingbird, a few requests may 404.
        `, 0, "  ", "  "))
		helpFlags(benchMixedFlags)
		fmt.Println("\nbench-post [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests POSTs. By default, 1000 POSTs are done against the named <container>. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used. Generally, you would use bench-put to populate the containers and objects, and then use bench-post with the same options to test POSTing.
        `, 0, "  ", "  "))
		helpFlags(benchPostFlags)
		fmt.Println("\nbench-put [options] <container> [object]")
		fmt.Println(brimtext.Wrap(`
Benchmark tests PUTs. By default, 1000 PUTs are done into the named <container>. If you specify [object] it will be used as a prefix for the object names, otherwise "bench-" will be used.
        `, 0, "  ", "  "))
		helpFlags(benchPutFlags)
		fmt.Println("\ndelete [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a DELETE request. A DELETE, as probably expected, is used to remove the target.
        `, 0, "  ", "  "))
		fmt.Println("\ndownload [options] [container] [object] <destpath>")
		fmt.Println(brimtext.Wrap(`
Downloads an object or objects to a local file or files. The <destpath> indicates where you want the file or files to be created. If you don't give [container] [object] the entire account will be downloaded (requires -a for confirmation). If you just give [container] that entire container will be downloaded. Perhaps obviously, if you give [container] [object] just that object will be downloaded.
        `, 0, "  ", "  "))
		helpFlags(downloadFlags)
		fmt.Println("\nget [options] [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a GET request. A GET on an account or container will output the listing of containers or objects, respectively. A GET on an object will output the content of the object to standard output.
        `, 0, "  ", "  "))
		helpFlags(getFlags)
		fmt.Println("\nhead [options] [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a HEAD request, giving overall information about the account, container, or object.
        `, 0, "  ", "  "))
		helpFlags(headFlags)
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

func fatalf(frmt string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, frmt, args...)
	os.Exit(1)
}

func verbosef(frmt string, args ...interface{}) {
	if *globalFlagVerbose {
		fmt.Fprintf(os.Stderr, frmt, args...)
	}
}

func helpFlags(flags *flag.FlagSet) {
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
	fmt.Print(brimtext.Align(data, opts))
}

func main() {
	if err := globalFlags.Parse(os.Args[1:]); err != nil || len(globalFlags.Args()) == 0 {
		fatal(err)
	}
	if *globalFlagAuthURL == "" {
		fatalf("No Auth URL set; use -A\n")
	}
	if *globalFlagAuthUser == "" {
		fatalf("No Auth User set; use -U\n")
	}
	if *globalFlagAuthKey == "" && *globalFlagAuthPassword == "" {
		fatalf("No Auth Key or Password set; use -K or -P\n")
	}
	c, resp := nectar.NewClient(*globalFlagAuthTenant, *globalFlagAuthUser, *globalFlagAuthPassword, *globalFlagAuthKey, *globalFlagStorageRegion, *globalFlagAuthURL, *globalFlagInternalStorage)
	if resp != nil {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("Auth responded with %d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	cmd := ""
	args := append([]string{}, globalFlags.Args()...)
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "bench-get":
		benchGet(c, args)
	case "bench-head":
		benchHead(c, args)
	case "bench-mixed":
		benchMixed(c, args)
	case "bench-post":
		benchPost(c, args)
	case "bench-put":
		benchPut(c, args)
	case "delete":
		delet(c, args)
	case "download":
		download(c, args)
	case "get":
		get(c, args)
	case "head":
		head(c, args)
	case "post":
		post(c, args)
	case "put":
		put(c, args)
	case "upload":
		upload(c, args)
	default:
		fatalf("Unknown command: %s\n", cmd)
	}
}

func benchGet(c nectar.Client, args []string) {
	if err := benchGetFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(benchGetFlags.Args())
	if container == "" {
		fatalf("bench-get requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *benchGetFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *benchGetFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *benchGetFlagCSV != "" {
		csvf, err := os.Create(*benchGetFlagCSV)
		if err != nil {
			fatal(err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "headers_elapsed_nanoseconds", "elapsed_nanoseconds"})
	}
	var csvotw *csv.Writer
	if *benchGetFlagCSVOT != "" {
		csvotf, err := os.Create(*benchGetFlagCSVOT)
		if err != nil {
			fatal(err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
	}
	iterations := *benchGetFlagIterations
	if iterations < 1 {
		iterations = 1
	}
	concurrency := *globalFlagConcurrency
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
				verbosef("GET %s/%s\n", getContainer, getObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.GetObject(getContainer, getObject, globalFlagHeaders.Headers())
				if csvw != nil {
					headers_elapsed = time.Now().Sub(start).Nanoseconds()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "GET %s/%s - %d %s - %s\n", getContainer, getObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("GET %s/%s - %d %s - %s\n", getContainer, getObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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
	for iteration := 0; iteration <= iterations; iteration++ {
		for i := 1; i <= count; i++ {
			waiting := true
			for waiting {
				select {
				case <-ticker.C:
					soFar := iteration*count + i - concurrency
					now := time.Now()
					elapsed := now.Sub(start)
					fmt.Printf("\n%.05fs for %d GETs so far, %.05fs per GET, or %.05f GETs per second...", float64(elapsed)/float64(time.Second), soFar, float64(elapsed)/float64(time.Second)/float64(soFar), float64(soFar)/float64(elapsed/time.Second))
					if csvotw != nil {
						csvotw.Write([]string{
							fmt.Sprintf("%d", now.UnixNano()),
							fmt.Sprintf("%d", soFar-lastSoFar),
						})
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
	fmt.Printf("%.05fs total time, %.05fs per GET, or %.05f GETs per second.\n", float64(elapsed)/float64(time.Second), float64(elapsed)/float64(time.Second)/float64(iterations*count), float64(iterations*count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", iterations*count-lastSoFar),
		})
	}
}

func benchHead(c nectar.Client, args []string) {
	if err := benchHeadFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(benchHeadFlags.Args())
	if container == "" {
		fatalf("bench-head requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *benchHeadFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *benchHeadFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *benchHeadFlagCSV != "" {
		csvf, err := os.Create(*benchHeadFlagCSV)
		if err != nil {
			fatal(err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "headers_elapsed_nanoseconds", "elapsed_nanoseconds"})
	}
	var csvotw *csv.Writer
	if *benchHeadFlagCSVOT != "" {
		csvotf, err := os.Create(*benchHeadFlagCSVOT)
		if err != nil {
			fatal(err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
	}
	iterations := *benchHeadFlagIterations
	if iterations < 1 {
		iterations = 1
	}
	concurrency := *globalFlagConcurrency
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
				verbosef("HEAD %s/%s\n", headContainer, headObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.HeadObject(headContainer, headObject, globalFlagHeaders.Headers())
				if csvw != nil {
					headers_elapsed = time.Now().Sub(start).Nanoseconds()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "HEAD %s/%s - %d %s - %s\n", headContainer, headObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("HEAD %s/%s - %d %s - %s\n", headContainer, headObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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
					fmt.Printf("\n%.05fs for %d HEADs so far, %.05fs per HEAD, or %.05f HEADs per second...", float64(elapsed)/float64(time.Second), soFar, float64(elapsed)/float64(time.Second)/float64(soFar), float64(soFar)/float64(elapsed/time.Second))
					if csvotw != nil {
						csvotw.Write([]string{
							fmt.Sprintf("%d", now.UnixNano()),
							fmt.Sprintf("%d", soFar-lastSoFar),
						})
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
	fmt.Printf("%.05fs total time, %.05fs per HEAD, or %.05f HEADs per second.\n", float64(elapsed)/float64(time.Second), float64(elapsed)/float64(time.Second)/float64(iterations*count), float64(iterations*count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", iterations*count-lastSoFar),
		})
	}
}

func benchMixed(c nectar.Client, args []string) {
	if err := benchMixedFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(benchMixedFlags.Args())
	if container == "" {
		fatalf("bench-mixed requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *benchMixedFlagContainers
	if containers < 1 {
		containers = 1
	}
	size := int64(*benchMixedFlagSize)
	if size < 0 {
		size = 4096
	}
	timespan, err := time.ParseDuration(*benchMixedFlagTime)
	if err != nil {
		fatal(err)
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
	ratios := strings.Split(*benchMixedFlagRatios, ",")
	if len(ratios) != 5 {
		fatalf("bench-mixed got a bad -ratio value: %v\n", ratios)
	}
	var opOrder []int
	for op, ratio := range ratios {
		n, err := strconv.Atoi(ratio)
		if err != nil {
			fatalf("bench-mixed got a bad -ratio value: %v %s\n", ratios, err)
		}
		for x := 0; x < n; x++ {
			opOrder = append(opOrder, op)
		}
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *benchMixedFlagCSV != "" {
		csvf, err := os.Create(*benchMixedFlagCSV)
		if err != nil {
			fatal(err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "method", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
	}
	var csvotw *csv.Writer
	if *benchMixedFlagCSVOT != "" {
		csvotf, err := os.Create(*benchMixedFlagCSVOT)
		if err != nil {
			fatal(err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "method", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
	}
	if containers == 1 {
		fmt.Printf("Ensuring container exists...")
		verbosef("PUT %s\n", container)
		resp := c.PutContainer(container, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if *globalFlagContinueOnError {
				fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			} else {
				fatalf("PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			}
		}
		resp.Body.Close()
	} else {
		fmt.Printf("Ensuring %d containers exist...", containers)
		for x := 0; x < containers; x++ {
			putContainer := fmt.Sprintf("%s%d", container, x)
			verbosef("PUT %s\n", putContainer)
			resp := c.PutContainer(putContainer, globalFlagHeaders.Headers())
			if resp.StatusCode/100 != 2 {
				bodyBytes, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if *globalFlagContinueOnError {
					fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					continue
				} else {
					fatalf("PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
				}
			}
			resp.Body.Close()
		}
	}
	fmt.Println()
	concurrency := *globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	var deletes int64
	var gets int64
	var heads int64
	var posts int64
	var puts int64
	for x := 0; x < concurrency; x++ {
		go func() {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			var start time.Time
			for {
				i := <-benchChan
				if i == 0 {
					break
				}
				op := i & 0xf
				i >>= 4
				opContainer := container
				if containers > 1 {
					opContainer = fmt.Sprintf("%s%d", opContainer, i%containers)
				}
				opObject := fmt.Sprintf("%s%d", object, i)
				verbosef("%s %s/%s\n", methods[op], opContainer, opObject)
				if csvw != nil {
					start = time.Now()
				}
				var resp *http.Response
				switch op {
				case delet:
					resp = c.DeleteObject(opContainer, opObject, globalFlagHeaders.Headers())
					atomic.AddInt64(&deletes, 1)
				case get:
					resp = c.GetObject(opContainer, opObject, globalFlagHeaders.Headers())
					atomic.AddInt64(&gets, 1)
				case head:
					resp = c.HeadObject(opContainer, opObject, globalFlagHeaders.Headers())
					atomic.AddInt64(&heads, 1)
				case post:
					headers := globalFlagHeaders.Headers()
					headers["X-Object-Meta-Bench-Mixed"] = strconv.Itoa(i)
					resp = c.PostObject(opContainer, opObject, headers)
					atomic.AddInt64(&posts, 1)
				case put:
					resp = c.PutObject(opContainer, opObject, globalFlagHeaders.Headers(), &io.LimitedReader{R: rnd, N: size})
					atomic.AddInt64(&puts, 1)
				default:
					panic(fmt.Errorf("programming error: %d", op))
				}
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
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("%s %s/%s - %d %s - %s\n", methods[op], opContainer, opObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				resp.Body.Close()
			}
			wg.Done()
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-Mixed for %s, each object is %d bytes, into 1 container, at %d concurrency...", timespan, size, concurrency)
	} else {
		fmt.Printf("Bench-Mixed for %s, each object is %d bytes, distributed across %d containers, at %d concurrency...", timespan, size, containers, concurrency)
	}
	timespanTicker := time.NewTicker(timespan)
	updateTicker := time.NewTicker(time.Minute)
	start := time.Now()
	var lastDeletes int64
	var lastGets int64
	var lastHeads int64
	var lastPosts int64
	var lastPuts int64
	var sentDeletes int
	var sentPuts int
requestLoop:
	for x := 0; ; x++ {
		var i int
		op := opOrder[x%len(opOrder)]
		switch op {
		case delet:
			if atomic.LoadInt64(&puts) < 1000 {
				continue
			}
			sentDeletes++
			i = sentDeletes
		case put:
			sentPuts++
			i = sentPuts
		default:
			rang := (int(atomic.LoadInt64(&puts)) - sentDeletes) / 2
			if rang < 1000 {
				continue
			}
			i = sentDeletes + rang/2 + (x % rang)
		}
		waiting := true
		for waiting {
			select {
			case <-timespanTicker.C:
				break requestLoop
			case <-updateTicker.C:
				now := time.Now()
				elapsed := now.Sub(start)
				snapshotDeletes := atomic.LoadInt64(&deletes)
				snapshotGets := atomic.LoadInt64(&gets)
				snapshotHeads := atomic.LoadInt64(&heads)
				snapshotPosts := atomic.LoadInt64(&posts)
				snapshotPuts := atomic.LoadInt64(&puts)
				total := snapshotDeletes + snapshotGets + snapshotHeads + snapshotPosts + snapshotPuts
				fmt.Printf("\n%.05fs for %d requests so far, %.05fs per request, or %.05f requests per second...", float64(elapsed)/float64(time.Second), total, float64(elapsed)/float64(time.Second)/float64(total), float64(total)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						"DELETE",
						fmt.Sprintf("%d", snapshotDeletes-lastDeletes),
					})
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						"GET",
						fmt.Sprintf("%d", snapshotGets-lastGets),
					})
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						"HEAD",
						fmt.Sprintf("%d", snapshotHeads-lastHeads),
					})
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						"POST",
						fmt.Sprintf("%d", snapshotPosts-lastPosts),
					})
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						"PUT",
						fmt.Sprintf("%d", snapshotPuts-lastPuts),
					})
					lastDeletes = snapshotDeletes
					lastGets = snapshotGets
					lastHeads = snapshotHeads
					lastPosts = snapshotPosts
					lastPuts = snapshotPuts
				}
			case benchChan <- i<<4 | op:
				waiting = false
			}
		}
	}
	close(benchChan)
	wg.Wait()
	stop := time.Now()
	elapsed := stop.Sub(start)
	timespanTicker.Stop()
	updateTicker.Stop()
	fmt.Println()
	total := deletes + gets + heads + posts + puts
	fmt.Printf("%.05fs for %d requests, %.05fs per request, or %.05f requests per second.\n", float64(elapsed)/float64(time.Second), total, float64(elapsed)/float64(time.Second)/float64(total), float64(total)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			"DELETE",
			fmt.Sprintf("%d", deletes-lastDeletes),
		})
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			"GET",
			fmt.Sprintf("%d", gets-lastGets),
		})
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			"HEAD",
			fmt.Sprintf("%d", heads-lastHeads),
		})
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			"POST",
			fmt.Sprintf("%d", posts-lastPosts),
		})
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			"PUT",
			fmt.Sprintf("%d", puts-lastPuts),
		})
	}
}

func benchPost(c nectar.Client, args []string) {
	if err := benchPostFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(benchPostFlags.Args())
	if container == "" {
		fatalf("bench-post requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *benchPostFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *benchPostFlagCount
	if count < 1 {
		count = 1000
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *benchPostFlagCSV != "" {
		csvf, err := os.Create(*benchPostFlagCSV)
		if err != nil {
			fatal(err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
	}
	var csvotw *csv.Writer
	if *benchPostFlagCSVOT != "" {
		csvotf, err := os.Create(*benchPostFlagCSVOT)
		if err != nil {
			fatal(err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
	}
	concurrency := *globalFlagConcurrency
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
				verbosef("POST %s/%s\n", postContainer, postObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.PostObject(postContainer, postObject, globalFlagHeaders.Headers())
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
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "POST %s/%s - %d %s - %s\n", postContainer, postObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("POST %s/%s - %d %s - %s\n", postContainer, postObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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
				fmt.Printf("\n%.05fs for %d POSTs so far, %.05fs per POST, or %.05f POSTs per second...", float64(elapsed)/float64(time.Second), soFar, float64(elapsed)/float64(time.Second)/float64(soFar), float64(soFar)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						fmt.Sprintf("%d", soFar-lastSoFar),
					})
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
	fmt.Printf("%.05fs total time, %.05fs per POST, or %.05f POSTs per second.\n", float64(elapsed)/float64(time.Second), float64(elapsed)/float64(time.Second)/float64(count), float64(count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", count-lastSoFar),
		})
	}
}

func benchPut(c nectar.Client, args []string) {
	if err := benchPutFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(benchPutFlags.Args())
	if container == "" {
		fatalf("bench-put requires <container>\n")
	}
	if object == "" {
		object = "bench-"
	}
	containers := *benchPutFlagContainers
	if containers < 1 {
		containers = 1
	}
	count := *benchPutFlagCount
	if count < 1 {
		count = 1000
	}
	size := int64(*benchPutFlagSize)
	if size < 0 {
		size = 4096
	}
	var csvw *csv.Writer
	var csvlk sync.Mutex
	if *benchPutFlagCSV != "" {
		csvf, err := os.Create(*benchPutFlagCSV)
		if err != nil {
			fatal(err)
		}
		csvw = csv.NewWriter(csvf)
		defer func() {
			csvw.Flush()
			csvf.Close()
		}()
		csvw.Write([]string{"completion_time_unix_nano", "object_name", "transaction_id", "status", "elapsed_nanoseconds"})
	}
	var csvotw *csv.Writer
	if *benchPutFlagCSVOT != "" {
		csvotf, err := os.Create(*benchPutFlagCSVOT)
		if err != nil {
			fatal(err)
		}
		csvotw = csv.NewWriter(csvotf)
		defer func() {
			csvotw.Flush()
			csvotf.Close()
		}()
		csvotw.Write([]string{"time_unix_nano", "count_since_last_time"})
		csvotw.Write([]string{fmt.Sprintf("%d", time.Now().UnixNano()), "0"})
	}
	if containers == 1 {
		fmt.Printf("Ensuring container exists...")
		verbosef("PUT %s\n", container)
		resp := c.PutContainer(container, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if *globalFlagContinueOnError {
				fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			} else {
				fatalf("PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
			}
		}
		resp.Body.Close()
	} else {
		fmt.Printf("Ensuring %d containers exist...", containers)
		for x := 0; x < containers; x++ {
			putContainer := fmt.Sprintf("%s%d", container, x)
			verbosef("PUT %s\n", putContainer)
			resp := c.PutContainer(putContainer, globalFlagHeaders.Headers())
			if resp.StatusCode/100 != 2 {
				bodyBytes, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				if *globalFlagContinueOnError {
					fmt.Fprintf(os.Stderr, "PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					continue
				} else {
					fatalf("PUT %s - %d %s - %s\n", putContainer, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
				}
			}
			resp.Body.Close()
		}
	}
	fmt.Println()
	concurrency := *globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	benchChan := make(chan int, concurrency)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for x := 0; x < concurrency; x++ {
		go func() {
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
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
				verbosef("PUT %s/%s\n", putContainer, putObject)
				if csvw != nil {
					start = time.Now()
				}
				resp := c.PutObject(putContainer, putObject, globalFlagHeaders.Headers(), &io.LimitedReader{R: rnd, N: size})
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
					csvlk.Unlock()
				}
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "PUT %s/%s - %d %s - %s\n", putContainer, putObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("PUT %s/%s - %d %s - %s\n", putContainer, putObject, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				resp.Body.Close()
			}
			wg.Done()
		}()
	}
	if containers == 1 {
		fmt.Printf("Bench-PUT of %d objects, each %d bytes, into 1 container, at %d concurrency...", count, size, concurrency)
	} else {
		fmt.Printf("Bench-PUT of %d objects, each %d bytes, distributed across %d containers, at %d concurrency...", count, size, containers, concurrency)
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
				fmt.Printf("\n%.05fs for %d PUTs so far, %.05fs per PUT, or %.05f PUTs per second...", float64(elapsed)/float64(time.Second), soFar, float64(elapsed)/float64(time.Second)/float64(soFar), float64(soFar)/float64(elapsed/time.Second))
				if csvotw != nil {
					csvotw.Write([]string{
						fmt.Sprintf("%d", now.UnixNano()),
						fmt.Sprintf("%d", soFar-lastSoFar),
					})
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
	fmt.Printf("%.05fs total time, %.05fs per PUT, or %.05f PUTs per second.\n", float64(elapsed)/float64(time.Second), float64(elapsed)/float64(time.Second)/float64(count), float64(count)/float64(elapsed/time.Second))
	if csvotw != nil {
		csvotw.Write([]string{
			fmt.Sprintf("%d", stop.UnixNano()),
			fmt.Sprintf("%d", count-lastSoFar),
		})
	}
}

func delet(c nectar.Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.DeleteObject(container, object, globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.DeleteContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.DeleteAccount(globalFlagHeaders.Headers())
	}
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func get(c nectar.Client, args []string) {
	if err := getFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(getFlags.Args())
	if *getFlagRaw || object != "" {
		var resp *http.Response
		if object != "" {
			resp = c.GetObject(container, object, globalFlagHeaders.Headers())
		} else if container != "" {
			resp = c.GetContainerRaw(container, *getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		} else {
			resp = c.GetAccountRaw(*getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		}
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		if *getFlagRaw || object == "" {
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
			fatal(err)
		}
		return
	}
	if container != "" {
		entries, resp := c.GetContainer(container, *getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		if *getFlagNameOnly {
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
	entries, resp := c.GetAccount(*getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	if *getFlagNameOnly {
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

func head(c nectar.Client, args []string) {
	if err := headFlags.Parse(args); err != nil {
		fatal(err)
	}
	container, object := parsePath(headFlags.Args())
	var resp *http.Response
	if object != "" {
		resp = c.HeadObject(container, object, globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.HeadContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.HeadAccount(globalFlagHeaders.Headers())
	}
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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

func put(c nectar.Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.PutObject(container, object, globalFlagHeaders.Headers(), os.Stdin)
	} else if container != "" {
		resp = c.PutContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.PutAccount(globalFlagHeaders.Headers())
	}
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func post(c nectar.Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.PostObject(container, object, globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.PostContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.PostAccount(globalFlagHeaders.Headers())
	}
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("%d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func upload(c nectar.Client, args []string) {
	if len(args) == 0 {
		fatalf("<sourcepath> is required for upload.\n")
	}
	sourcepath := args[0]
	container, object := parsePath(args[1:])
	if container == "" {
		abscwd, err := filepath.Abs(".")
		if err != nil {
			fatalf("Could not determine current working directory: %s\n", err)
		}
		container = filepath.Base(abscwd)
	}
	verbosef("Ensuring container %q exists.\n", container)
	resp := c.PutContainer(container, globalFlagHeaders.Headers())
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("PUT %s - %d %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
	concurrency := *globalFlagConcurrency
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
				verbosef("Uploading %q to %q %q.\n", path, container, object+path)
				f, err := os.Open(path)
				if err != nil {
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Cannot open %s while attempting to upload to %s/%s: %s\n", path, container, object+path, err)
						continue
					} else {
						fatalf("Cannot open %s while attempting to upload to %s/%s: %s\n", path, container, object+path, err)
					}
				}
				resp := c.PutObject(container, object+path, globalFlagHeaders.Headers(), f)
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					f.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "PUT %s/%s - %d %s - %s\n", container, object+path, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("PUT %s/%s - %d %s - %s\n", container, object+path, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				resp.Body.Close()
				f.Close()
			}
			wg.Done()
		}()
	}
	fi, err := os.Stat(sourcepath)
	if err != nil {
		fatalf("Could not stat %s: %s\n", sourcepath, err)
	}
	// This "if" is so a single file upload that happens to be a symlink will work.
	if fi.Mode().IsRegular() {
		uploadChan <- sourcepath
	} else {
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
	}
	close(uploadChan)
	wg.Wait()
}

func download(c nectar.Client, args []string) {
	if err := downloadFlags.Parse(args); err != nil {
		fatal(err)
	}
	args = downloadFlags.Args()
	if len(args) == 0 {
		fatalf("<destpath> is required for download.\n")
	}
	destpath := args[len(args)-1]
	container, object := parsePath(args[:len(args)-1])
	concurrency := *globalFlagConcurrency
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
					entries, resp := c.GetContainer(task.container, "", "", 0, "", "", false, globalFlagHeaders.Headers())
					if resp.StatusCode/100 != 2 {
						bodyBytes, _ := ioutil.ReadAll(resp.Body)
						resp.Body.Close()
						containerWG.Done()
						if *globalFlagContinueOnError {
							fmt.Fprintf(os.Stderr, "GET %s - %d %s - %s\n", task.container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
							continue
						} else {
							fatalf("GET %s - %d %s - %s\n", task.container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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
				verbosef("Downloading %s/%s to %s.\n", task.container, task.object, task.destpath)
				if dstdr := filepath.Dir(task.destpath); dstdr != "." {
					dirExistsLock.Lock()
					if !dirExists[dstdr] {
						if err := os.MkdirAll(dstdr, 0755); err != nil {
							if *globalFlagContinueOnError {
								fmt.Fprintf(os.Stderr, "Could not make directory path %s: %s\n", dstdr, err)
							} else {
								fatalf("Could not make directory path %s: %s\n", dstdr, err)
							}
						}
						dirExists[dstdr] = true
					}
					dirExistsLock.Unlock()
				}
				f, err := os.Create(task.destpath)
				if err != nil {
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Could not create %s: %s\n", task.destpath, err)
						continue
					} else {
						fatalf("Could not create %s: %s\n", task.destpath, err)
					}
				}
				resp := c.GetObject(task.container, task.object, globalFlagHeaders.Headers())
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					f.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "GET %s/%s - %d %s - %s\n", task.container, task.object, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
						continue
					} else {
						fatalf("GET %s/%s - %d %s - %s\n", task.container, task.object, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
					}
				}
				if _, err = io.Copy(f, resp.Body); err != nil {
					resp.Body.Close()
					f.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Could not complete content transfer from %s/%s to %s: %s\n", task.container, task.object, task.destpath, err)
						continue
					} else {
						fatalf("Could not complete content transfer from %s/%s to %s: %s\n", task.container, task.object, task.destpath, err)
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
				fatalf("Could not stat %s: %s\n", destpath, err)
			}
		} else if fi.IsDir() {
			destpath = filepath.Join(destpath, object)
		}
		downloadChan <- &downloadTask{container: container, object: object, destpath: destpath}
	} else if container != "" {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				fatalf("Could not stat %s: %s\n", destpath, err)
			}
		} else if !fi.IsDir() {
			fatalf("Cannot download a container to a single file: %s\n", destpath)
		}
		containerWG.Add(1)
		downloadChan <- &downloadTask{container: container, object: "", destpath: destpath}
	} else if !*downloadFlagAccount {
		fatalf("You must specify -a if you wish to download the entire account.\n")
	} else {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				fatalf("Could not stat %s: %s\n", destpath, err)
			}
		} else if !fi.IsDir() {
			fatalf("Cannot download an account to a single file: %s\n", destpath)
		}
		entries, resp := c.GetAccount("", "", 0, "", "", false, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatalf("GET - %d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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
