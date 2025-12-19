package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NumSectors = 2048
	DiskDelay  = 80 * time.Millisecond
	PrintDelay = 275 * time.Millisecond
)

// where the file lives
type FileInfo struct {
	DiskNumber     int
	StartingSector int
	FileLength     int
}

type dirReq struct {
	operation string // for the enter or lookup operation
	name      string
	info      FileInfo
	reply     chan dirLookupResp // where to send the result for lookup
}

type dirLookupResp struct {
	info        FileInfo
	file_exists bool
}

// DirectoryManager is a channel and goroutine
// passing along requests to enter and lookup files
type DirectoryManager struct {
	reqCh chan dirReq
}

func NewDirectoryManager() *DirectoryManager {
	dm := &DirectoryManager{reqCh: make(chan dirReq)}
	go dm.loop() // goroutine to process operation requests
	return dm
}

// directory manager goroutine
func (dm *DirectoryManager) loop() {
	table := make(map[string]FileInfo) // directory table
	for req := range dm.reqCh {
		switch req.operation {
		case "enter":
			table[req.name] = req.info
		case "lookup":
			info, file_exists := table[req.name]
			req.reply <- dirLookupResp{info: info, file_exists: file_exists}
		}
	}
}

func (dm *DirectoryManager) Enter(name string, info FileInfo) {
	dm.reqCh <- dirReq{operation: "enter", name: name, info: info}
}

func (dm *DirectoryManager) Lookup(name string) (FileInfo, bool) {
	reply := make(chan dirLookupResp, 1)
	dm.reqCh <- dirReq{operation: "lookup", name: name, reply: reply}
	respond := <-reply
	return respond.info, respond.file_exists
}

// disk subsystem

type DiskReadRequest struct {
	Sector int
	Reply  chan string
}

type DiskWriteRequest struct {
	Sector int
	Data   string
	Done   chan struct{}
}

type DiskRequest struct {
	Read  *DiskReadRequest
	Write *DiskWriteRequest
}

type DiskDevice struct {
	reqCh chan DiskRequest // channel of requests to the disk device
}

func NewDiskDevice() *DiskDevice {
	d := &DiskDevice{reqCh: make(chan DiskRequest)}
	go d.loop()
	return d
}

func (d *DiskDevice) loop() {
	sectors := make([]string, NumSectors)
	for req := range d.reqCh {
		time.Sleep(DiskDelay) // disk latency

		// WRITE
		if req.Write != nil {
			if req.Write.Sector >= 0 && req.Write.Sector < len(sectors) {
				sectors[req.Write.Sector] = req.Write.Data
			}
			close(req.Write.Done) // signal completion to the caller
			continue
		}

		// READ
		if req.Read != nil {
			out := ""
			if req.Read.Sector >= 0 && req.Read.Sector < len(sectors) {
				out = sectors[req.Read.Sector]
			}
			req.Read.Reply <- out
			continue
		}
	}
}

// serialize disk reads and writes through a single goroutine
func (d *DiskDevice) Write(sector int, data string) {
	done := make(chan struct{})
	d.reqCh <- DiskRequest{Write: &DiskWriteRequest{Sector: sector, Data: data, Done: done}} // single goroutine, FIFO order
	<-done                                                                                   // wait for disk server to finish
}

func (d *DiskDevice) Read(sector int) string {
	reply := make(chan string, 1)
	d.reqCh <- DiskRequest{Read: &DiskReadRequest{Sector: sector, Reply: reply}} // single goroutine, FIFO order
	return <-reply
}

type DiskPool struct {
	avail chan int // channel of available disk IDs
}

func NewDiskPool(numDisks int) *DiskPool {
	ch := make(chan int, numDisks)
	for i := 0; i < numDisks; i++ {
		ch <- i
	}
	return &DiskPool{avail: ch}
}

func (p *DiskPool) Acquire() int   { return <-p.avail } // blocks if none available
func (p *DiskPool) Release(id int) { p.avail <- id }    // returns disk to pool

type PrintJob struct { // one request to print a file
	FileName string
}

type Printer struct {
	id int // printer number
}

func (p Printer) PrintLine(line string) {
	time.Sleep(PrintDelay)

	fileName := fmt.Sprintf("PRINTER%d", p.id)
	f, input_error := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644) // open printer in append mode
	// os.O_CREATE create file if it doesn’t exist
	// os.O_WRONLY write-only
	// os.O_APPEND always write at the end

	if input_error != nil {
		fmt.Fprintf(os.Stderr, "printer %d : open error: %v\n", p.id, input_error)
		return
	}
	defer f.Close()

	_, _ = fmt.Fprintln(f, line)
}

func printerWorker(
	printer Printer,
	jobs <-chan PrintJob, // the printer spooler job queue
	dir *DirectoryManager,
	disks []*DiskDevice,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for job := range jobs { // take jobs from the queue
		info, found := dir.Lookup(job.FileName) // find where the file is on disk

		if found == false {
			fmt.Printf("file not found : %s\n", job.FileName)
			continue
		}

		start := info.StartingSector
		diskID := info.DiskNumber

		buf := ""

		// disk reads are serialized by the disk device server goroutine
		// each disk has one goroutine processing requests one at a time
		for line_number := 0; line_number < info.FileLength; line_number++ {
			buf = disks[diskID].Read(start + line_number)
			printer.PrintLine(buf)
		}
	}
}

type User struct {
	id       int
	dir      *DirectoryManager
	diskPool *DiskPool
	disks    []*DiskDevice

	nextFreeSec []int

	jobQueue chan<- PrintJob // channel to send print jobs
}

func (u *User) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	userFile := fmt.Sprintf("USER%d", u.id)
	f, input_error := os.Open(userFile)
	if input_error != nil {
		fmt.Printf("file not found : %s\n", userFile)
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		switch {
		case strings.HasPrefix(line, ".save"):
			fileName := parseParam(line)
			u.saveFile(fileName, sc)
		case strings.HasPrefix(line, ".print"):
			fileName := parseParam(line)
			// send print job to print spooler instead of creating threads for each print
			u.jobQueue <- PrintJob{FileName: fileName}
		default:
			fmt.Fprintf(os.Stderr, "unknown%d: %s\n", u.id, line)
		}
	}
}

func parseParam(line string) string {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func (u *User) saveFile(fileName string, sc *bufio.Scanner) {
	diskID := u.diskPool.Acquire()
	defer u.diskPool.Release(diskID)

	offset := u.nextFreeSec[diskID]
	fileLines := 0

	for sc.Scan() {
		line := sc.Text()
		if strings.TrimSpace(line) == ".end" {
			break
		}
		sector := offset + fileLines
		u.disks[diskID].Write(sector, line)
		fileLines++
	}

	info := FileInfo{
		DiskNumber:     diskID,
		StartingSector: offset,
		FileLength:     fileLines,
	}
	u.dir.Enter(fileName, info)

	u.nextFreeSec[diskID] = offset + fileLines
}

func parseArg(s string) int {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "-")
	n, input_error := strconv.Atoi(s)
	if input_error != nil {
		return 0
	}
	return n
}

func main() {

	// run w no args
	var numUsers = 1
	var numDisks = 1
	var numPrinters = 1

	if len(os.Args) >= 2 {
		numUsers = parseArg(os.Args[1])
	}
	if len(os.Args) >= 3 {
		numDisks = parseArg(os.Args[2])
	}
	if len(os.Args) >= 4 {
		numPrinters = parseArg(os.Args[3])
	}

	// initialize directory manager, disks, disk pool, printers, users
	dir := NewDirectoryManager()

	// initialize disk devices
	disks := make([]*DiskDevice, numDisks)

	for i := 0; i < numDisks; i++ {
		disks[i] = NewDiskDevice()
	}

	// initialize disk pool
	diskPool := NewDiskPool(numDisks)

	// initialize printer job queue
	printQueue := make(chan PrintJob, 50)

	// start printer goroutines
	var pWG sync.WaitGroup
	pWG.Add(numPrinters)

	// one goroutine per printer instead of multiple PrintJobThread
	for pid := 0; pid < numPrinters; pid++ {
		go printerWorker(Printer{id: pid}, printQueue, dir, disks, &pWG)
	}

	// disk allocation pointers
	nextFree := make([]int, numDisks)

	// start user goroutines
	var uWG sync.WaitGroup
	uWG.Add(numUsers)

	for uid := 0; uid < numUsers; uid++ {
		u := &User{id: uid, dir: dir, diskPool: diskPool,
			disks: disks, nextFreeSec: nextFree, jobQueue: printQueue,
		}
		go u.Run(&uWG)
	}

	uWG.Wait()

	close(printQueue)

	pWG.Wait()
}
