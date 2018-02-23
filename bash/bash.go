/*******************************************************************************
* Author: Antony Toron
* File name: bash.go
* Date created: 2/16/18
*
* Description: command-line version of server, takes in commands to store files
* and also retreive files.
*******************************************************************************/

package bash

import (
    "fmt"
    "strings"
    "bufio"
    "os"
    "io/ioutil"
    "log"
    "path/filepath"
    "math"
    "sync"
)

const MAX_BUFFER_SIZE int = 1024;

// storageType
const LOCALHOST int = 0;
const EBS int = 1;

func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

func nextToken(line string) (string, int) {
    i := 0
    j := 0
    k := 0
    for i < len(line) && line[i] == ' ' {i++}
    j = i
    for (i < len(line) && line[i] != ' ' && line[i] != '\n') {
        k++
        i++
    }
    for (i < len(line) && line[i] == ' ') {i++}
    token := line[j:j + k]
    return token, i
}

/*
    Arguments:
        path - path to the file to save
        storageType - underlying storage to choose for saving the file
            Possible choices: "localhost" - save to ./storage/drive[1-4]
                              "ebs" - save to ebs drive (local)
                              "dropbox", etc.

    Description (assuming RAID level 4, see Modern OS Page 375 for details; note
    that, currently, the simplification will be made to be working with files, 
    while canonical RAID on the hardware level would be using strips [k sectors
    or bytes each]):
        1) Open the file, check the size of the file
        2) Determine size of each strip (one-third of the file in this case, not
        a fixed size, as strips in hardware would be; used as a general term
        to describe the components that will be distributed here).
        3) Pick (3) locations to save each strip, open connection or file to
        those locations
        4) Start separate goroutines to handle writes to separate locations -
        read in bytes from the original file and write them as you go, to not
        store the entire file in memory
        5) Return when all goroutines complete their tasks (including the parity
        writes, which here sis the exlusive-or of all the strips together)

        Caveats: If this is the first write to this file, then save the storage
        backbone for the file, and optionally what drives the file was
        distributed onto (including the purpose for each one)

    Implementation possibilities:
        a) offload task of writing to files to separate goroutings (parallel)
            - [will be implemented] speeds up the writing, instead of doing it
            sequentially -> the function returns when the writes are completed,
            however, so to run this in the background, a separate goroutine
            should be used

    Implementation TODOs:
        a) Set a constant strip size (k bytes long), and distribute the file
        into > 3 strips, instead of 3 strips of variable length per file
            This will make more sense in the case that the raw drive can be
            used, in which case blocks of bytes can be moved from place to place
            via "dd", etc. (likely that writing to raw HDD is permitted, because
            the /dev/sda files can be used just like any other files -> can
            open, seek, write to file, etc. [this should be done before any
            file system is put onto the disk])**
            See https://goo.gl/PfphAK for how to write to /dev/sda files,
            opening them/using them as block devices might be easier than just
            writing to file, which would act as a character device, etc. ->
            lots of things to take into account
        b) Create a persistent data storage system to save information about
        individual files and the locations of their storage (temporary solution
        is a file)

*/
func saveFile(path string, storageType int) {
    const STRIP_COUNT int = 3;

    filename := filepath.Base(path);
    file, err := os.Open(path); check(err);

    fileStat, err := file.Stat(); check(err);
    size := fileStat.Size(); // in bytes

    stripLength := size / STRIP_COUNT; // may be > MAX_BUFFER_SIZE

    buf_size := math.Min(MAX_BUFFER_SIZE, component_length);

    switch storageType {
        case LOCALHOST:
            saveLocalhost(file, filename, stripLength, size)
        case EBS:
            fmt.Println("Not implemented")
        default:
            fmt.Println("Not implemented")
    }

    // file.Close(); <-- currently saveLocalhost does this for you
}

type readOp struct {
    start int
    end int
    response chan []byte // channel to send back the read bytes
}

type readResponse struct {
    payload []byte
    err error
}

func parityWriter(location string, parityChannel chan []byte, c *Cond, 
                  condition *bool, completionChannel chan int, 
                  writerCount int) {
    parityFile, err := os.Open(location); check(err);
    *condition = false // should initially be false, to pause writers

    // unsigned parity strip
    parityStrip := make([]byte, MAX_BUFFER_SIZE)
    payloadCount := 0
    currentLocation := 0
    for payload := range parityChannel { // also know job is done when buffers < max size
        payloadCount++

        if payloadCount == 3 { // got all necessary parity bits
            payloadCount = 0

            // can let the writers continue, perform this before initiating IO
            // to not block writers longer than necessary
            c.L.Lock()
            *condition = true
            c.Broadcast()
            c.L.Unlock()

            // can write the parity buffer to the parity drive now (at currentLocation)
            _, err := parityFile.WriteAt(parityStrip, currentLocation)
            check(err) // err will be not nil if all bytes written, may need to custom handle

            currentLocation += len(parityStrip)

            c.L.Lock()
            *condition = false // pause writers when they finish next time
            c.L.Unlock()

        } else if payloadCount == 1 {
            // may need to shrink the parity strip
            if  len(payload) < MAX_BUFFER_SIZE {
                parityStrip := make([]byte, len(payload))
            }
        }

        // may need to check length of received payload here
        // XOR with current parityStrip
        for i := 0; i < len(payload); i++ {
            parityStrip ^= payload[i]
        }
    }

    completionChannel <- 0 // success
}

func reader(originalFile *File, readRequests chan<- *readOp) {
    for request := range readRequests { // finish when channel closes
        readBuf := make([]byte, readRequest.end - readRequest.start)
        _, err := originalFile.ReadAt(readBuf, readRequest.start)
        // note: like writing, err will be non-nil here if numRead < len(readBuf)
        check(err)

        readRequest.response <- readBuf
    }
}

func writer(start int, end int, location string, readRequests chan<- *readOp,
            parityChannel chan []byte, c *Cond, condition *bool,
            completionChannel chan int) {
    /*
        Issue read requests from original file until you have written your 
        entire strip to disk
    */
    var currentLocation int = start;
    for currentLocation != end {
        // construct a read request
        endLocation := math.Min(currentLocation + MAX_BUFFER_SIZE, end);
        read := &readOp {
            start: currentLocation,
            end: endLocation,
            response: make(chan []byte)
        }

        readRequests <- read
        readReponse <- read.response // get the response from the reader, blocking
        check(readResponse.err);
        if (len(readResponse.payload) < (endLocation - currentLocation)) {
            fmt.Println("Didn't read as many bytes as wanted");
            currentLocation += len(readResponse.payload);
        } else {
            currentLocation = endLocation;
        }

        // PAUSE HERE - before sending more payloads to the parity channel,
        // ensure that all of the other writers have also finished their tasks
        c.L.Lock()
        for !*condition {
            c.Wait()
        }
        c.L.Unlock()

        // send over the payload to the parity writer before initiating IO
        parityChannel <- readResponse.payload

        // write the payload to the end file
        file, err := os.Open(location); check(err);
        _, err := file.WriteAt(readResponse.payload, start)
        // note: will error if numWritten < length of payload!!, may need to
        // do custom error handle here
        check(err)
    }

    // return and let people know you are done
    completionChannel <- 0 // success
}

func saveLocalhost(originalFile *File, filename string, stripLength int, fileSize int) {
    // make: make allocates memory and initializes an object of type slice, map
    // or chan (only), while new only allocates the memory, but leaves it zeroed

    // chan: connects concurrent goroutines to pass values between them (chan's
    // are therefore typed); sending into channel = [chan name] <- [value];
    // receive the value by doing [value] <- [chan name]
    // send and receives block until both sender/receiver are ready
    // Note: by default, channels are unbuffered, so they only accept sneds if
    // there is a corresponding receive already waiting, buffered channels
    // accept a limited amount of values without a receiver
    // ex: make(chan string, 2) // buffers up to 2 values

    // can use <- chan to synchronize with the goroutines (i.e. pass in a
    // channel used specifically to block on to the goroutine, and wait for the 
    // goroutine to pass a completion value into the channel that you can 
    // consume); see https://gobyexample.com/channel-synchronization, can also
    // close the channel to communicate that (do a 2-value form of receive to
    // check)
    // -> to wait on multiple channels, can use select:
    // https://gobyexample.com/select

    // https://gobyexample.com/worker-pools - worker pools, to send jobs to

    // design: one reader that takes read requests from multiple writers
    // condition variable = for the parity drive to let the other drives know
    // that they can send over their next chunk of data that they have read in
    // -> the parity drive only broadcasts when it has successfully written the
    // parity data and is ready for the next batch to come in

    const STRIP_COUNT int = 3; // analogous to saying "writer count"

    readRequests := make(chan *readOp, STRIP_COUNT)
    parityChannel := make(chan []byte, STRIP_COUNT)
    completionChannel := make(chan int, STRIP_COUNT + 1)

    m := sync.Mutex{};
    m.Lock();
    c := sync.NewCond(&m);
    condition := false

    // initiate a reader
    go reader(originalFile, readRequests)

    // initiate a parity writer (make these strings constants at the top)
    go parityWriter("./storage/drivep/" + filename + "_p", parityChannel, c,
                    &condition, completionChannel, STRIP_COUNT)

    // initiate the writers
    for i := 0; i < STRIP_COUNT; i++ {
        // calculate start and end of this writer
        go writer(i * stripLength, (i + 1) * stripLength,
                  "./storage/drive" + i + "/" + filename + "_" + i, 
                  readRequests, parityChannel, c, &condition, completionChannel)
    }

    // wait for all of the writers (and the parity writer) to be done
    for i := 0; i < STRIP_COUNT + 1; i++ {
        <- completionChannel // may want to get error codes here
    }

    close(completionChannel) // don't need it anymore
    close(parityChannel) // stop the parity writer
    close(readRequests) // stop the reader channel

    file.Close()
}

func Run() {
    //running := true
    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        fmt.Println("Enter prompt: ")
        line := scanner.Text()

        // get command
        command, position := nextToken(line)
        line = line[position:]
        //fmt.Println(command); // note: semicolons are fine in go

        // save a file to storage
        if (strings.Compare(command, "save") == 0) {
            path, position := nextToken(line)
            line = line[position:]
            level, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(path, "") == 0 || strings.Compare(level, "") == 0) {
                fmt.Println("usage: save [path] [raid level]");
                break;
            }

            saveFile(path, level);

            fmt.Println("Saved file.")
        } else if (strings.Compare(command, "download") == 0) {
            filename, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(filename, "") == 0) {
                fmt.Println("usage: download [filename]");
                break;
            }


        } else { // quit
            break;
        }
    }
    
}