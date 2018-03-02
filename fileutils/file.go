/*******************************************************************************
* Author: Antony Toron
* File name: file.go
* Date created: 2/16/18
*
* Description: implements file utilities - saving files, downloading files, etc.
*******************************************************************************/

package fileutils

import (
    "fmt"
    "os"
    "log"
    "path/filepath"
    "math"
    "sync"
)

const MAX_BUFFER_SIZE int = 1024; //1024
const STRIP_COUNT int64 = 3;

// storageType
const LOCALHOST int = 0;
const EBS int = 1;

/*
    Global Variables
    Condition variables, locks, and checks
*/
var payloadCount int
var parityWriterReady bool
var m sync.Mutex
var c *sync.Cond
var allowances []bool
var allowanceLocks []sync.Mutex
var allowanceConditions []*sync.Cond

func init() {
    fmt.Println("initializing fileutils")
    payloadCount = 0
    m = sync.Mutex{};
    c = sync.NewCond(&m);
    parityWriterReady = false
    allowances = make([]bool, STRIP_COUNT)
    allowanceLocks = make([]sync.Mutex, STRIP_COUNT)
    allowanceConditions = make([]*sync.Cond, STRIP_COUNT)
    for i := 0; i < len(allowanceConditions); i++ {
        allowances[i] = true
        allowanceConditions[i] = sync.NewCond(&allowanceLocks[i]);
    }

}


func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

func openFile(path string) (*os.File, error) {
    if _, err := os.Stat(path); os.IsNotExist(err) {
        file, err := os.Create(path)
        return file, err
    } else {
        file, err := os.Create(path)
        return file, err
    }
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
        c) The file may or may not be an exact multiple of the strip count, in
        which case padding should be added to the end of the file (add a 1 and
        0 to 2 bits of 0s - you need at least one bit of padding which will end
        up as at least 1 byte most likely)

*/
func SaveFile(path string, storageType int) {
    //const STRIP_COUNT int64 = 3; // issue: might not be divisible by 3, padding?**

    filename := filepath.Base(path);
    fmt.Printf("Filename: %s, path = %s\n", filename, path)
    //file, err := os.Open(path); check(err);
    file, err := os.Open(path); check(err);

    fileStat, err := file.Stat(); check(err);
    size := fileStat.Size(); // in bytes
    fmt.Printf("Size of file: %d\n", size)
    /*
        Calculate length of the strips the file will be divided into
        Add padding to the last strip of the file to be even multiple of
        STRIP_COUNT
    */
    remainder := size % STRIP_COUNT;
    var stripLength int64 = size / STRIP_COUNT; // may be > MAX_BUFFER_SIZE
    var padding int64 = 0
    if remainder == 0 {
        /*
            Each strip will have one extra byte in it, and the last strip will
            have STRIP_COUNT bytes of padding, since we need at least one
            byte of padding and all strips should be of the same size
        */
        stripLength += 1;
        padding = STRIP_COUNT
    } else {
        /*
            The last strip will have <remainder> bytes of padding
        */
        stripLength = (size + remainder) / STRIP_COUNT;
        padding = remainder
    }

    fmt.Printf("Strip length: %d\n", stripLength)

    switch storageType {
        case LOCALHOST:
            saveLocalhost(file, filename, stripLength, size, padding)
        case EBS:
            fmt.Println("Not implemented")
        default:
            fmt.Println("Not implemented")
    }

    // file.Close(); <-- currently saveLocalhost does this for you
}

type readResponse struct {
    payload []byte
    err error
}

type readOp struct {
    start int64
    end int64
    response chan *readResponse
    //response chan []byte // channel to send back the read bytes
}

// condition based on this payloadCount
//var payloadCount int = 0;

// assumed that lock is acquired to call this
func condition() bool {
    return int64(payloadCount) == STRIP_COUNT;
}

// alternate design: can hold multiple parityStrips in memory and release them
// once done with them (attach a tag to the buffer sent in parityChannel to
// know to which parityStrip this goes to)
func parityWriter(location string, parityChannel chan []byte, completionChannel chan int, 
                  writerCount int64) {
    parityFile, err := openFile(location); check(err)
    //*condition = false // should initially be false, to pause writers

    // unsigned parity strip
    parityStrip := make([]byte, MAX_BUFFER_SIZE)
    //payloadCount := 0
    var currentLocation int64 = 0
    localPayloadCount := 0
    var shrink bool = false;
    for payload := range parityChannel { // also know job is done when buffers < max size
        //(*payloadCount)++
        localPayloadCount++
        shrink = false
        if localPayloadCount == 1 {
            // may need to shrink the parity strip
            if  len(payload) < MAX_BUFFER_SIZE || len(payload) < len(parityStrip) {
                parityStrip = make([]byte, len(payload))
                shrink = true
            }
        }

        // XOR with current parityStrip
        // assert that parity strip length is same as payload length here
        if (len(payload) != len(parityStrip)) {
            fmt.Println("Error: length of payload and parityStrip don't match")
            fmt.Printf("Payload = %d, parityStrip = %d\n", len(payload), len(parityStrip))
            if (!shrink) {
                fmt.Println("Didn't shrink.")
            }
        }
        for i := 0; i < len(payload); i++ {
            parityStrip[i] ^= payload[i]
        }

        if localPayloadCount == 3 { // got all necessary parity bits
            // can let the writers continue, perform this before initiating IO
            // to not block writers longer than necessary
            localPayloadCount = 0
            for i := 0; i < len(allowances); i++ {
                allowanceLock := allowanceConditions[i]
                allowanceLock.L.Lock()

                allowances[i] = true

                allowanceLock.L.Unlock()

                allowanceLock.Signal() // let this writer continue
            }

            // c.L.Lock()
            // payloadCount = 0
            // localPayloadCount = 0
            // //*condition = true
            // c.L.Unlock() // flipping these two lines made a performance difference?
            // c.Broadcast()

            // can write the parity buffer to the parity drive now (at currentLocation)
            _, err := parityFile.WriteAt(parityStrip, currentLocation)
            check(err) // err will be not nil if all bytes written, may need to custom handle

            currentLocation += int64(len(parityStrip))
        }
    }

    completionChannel <- 0 // success
    fmt.Println("Parity writer exiting");
}

func reader(originalFile *os.File, readRequests <-chan *readOp) {
    for request := range readRequests { // finish when channel closes
        //fmt.Printf("Read request from %d to %d\n", request.start, request.end)
        readBuf := make([]byte, request.end - request.start)
        _, err := originalFile.ReadAt(readBuf, request.start)
        // note: like writing, err will be non-nil here if numRead < len(readBuf)
        // check(err) <- might not need to check here, since err is sent in response

        response := &readResponse {
            payload: readBuf,
            err: err}

        request.response <- response
    }
    fmt.Println("Reader exiting");
}

func writer(start int64, end int64, location string, readRequests chan<- *readOp,
            parityChannel chan []byte,
            completionChannel chan int, padding int64, ID int) {
    /*
        Issue read requests from original file until you have written your 
        entire strip to disk
    */
    fmt.Printf("Writer initialized from %d to %d\n", start, end)
    file, err := openFile(location); check(err) // file to write into
    var currentLocation int64 = start;
    var locationInOutputFile int64 = 0;
    for currentLocation != end { // should be <= for debugging?
        // construct a read request
        endLocation := int64(math.Min(float64(currentLocation + int64(MAX_BUFFER_SIZE)), 
                                float64(end)));
        
        read := &readOp {
            start: currentLocation,
            end: endLocation,
            response: make(chan *readResponse)}

        readRequests <- read
        response := <- read.response // get the response from the reader, blocking
        check(response.err);
        var payloadLength int64 = int64(len(response.payload))
        if (payloadLength < (endLocation - currentLocation)) {
            fmt.Println("Didn't read as many bytes as wanted");
            currentLocation += payloadLength;
        } else {
            currentLocation = endLocation;
        }

        // PAUSE HERE - before sending more payloads to the parity channel,
        // ensure that all of the other writers have also finished their tasks
        personalLock := allowanceConditions[ID]
        personalLock.L.Lock()
        for !allowances[ID] { // while you don't have an allowance to send
            personalLock.Wait()
        }

        // compute the true payload: it is possible that padding should be added
        // to this payload, so if padding is non-zero, add that many bytes
        // but only if this is the last iteration (i.e. currentLocation == end)
        if padding != 0 && currentLocation == end {
            paddingSlice := make([]byte, int(padding))
            paddingSlice[0] = 0x80
            response.payload = append(response.payload, paddingSlice...)
        }

        // send once you know you can
        parityChannel <- response.payload

        // reduce your own allowance, you've already sent
        allowances[ID] = false

        personalLock.L.Unlock()

        // c.L.Lock()
        // for !condition() {
        //     c.Wait()
        // }
        // c.L.Unlock()

        // // compute the true payload: it is possible that padding should be added
        // // to this payload, so if padding is non-zero, add that many bytes
        // // but only if this is the last iteration (i.e. currentLocation == end)
        // if padding != 0 && currentLocation == end {
        //     paddingSlice := make([]byte, int(padding))
        //     paddingSlice[0] = 0x80
        //     response.payload = append(response.payload, paddingSlice...)
        // }

        // send over the payload to the parity writer before initiating IO
        // c.L.Lock()
        // payloadCount++
        // c.L.Unlock()

        // parityChannel <- response.payload
        // c.L.Lock() // or && payloadCount == 3 above
        // parityChannel <- response.payload
        // //c.L.Lock()
        // (payloadCount)++
        // c.L.Unlock()

        // write the payload to the end file
        _, err := file.WriteAt(response.payload, locationInOutputFile)
        locationInOutputFile += int64(len(response.payload))
        // note: will error if numWritten < length of payload!!, may need to
        // do custom error handle here
        check(err)


        /*
            Wait for parity writer to be ready for new line of messages
        */
    }

    // add padding (if not 0)
    /*
    if padding != 0 {
        paddingSlice := make([]byte, int(padding))
        paddingSlice[0] = 0x80 // padding byte = 1000 0000
        file.WriteAt(paddingSlice, locationInOutputFile)
    }
    */

    // return and let people know you are done
    completionChannel <- 0 // success
    fmt.Println("Writer exiting");
}

func saveLocalhost(originalFile *os.File, filename string, stripLength int64, fileSize int64,
                   padding int64) {
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

    // const STRIP_COUNT int = 3; // analogous to saying "writer count"

    readRequests := make(chan *readOp, STRIP_COUNT)
    parityChannel := make(chan []byte, STRIP_COUNT)
    completionChannel := make(chan int, STRIP_COUNT + 1)

    // globally defined
    m = sync.Mutex{};
    c = sync.NewCond(&m);

    // "global" condition check, increment when you send, parity writer broadcasts when
    // it is done processing the payloadCount = 3 condition
    payloadCount = 0
    parityWriterReady = false
    allowances = make([]bool, STRIP_COUNT)
    allowanceLocks = make([]sync.Mutex, STRIP_COUNT)
    allowanceConditions = make([]*sync.Cond, STRIP_COUNT)
    for i := 0; i < len(allowanceConditions); i++ {
        allowances[i] = true
        allowanceConditions[i] = sync.NewCond(&allowanceLocks[i]);
    }

    //condition := false

    // initiate a reader
    go reader(originalFile, readRequests)

    // initiate a parity writer (make these strings constants at the top)
    go parityWriter("./storage/drivep/" + filename + "_p", parityChannel,
                     completionChannel, STRIP_COUNT)

    // initiate the writers
    for i := int64(0); i < STRIP_COUNT; i++ {
        // "./storage/drive" + i + "/" + filename + "_" + i,
        storageFile := fmt.Sprintf("./storage/drive%d/%s_%d", i + 1, filename, i + 1)
        // if this is the writer responsible for the last strip of the file,
        // must add padding
        if (i == STRIP_COUNT - 1) {

            go writer(i * stripLength, (i + 1) * stripLength - padding,
                  storageFile, readRequests, parityChannel, 
                  completionChannel, padding, int(i))
        } else {
            // calculate start and end of this writer
            go writer(i * stripLength, (i + 1) * stripLength,
                  storageFile, readRequests, parityChannel, 
                  completionChannel, 0, int(i)) // no padding necessary in earlier strips
        }
    }

    // wait for all of the writers to be done
    for i := int64(0); i < STRIP_COUNT; i++ {
        <- completionChannel // may want to get error codes here
    }

    close(parityChannel) // stop the parity writer

    // wait until the parity writer finishes
    <- completionChannel

    close(completionChannel) // don't need it anymore
    close(readRequests) // stop the reader channel

    originalFile.Close()
}