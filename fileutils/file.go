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
    "errors"
    // "crypto/sha256"
    "crypto/md5"
    "os/exec"
    "bytes"
    "strings"
    // "time"
)

const MAX_BUFFER_SIZE int = 8192*8; //1024, empirically 8192*8 seems pretty good, more testing needed for this
const STRIP_COUNT int64 = 3;
const MD5_SIZE = md5.Size;

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

func initialize() {
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
        file, err := os.Create(path) // maybe os.OpenFile is better here, can specify mode of opening
        return file, err
    } else {
        os.Remove(path)
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
    // may be > MAX_BUFFER_SIZE
    var stripLength int64 = int64(math.Ceil(float64(size) / float64(STRIP_COUNT)));
    var padding int64 = 0
    if remainder == 0 {
        /*
            Each strip will have one extra byte in it, and the last strip will
            have STRIP_COUNT - 1 bytes of padding, since we need at least one
            byte of padding and all strips should be of the same size
        */
        stripLength += 1;
        // took strip_count - 1 of your bytes and gave it to the other drives, 
        // one per each, + 1 for the necessary byte of padding
        padding = STRIP_COUNT;
    } else {
        /*
            Padding in this case is the difference between the file size and the
            calculated size given the strip length
        */
        // stripLength = (size + remainder) / STRIP_COUNT;
        padding = (stripLength * STRIP_COUNT) - size;
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
    numBytes int64
    response chan *readResponse
    //response chan []byte // channel to send back the read bytes
}

// alternate design: can hold multiple parityStrips in memory and release them
// once done with them (attach a tag to the buffer sent in parityChannel to
// know to which parityStrip this goes to)
func parityWriter(location string, parityChannel chan []byte, completionChannel chan int, 
                  writerCount int64) {
    parityFile, err := openFile(location); check(err)

    // unsigned parity strip
    parityStrip := make([]byte, MAX_BUFFER_SIZE)
    var currentLocation int64 = 0
    localPayloadCount := 0

    /*
        Save hash of parity file as well
    */
    currentHash := md5.New()

    for payload := range parityChannel { // also know job is done when buffers < max size
        localPayloadCount++
        if localPayloadCount == 1 {
            // may need to shrink the parity strip
            if  len(payload) < MAX_BUFFER_SIZE || len(payload) < len(parityStrip) {
                parityStrip = make([]byte, len(payload))
            } else {
                // reset parityStrip
                parityStrip = make([]byte, MAX_BUFFER_SIZE)
            }
        }

        // XOR with current parityStrip
        // assert that parity strip length is same as payload length here
        if (len(payload) != len(parityStrip)) {
            err := errors.New("Error: length of payload and partyStrip don't match")
            check(err)
            //fmt.Printf("Payload = %d, parityStrip = %d\n", len(payload), len(parityStrip))
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

            // can write the parity buffer to the parity drive now (at currentLocation)
            _, err := parityFile.WriteAt(parityStrip, currentLocation)
            check(err) // err will be not nil if all bytes written, may need to custom handle

            currentLocation += int64(len(parityStrip))

            // update hash
            currentHash.Write(parityStrip)
        }
    }

    /*
        Append the hash to the end of the parity file, can later put the length
        of the hash if this is necessary (appended to the end at a constant known
        to me)
    */
    finalHash := currentHash.Sum(nil)
    _, err = parityFile.WriteAt(finalHash, currentLocation)
    check(err)

    parityFile.Close()

    completionChannel <- 0 // success
    fmt.Println("Parity writer exiting");
}

func reader(originalFile *os.File, readRequests <-chan *readOp) {
    for request := range readRequests { // finish when channel closes
        //fmt.Printf("Read request from %d to %d\n", request.start, request.start + request.numBytes)
        //readBuf := make([]byte, request.end - request.start)
        readBuf := make([]byte, request.numBytes)
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
    // end should not be included [start, end)
    fmt.Printf("Writer initialized from %d to %d\n", start, end)
    file, err := openFile(location); check(err) // file to write into
    var currentLocation int64 = start;
    var locationInOutputFile int64 = 0;

    /*
        Keep a temporary hash variable, using MD5 for sake of practicality (faster)
        Init = MD5(buffer_1) // where buffer1 is the first buffer in my loop over file

        for all buffer_i, set the temporary variable = MD5(temporary variable | MD5(buffer_i))

        Final value of the temporary variable will be the checksum for the file overall
        Compute the checksum the same way when downloading this component

        Alternate temporary solution = 
        h := md5.New()
        io.WriteString(h, data) // for all buffers, actually h.Write(buffer) for []byte

        h.Sum(nil) // final checksum

    */
    currentHash := md5.New()

    for currentLocation < end { // should be <= for debugging? !=
        // construct a read request
        num := int64(math.Min(float64(MAX_BUFFER_SIZE), float64(end - currentLocation)))
        read := &readOp {
            start: currentLocation,
            numBytes: num,
            response: make(chan *readResponse)}

        readRequests <- read
        response := <- read.response // get the response from the reader, blocking
        check(response.err);
        var payloadLength int64 = int64(len(response.payload))
        if (payloadLength < num) {
            fmt.Println("Didn't read as many bytes as wanted");
            currentLocation += payloadLength;
        } else {
            currentLocation += payloadLength;
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

        // write the payload to the end file
        _, err := file.WriteAt(response.payload, locationInOutputFile)
        locationInOutputFile += int64(len(response.payload))
        // note: will error if numWritten < length of payload!!, may need to
        // do custom error handle here
        check(err)

        // update the hash
        currentHash.Write(response.payload)
    }
    
    /*
        Compute final hash of the file, and append it to the end of the file
    */
    // startTime := time.Now()
    finalHash := currentHash.Sum(nil)
    // elapsed := time.Since(startTime)
    // fmt.Printf("Hash took %s", elapsed)

    fmt.Printf("Final hash: %x, length = %d\n", finalHash, len(finalHash))

    _, err = file.WriteAt(finalHash, locationInOutputFile)
    check(err)

    file.Close()

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

    readRequests := make(chan *readOp, STRIP_COUNT)
    parityChannel := make(chan []byte, STRIP_COUNT)
    completionChannel := make(chan int, STRIP_COUNT + 1)

    // initializes all of the global condition variables and mutexes
    initialize()

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

/*
    It has been determined that the drive with ID driveID is corrupted, when
    reading a portion of the file, located in offendingFile. Fix the drive by
    running fsck on it or some variant of this (to stop using the bad sectors
    that corrupted this file), and then rewrite this portion of the file again,
    by getting the correct version of it by using the other drives to recover.
*/
func recoverFromDriveFailure(driveID int, offendingFile *os.File, 
                            offendingFileLocation string, outputFile *os.File, 
                            storageType int, isParityDisk bool, hadPadding bool) {
    /*
        Fix the drive, if appropriate
    */
    if storageType == EBS {
        driveName := fmt.Sprintf("drive%d", driveID + 1)
        cmd := exec.Command("fsck", driveName)

        var out bytes.Buffer
        cmd.Stdout = &out
        err := cmd.Run()
        check(err)

        fmt.Printf("Fsck stdout: %q\n", out.String())
    }

    /*
        Delete the offending file, and recreate it with the correct data
    */
    oName := offendingFile.Name()
    offendingFile.Close()
    os.Remove(offendingFileLocation)
    fmt.Printf("Offending file location %s\n", offendingFileLocation)
    fixedFile, err := openFile(offendingFileLocation); check(err)

    rawFileName := oName
    lastIndex := strings.LastIndex(rawFileName, "_")
    rawFileName = rawFileName[:lastIndex] // cut off the _driveID part
    x := strings.Split(rawFileName, "/")
    rawFileName = x[len(x) - 1] // get the last part of the path, if name = path
    fmt.Printf("Raw file name: %s\n", rawFileName)  

    if !isParityDisk {
        // read all of the other disks besides this one, and XOR with the parity
        // disk bit by bit and reconstruct the file
        // NOTE: this can be done much more efficiently by issuing more IO requests
        // and using a similar approach as the original saving of the file, but
        // when recovering the file, performance isn't as big of an issue because
        // of the rarity of the occasion (temporary implementation)

        otherDriveFiles := make([]*os.File, STRIP_COUNT - 1)

        parityDriveFileName := fmt.Sprintf("storage/drivep/%s_p", rawFileName)
        parityDriveFile, err := os.Open(parityDriveFileName)
        check(err)
        
        count := 0
        for i := 0; i < int(STRIP_COUNT); i++ {
            if i != driveID {
                tmpName := fmt.Sprintf("storage/drive%d/%s_%d", i + 1, rawFileName, i + 1)
                fmt.Printf("Tmp name: %s\n", tmpName)
                otherDriveFiles[count], err = os.Open(tmpName); check(err)

                count++
            }
        }

        fileStat, err := parityDriveFile.Stat(); check(err);
        rawSize := fileStat.Size(); // in bytes
        rawSize -= MD5_SIZE
        size := rawSize // subtract size for hash at end

        trueParityStrip := make([]byte, MAX_BUFFER_SIZE)
        buf := make([]byte, MAX_BUFFER_SIZE)

        var currentLocation int64 = 0
        lastBuffer := false
        currentHash := md5.New()

        for currentLocation != size {
            // check if need to resize the buffers
            if (size - currentLocation) < int64(MAX_BUFFER_SIZE) {
                lastBuffer = true
                newSize := size - currentLocation

                trueParityStrip = make([]byte, newSize)
                buf = make([]byte, newSize)
            } else {
                trueParityStrip = make([]byte, MAX_BUFFER_SIZE)
            }

            // true parity strip
            _, err = parityDriveFile.ReadAt(trueParityStrip, currentLocation)
            check(err)

            // compute the missing piece by XORing all of the other strips
            for i := 0; i < len(otherDriveFiles); i++ {
                file := otherDriveFiles[i]

                _, err = file.ReadAt(buf, currentLocation)
                check(err)

                for j := 0; j < len(trueParityStrip); j++ {
                    trueParityStrip[j] ^= buf[j]
                }
            }

            // write missing piece into the fixed file
            fixedFile.WriteAt(trueParityStrip, currentLocation)

            // update fixed hash
            currentHash.Write(trueParityStrip)

            // also write into the outputfile we were supposed to return
            // make sure not to write the padding in here, though
            if hadPadding && lastBuffer {
                truePaddingSize := 0
                for i := len(trueParityStrip) - 1; i >= len(trueParityStrip) - int(STRIP_COUNT); i-- {
                    if trueParityStrip[i] == 0x80 {
                        truePaddingSize = len(trueParityStrip) - i
                        break
                    }
                }

                fmt.Printf("True padding size in fix = %d\n", truePaddingSize)
                fmt.Printf("length before trim: %d\n", len(trueParityStrip))

                // resize the size of the true raw data
                trueParityStrip = append([]byte(nil), trueParityStrip[:len(trueParityStrip) - truePaddingSize]...)

                fmt.Printf("length after trim: %d\n", len(trueParityStrip))

                // update "size" of the file
                size -= int64(truePaddingSize)
            }

            outputFile.WriteAt(trueParityStrip, rawSize * int64(driveID) + currentLocation)

            // update location
            currentLocation += int64(len(trueParityStrip))
        }

        fixedHash := currentHash.Sum(nil)
        fmt.Printf("Fixed hash ID %d: %x, length = %d\n", driveID, fixedHash, len(fixedHash))
        _, err = fixedFile.WriteAt(fixedHash, currentLocation)
        check(err)

        /*
            File is fixed! Maybe should be fixing other files at the same time
            as fixing this one, or could just be realizing that later when you try
            to read one of those broken files (not immediately clear which
            one is better because we are running fsck to fix anyway, and will
            realize that the fix caused some of the file data to change)
        */
    } else {
        /*
            Read all of the other drive files, XOR them together, and write them
            to the parity drive
        */
        otherDriveFiles := make([]*os.File, STRIP_COUNT)

        for i := 0; i < int(STRIP_COUNT); i++ {
            tmpName := fmt.Sprintf("storage/drive%d/%s_%d", i + 1, rawFileName, i + 1)
            otherDriveFiles[i], err = os.Open(tmpName); check(err)
        }

        fileStat, err := otherDriveFiles[0].Stat(); check(err);
        size := fileStat.Size(); // in bytes
        size -= MD5_SIZE // chop off the hash

        filesParityStrip := make([]byte, MAX_BUFFER_SIZE)
        buf := make([]byte, MAX_BUFFER_SIZE)

        var currentLocation int64 = 0
        currentHash := md5.New()
        for currentLocation != size {
            // check if need to resize the buffers
            if (size - currentLocation) < int64(MAX_BUFFER_SIZE) {
                newSize := size - currentLocation

                filesParityStrip = make([]byte, newSize)
                buf = make([]byte, newSize)
            } else {
                filesParityStrip = make([]byte, MAX_BUFFER_SIZE)
            }

            // compute the missing piece by XORing all of the other strips
            for i := 0; i < len(otherDriveFiles); i++ {
                file := otherDriveFiles[i]

                _, err = file.ReadAt(buf, currentLocation)
                check(err)

                for j := 0; j < len(filesParityStrip); j++ {
                    filesParityStrip[j] ^= buf[j]
                }
            }

            // write missing piece into the fixed file
            fixedFile.WriteAt(filesParityStrip, currentLocation)

            // update fixed hash
            currentHash.Write(filesParityStrip)

            // update location
            currentLocation += int64(len(filesParityStrip))
        }

        fixedHash := currentHash.Sum(nil)
        fmt.Printf("Fixed hash ID %d: %x, length = %d\n", driveID, fixedHash, len(fixedHash))
        _, err = fixedFile.WriteAt(fixedHash, currentLocation)
        check(err)

        // File is fixed!
    }

    // TODO: print some useful success message here
}

/*
    Simple reader and writer, reads the target file, which is a component (filename) 
    of a larger original file, and writes it to the output file, at the
    given range specified (since the component is from a range of the original
    file - offset = ID * size of component)
    sourcePath = where the file was originally saved - maybe might need this later
*/
func basicReaderWriter(filename string, outputFile *os.File, 
                       ID int, hasPadding bool, completionChannel chan int,
                       canRecoverChannel chan int, storageType int) {
    // read from respective slice, and write it to the output file
    file, err := os.Open(filename); check(err)
    fileStat, err := file.Stat(); check(err);
    rawSize := fileStat.Size(); // in bytes
    size := rawSize

    // subtract the size of the hash at the end to get the true size
    size -= int64(MD5_SIZE)

    offsetInOutput := int64(ID) * size
    var position int64 = 0

    currentHash := md5.New()
    lastBuffer := false
    buf := make([]byte, MAX_BUFFER_SIZE)
    for position != size {

        if (size - position) <= int64(MAX_BUFFER_SIZE) { // will enter conditional at last bit of file
            buf = make([]byte, size - position)

            // Should calculate padding on this buffer
            lastBuffer = true
        }

        // request read at specific location
        // maybe could be creating a specific reader goroutine to perform these
        // reads so that this thread could keep running
        _, err = file.ReadAt(buf, position)
        check(err)

        /*
            Calculate the padding on this, but update hash before fixing buffer,
            since the hash includes the padding in it
        */
        currentHash.Write(buf)
        // calculate padding (if it exists) in this slice
        if hasPadding && lastBuffer {
            truePaddingSize := 0
            for i := len(buf) - 1; i >= len(buf) - int(STRIP_COUNT); i-- {
                if buf[i] == 0x80 {
                    truePaddingSize = len(buf) - i
                    break
                }
            }

            fmt.Printf("True padding size = %d\n", truePaddingSize)

            // resize the size of the true raw data
            buf = append([]byte(nil), buf[:len(buf) - truePaddingSize]...)

            // update "size" of the file
            size -= int64(truePaddingSize)
        }


        // write into the outputfile at the specific offset that this writer
        // is responsible for
        _, err = outputFile.WriteAt(buf, offsetInOutput); check(err)

        // update positions
        length := int64(len(buf))
        position += length
        offsetInOutput += length

        // update hash
        // currentHash.Write(buf)
    }

    /*
        Check if the computed hash on this component is correct, otherwise need
        to handle appropriately (mark this disk as a bad disk and/or just rewrite
        this component again, and then calculate the true value of this component
        by using the parity drive to recover)
    */

    finalHash := currentHash.Sum(nil)
    fmt.Printf("Final hash ID %d: %x, length = %d\n", ID, finalHash, len(finalHash))
    originalHash := make([]byte, MD5_SIZE)
    _, err = file.ReadAt(originalHash, rawSize - MD5_SIZE)
    check(err)
    fmt.Printf("Original hash ID %d: %x, length = %d\n", ID, originalHash, len(originalHash))

    var hashesMatch bool = true
    if len(originalHash) != MD5_SIZE {
        fmt.Printf("Original hash not correct length\n")
        hashesMatch = false
    }
    for i := 0; i < MD5_SIZE; i++ {
        if (finalHash[i] != originalHash[i]) {
            hashesMatch = false
        }
    }

    if !hashesMatch {
        completionChannel <- ID + 1
        <- canRecoverChannel // wait until master says that this drive can recover
        fmt.Printf("This drive is messed up, ID = %d\n", ID)
        recoverFromDriveFailure(ID, file, filename, outputFile, storageType, 
                                false, hasPadding)

        fmt.Printf("Successfully fixed drive ID = %d\n", ID)
    }

    file.Close()

    completionChannel <- 0 // success
    fmt.Println("RW exiting")
}

func basicParityChecker(filename string, parityCompletionChannel chan int, 
                        canRecoverChannel chan int, storageType int) {
    parityFile, err := os.Open(filename); check(err)

    fileStat, err := parityFile.Stat()
    rawSize := fileStat.Size()
    size := rawSize
    size -= MD5_SIZE

    buf := make([]byte, MAX_BUFFER_SIZE)
    currentHash := md5.New()

    var currentLocation int64 = 0
    for currentLocation != size {
        if (size - currentLocation) < int64(MAX_BUFFER_SIZE) {
            fmt.Printf("Current location: %d, size: %d\n", currentLocation, size)
            buf = make([]byte, size - currentLocation)
        }

        _, err = parityFile.ReadAt(buf, currentLocation)
        check(err)

        currentHash.Write(buf)

        currentLocation += int64(len(buf))
    }

    finalHash := currentHash.Sum(nil)
    fmt.Printf("Final hash parity: %x, length = %d\n", finalHash, len(finalHash))
    originalHash := make([]byte, MD5_SIZE)
    _, err = parityFile.ReadAt(originalHash, rawSize - MD5_SIZE)
    check(err)
    fmt.Printf("Original hash parity: %x, length = %d\n", originalHash, len(originalHash))

    var hashesMatch bool = true
    if len(originalHash) != MD5_SIZE {
        fmt.Printf("Original hash not correct length\n")
        hashesMatch = false
    }
    for i := 0; i < MD5_SIZE; i++ {
        if (finalHash[i] != originalHash[i]) {
            hashesMatch = false
        }
    }

    if !hashesMatch {
        parityCompletionChannel <- 1
        <- canRecoverChannel // wait until master says that this drive can recover
        fmt.Printf("This drive is messed up, ID = parity\n")
        recoverFromDriveFailure(0, parityFile, filename, nil, storageType, 
                                true, false)

        fmt.Printf("Successfully fixed drive ID = parity\n")
    }

    parityFile.Close()

    parityCompletionChannel <- 0 // success
    fmt.Println("Parity checker exiting.")
}

/*
    Retrieve a file that was saved to the system

    pathToFile = where the file is located in the file system of the person who
    saved the file (just name of file for now)

        Note: can save a SHA-256 hash of the path that a user gave, and that
        can be the name of the file, because the path should be unique to each
        file (there shouldn't be files that are named exactly the same way
        in the same folder)

        Can possibly do hash(user | pathToFile)

    location = where to put the file after retreiving it

    TODO:
        - make sure to do checks to perform if a certain drive is not correct
        or the file does not exist (can simulate this in a test by deleting
        the file) - a writer can report that it doesn't have its drive back
        to this function and we can spawn a new goroutine to use the parity
        drive information (also might need to recover parity drive info so
        may need to write that in this function as we start reading)
*/
func getFileLocalhost(pathToFile string) { // location string
    // can delete this after sent in real model
    // buf := make([]byte, len(pathToFile))
    // copy(buf[:], pathToFile)
    // hash := sha256.Sum256(buf)
    // s := string(hash[:len(hash)])
    filename := fmt.Sprintf("downloaded-%s", pathToFile)
    s := filename
    // filename := fmt.Sprintf("tmp-%s", s)

    fmt.Printf("Creating file: %s\n", s)
    outputFile, err := os.Create(filename); check(err)

    /*
        Check if any of the drives is down - recover appropriately if this is
        the case.

        For now, can just append the 256 byte hash of the file to the end of the
        file and compare it each time I read the component (instead of having
        to compute the XOR every single time, which might not be necessary
        if not reading the whole file -> this might never happen for this
        application though) -> XOR would just tell me something is wrong, still
        don't know where something went wrong, so need to do the hash approach

        the file will be corrupted if the sector went bad, so the hash will solve
        this, just need to make sure to run fsck or something on the offending
        drive

        hash might be really slow though, maybe should hash in parts

        since go hashing is not optimized, can just hash my buffer, keep a temporary
        hashing variable = to that, then move onto the next buffer, hash it, and then
        save the hash of those two together as the new value of the temporary variable,
        and continuously do this until you finish the whole file (do this same
        calculation whenever reading a segment of a file, error out if there is
        some issue in the final comparison)

    */

    completionChannel := make(chan int, STRIP_COUNT)
    canRecoverChannel := make(chan int, STRIP_COUNT)
    parityCompletionChannel := make(chan int, 1)

    for i := 0; i < int(STRIP_COUNT); i++ {
        sliceFilename := fmt.Sprintf("storage/drive%d/%s_%d", i + 1, pathToFile, i + 1)
        hasPadding := (i == int(STRIP_COUNT) - 1)
        go basicReaderWriter(sliceFilename, outputFile, i, hasPadding, 
                            completionChannel, canRecoverChannel, LOCALHOST)
    }

    // also create a basic reader to check the correctness of the redundant
    // bits stored on the parity disk
    parityFilename := fmt.Sprintf("storage/drivep/%s_p", pathToFile)
    go basicParityChecker(parityFilename, parityCompletionChannel, 
                        canRecoverChannel, LOCALHOST)

    // wait for all of the writers to be done
    numberOfErrors := 0
    // brokenDrive := -1
    for i := int64(0); i < STRIP_COUNT; i++ {
        errorCode := <- completionChannel // may want to get error codes here
        if errorCode != 0 { // some drive had an issue
            numberOfErrors++
            // brokenDrive = errorCode
        }

        // got all of the other drives
        if i == STRIP_COUNT - int64(numberOfErrors) {
            // can let the drive(s) with errors know that they can use the
            // other drives' files
            // Note: for now, assuming only one drive will go down at a time

            canRecoverChannel <- 0
            // should wait for recovery to finish, will return a 0 after it
            // returns
            errorCode = <- completionChannel
        }
    }

    fmt.Printf("All of the writers finished\n")

    // ensure parity checker finishes
    errorCode := <- parityCompletionChannel
    if errorCode != 0 {
        canRecoverChannel <- 0
        errorCode = <- parityCompletionChannel
    }

    fmt.Printf("Parity writer finished\n")

    // remove after sent
    // os.Remove(filename)
    outputFile.Close()

    close(completionChannel)
    close(parityCompletionChannel)
    close(canRecoverChannel)

    fmt.Printf("Closed channels\n")
}

/*
    Call to get file that was saved to the system
*/
func GetFile(pathToFile string, storageType int) {
    switch storageType {
        case LOCALHOST:
            getFileLocalhost(pathToFile)
        case EBS:
            fmt.Println("Not implemented")
        default:
            fmt.Println("Not implemented")
    }
}