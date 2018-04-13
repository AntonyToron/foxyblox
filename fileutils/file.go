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
    // "os/exec"
    // "bytes"
    "strings"
    "foxyblox/types"
    // "time"
)

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

func initialize(configs *types.Config, diskLocations []string) {
    dataDiskCount := len(diskLocations) - configs.ParityDiskCount

    payloadCount = 0
    m = sync.Mutex{};
    c = sync.NewCond(&m);
    parityWriterReady = false
    allowances = make([]bool, dataDiskCount)
    allowanceLocks = make([]sync.Mutex, dataDiskCount)
    allowanceConditions = make([]*sync.Cond, dataDiskCount)
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

// return true if either file or directory exists with given path
func pathExists(path string) (bool) {
    _, err := os.Stat(path)
    return !os.IsNotExist(err)
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

        New TODO: make this compatible with adding in a username - add this to
        the file structure of the user, and also allow passing in locations to
        save in

        Here, storageType is not necessary - should do a parse on each one of the
        disk locations and save that component separately (use the regular
        writers and readers for localhost/EBS, but need slightly special 
        handling for the file otherwise [first create the file locally, and then
        have to send it to the other systems if not local])

        TODO: create folder under user's username if doesn't exist!!, in all of
        the disks that exist locally, just so that the local writers don't have
        any issues

        // storageType int,
*/
func SaveFile(path string, username string, diskLocations []string, configs *types.Config) {
    dataDisks := configs.Datadisks

    // if user does not have a defined structure, create folders for him in all
    // of the drives (maybe can do this in some sort of add user function)
    // or maybe just need to do this for the ones in diskLocations
    for i := 0; i < len(diskLocations); i++ { // maybe just do for diskLocations TODO
        // this won't work for all of the drives, since not all will have the
        // perfect local structure TODO
        fmt.Printf("datadisk: %s\n", dataDisks[i])
        directory := fmt.Sprintf("%s/%s", diskLocations[i], username)
        if !pathExists(directory) {
            os.Mkdir(directory, 0755)
        }
    }

    /*
        Modify diskLocations slightly to point it to saving somewhere locally
        first, and then the components can be sent where they are actually
        destined (if the location is not already local)
    */

    localDiskLocations := make([]string, len(diskLocations))
    for i := 0; i < len(diskLocations); i++ {
        // check if not local, TODO: just assigning it to be equal for now
        localDiskLocations[i] = diskLocations[i]
    }

    filename := filepath.Base(path);
    fmt.Printf("Filename: %s, path = %s\n", filename, path)
    originalFile, err := os.Open(path); check(err);

    fileStat, err := originalFile.Stat(); check(err);
    size := fileStat.Size(); // in bytes
    fmt.Printf("Size of file: %d\n", size)

    // NOTE: this could equivalently be just configs.DataDiskCount
    dataDiskCount := len(diskLocations) - configs.ParityDiskCount
    fmt.Printf("Data disk count: %d\n", dataDiskCount)

    /*
        Calculate length of the strips the file will be divided into
        Add padding to the last strip of the file to be even multiple of
        STRIP_COUNT
    */
    remainder := size % int64(dataDiskCount);
    // may be > types.MAX_BUFFER_SIZE
    var stripLength int64 = int64(math.Ceil(float64(size) / float64(dataDiskCount)));
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
        padding = int64(dataDiskCount);
    } else {
        /*
            Padding in this case is the difference between the file size and the
            calculated size given the strip length
        */
        padding = (stripLength * int64(dataDiskCount)) - size;
    }

    /*
        Initiate the writers and readers
    */

    readRequests := make(chan *readOp, dataDiskCount)
    parityChannel := make(chan []byte, dataDiskCount)
    completionChannel := make(chan int, dataDiskCount + configs.ParityDiskCount)

    // initializes all of the global condition variables and mutexes
    initialize(configs, diskLocations)

    // initiate a reader
    go reader(originalFile, readRequests)

    // initiate a parity writer
    // TODO: this isn't entirely general, assumes only one parity disk regardless
    parityDiskFile := fmt.Sprintf("%s/%s/%s_p", localDiskLocations[len(diskLocations) - 1],
                                  username, filename)
    go parityWriter(parityDiskFile, parityChannel,
                    completionChannel, dataDiskCount)

    // initiate the writers
    for i := int64(0); i < int64(dataDiskCount); i++ {
        // "./storage/drive" + i + "/" + filename + "_" + i,
        storageFile := fmt.Sprintf("%s/%s/%s_%d", localDiskLocations[i],
                                   username, filename, i)
        // if this is the writer responsible for the last strip of the file,
        // must add padding
        if (i == int64(dataDiskCount) - 1) {

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
    for i := 0; i < dataDiskCount; i++ {
        <- completionChannel // may want to get error codes here
    }

    close(parityChannel) // stop the parity writer

    // wait until the parity writer finishes
    <- completionChannel

    close(completionChannel) // don't need it anymore
    close(readRequests) // stop the reader channel

    originalFile.Close()

    /* TODO
        Now, determine if any of the saved components need to be distributed
        to other systems, by parsing through the names

    */

    // file.Close(); <-- currently saveLocalhost does this for you
}

// alternate design: can hold multiple parityStrips in memory and release them
// once done with them (attach a tag to the buffer sent in parityChannel to
// know to which parityStrip this goes to)
func parityWriter(location string, parityChannel chan []byte, completionChannel chan int, 
                  writerCount int) {
    parityFile, err := openFile(location); check(err)

    // unsigned parity strip
    parityStrip := make([]byte, types.MAX_BUFFER_SIZE)
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
            if  len(payload) < types.MAX_BUFFER_SIZE || len(payload) < len(parityStrip) {
                parityStrip = make([]byte, len(payload))
            } else {
                // reset parityStrip
                parityStrip = make([]byte, types.MAX_BUFFER_SIZE)
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

        if localPayloadCount == writerCount { // got all necessary parity bits
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
    fmt.Printf("Opened file\n")
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
        num := int64(math.Min(float64(types.MAX_BUFFER_SIZE), float64(end - currentLocation)))
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

/*
    It has been determined that the drive with ID driveID is corrupted, when
    reading a portion of the file, located in offendingFile. Fix the drive by
    running fsck on it or some variant of this (to stop using the bad sectors
    that corrupted this file), and then rewrite this portion of the file again,
    by getting the correct version of it by using the other drives to recover.
*/
func recoverFromDriveFailure(driveID int, offendingFile *os.File, 
                            offendingFileLocation string, outputFile *os.File, 
                            isParityDisk bool, hadPadding bool, diskLocations []string,
                            username string, configs * types.Config) {
    /*
        Fix the drive, if appropriate
        TODO: this is also not entirely geeneral yet, need to do this for
        other types of systems too - note: the diskLocation passed in here is
        probably not the broken one (we could have fetched this file from
        some other disk, so should fix that external drive too)
    */
    // if storageType == EBS {
    //     driveName := fmt.Sprintf("drive%d", driveID + 1)
    //     cmd := exec.Command("fsck", driveName)

    //     var out bytes.Buffer
    //     cmd.Stdout = &out
    //     err := cmd.Run()
    //     check(err)

    //     fmt.Printf("Fsck stdout: %q\n", out.String())
    // }

    dataDiskCount := len(diskLocations) - configs.ParityDiskCount

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

        otherDriveFiles := make([]*os.File, dataDiskCount - 1)

        parityDriveFileName := fmt.Sprintf("%s/%s/%s_p", diskLocations[len(diskLocations) - 1],
                                  username, rawFileName)

        parityDriveFile, err := os.Open(parityDriveFileName)
        check(err)
        
        count := 0
        for i := 0; i < dataDiskCount; i++ {
            if i != driveID {
                tmpName := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i],
                                        username, rawFileName, i)
                fmt.Printf("Tmp name: %s\n", tmpName)
                otherDriveFiles[count], err = os.Open(tmpName); check(err)

                count++
            }
        }

        fileStat, err := parityDriveFile.Stat(); check(err);
        rawSize := fileStat.Size(); // in bytes
        rawSize -= types.MD5_SIZE
        size := rawSize // subtract size for hash at end

        trueParityStrip := make([]byte, types.MAX_BUFFER_SIZE)
        buf := make([]byte, types.MAX_BUFFER_SIZE)

        var currentLocation int64 = 0
        lastBuffer := false
        currentHash := md5.New()

        for currentLocation != size {
            // check if need to resize the buffers
            if (size - currentLocation) < int64(types.MAX_BUFFER_SIZE) {
                lastBuffer = true
                newSize := size - currentLocation

                trueParityStrip = make([]byte, newSize)
                buf = make([]byte, newSize)
            } else {
                trueParityStrip = make([]byte, types.MAX_BUFFER_SIZE)
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
                for i := len(trueParityStrip) - 1; i >= len(trueParityStrip) - dataDiskCount; i-- {
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

        for i := 0; i < len(otherDriveFiles); i++ {
            otherDriveFiles[i].Close()
        }
        parityDriveFile.Close()

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
        otherDriveFiles := make([]*os.File, dataDiskCount)

        for i := 0; i < dataDiskCount; i++ {
            tmpName := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i],
                                        username, rawFileName, i)
            otherDriveFiles[i], err = os.Open(tmpName); check(err)
        }

        fileStat, err := otherDriveFiles[0].Stat(); check(err);
        size := fileStat.Size(); // in bytes
        size -= types.MD5_SIZE // chop off the hash

        filesParityStrip := make([]byte, types.MAX_BUFFER_SIZE)
        buf := make([]byte, types.MAX_BUFFER_SIZE)

        var currentLocation int64 = 0
        currentHash := md5.New()
        for currentLocation != size {
            // check if need to resize the buffers
            if (size - currentLocation) < int64(types.MAX_BUFFER_SIZE) {
                newSize := size - currentLocation

                filesParityStrip = make([]byte, newSize)
                buf = make([]byte, newSize)
            } else {
                filesParityStrip = make([]byte, types.MAX_BUFFER_SIZE)
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
        for i := 0; i < len(otherDriveFiles); i++ {
            otherDriveFiles[i].Close()
        }

        // File is fixed!
    }

    fixedFile.Close()

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
                       canRecoverChannel chan int, diskLocations []string,
                       username string, configs *types.Config) {
    dataDiskCount := len(diskLocations) - configs.ParityDiskCount

    // read from respective slice, and write it to the output file
    file, err := os.Open(filename); check(err)
    fileStat, err := file.Stat(); check(err);
    rawSize := fileStat.Size(); // in bytes
    size := rawSize

    // subtract the size of the hash at the end to get the true size
    size -= int64(types.MD5_SIZE)

    offsetInOutput := int64(ID) * size
    var position int64 = 0

    currentHash := md5.New()
    lastBuffer := false
    buf := make([]byte, types.MAX_BUFFER_SIZE)
    for position != size {

        if (size - position) <= int64(types.MAX_BUFFER_SIZE) { // will enter conditional at last bit of file
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
            for i := len(buf) - 1; i >= len(buf) - dataDiskCount; i-- {
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
    originalHash := make([]byte, types.MD5_SIZE)
    _, err = file.ReadAt(originalHash, rawSize - types.MD5_SIZE)
    check(err)
    fmt.Printf("Original hash ID %d: %x, length = %d\n", ID, originalHash, len(originalHash))

    var hashesMatch bool = true
    if len(originalHash) != types.MD5_SIZE {
        fmt.Printf("Original hash not correct length\n")
        hashesMatch = false
    }
    for i := 0; i < types.MD5_SIZE; i++ {
        if (finalHash[i] != originalHash[i]) {
            hashesMatch = false
        }
    }

    if !hashesMatch {
        completionChannel <- ID + 1 // to make sure ID is not 0, so that reads as error
        <- canRecoverChannel // wait until master says that this drive can recover
        fmt.Printf("This drive is messed up, ID = %d\n", ID)
        recoverFromDriveFailure(ID, file, filename, outputFile, 
                                false, hasPadding, diskLocations, username,
                                configs)

        fmt.Printf("Successfully fixed drive ID = %d\n", ID)
    }

    file.Close()

    completionChannel <- 0 // success
    fmt.Println("RW exiting")
}

func basicParityChecker(filename string, parityCompletionChannel chan int, 
                        canRecoverChannel chan int, diskLocations []string,
                        username string, configs *types.Config) {
    parityFile, err := os.Open(filename); check(err)

    fileStat, err := parityFile.Stat()
    rawSize := fileStat.Size()
    size := rawSize
    size -= types.MD5_SIZE

    buf := make([]byte, types.MAX_BUFFER_SIZE)
    currentHash := md5.New()

    var currentLocation int64 = 0
    for currentLocation != size {
        if (size - currentLocation) < int64(types.MAX_BUFFER_SIZE) {
            buf = make([]byte, size - currentLocation)
        }

        _, err = parityFile.ReadAt(buf, currentLocation)
        check(err)

        currentHash.Write(buf)

        currentLocation += int64(len(buf))
    }

    finalHash := currentHash.Sum(nil)
    fmt.Printf("Final hash parity: %x, length = %d\n", finalHash, len(finalHash))
    originalHash := make([]byte, types.MD5_SIZE)
    _, err = parityFile.ReadAt(originalHash, rawSize - types.MD5_SIZE)
    check(err)
    fmt.Printf("Original hash parity: %x, length = %d\n", originalHash, len(originalHash))

    var hashesMatch bool = true
    if len(originalHash) != types.MD5_SIZE {
        fmt.Printf("Original hash not correct length\n")
        hashesMatch = false
    }
    for i := 0; i < types.MD5_SIZE; i++ {
        if (finalHash[i] != originalHash[i]) {
            hashesMatch = false
        }
    }

    if !hashesMatch {
        parityCompletionChannel <- 1
        <- canRecoverChannel // wait until master says that this drive can recover
        fmt.Printf("This drive is messed up, ID = parity\n")
        recoverFromDriveFailure(0, parityFile, filename, nil, 
                                true, false, diskLocations, username, configs)

        fmt.Printf("Successfully fixed drive ID = parity\n")
    }

    parityFile.Close()

    parityCompletionChannel <- 0 // success
    fmt.Println("Parity checker exiting.")
}

/*
    Retrieve a file that was saved to the system

    filename = where the file is located in the file system of the person who
    saved the file (just name of file for now)

        Note: can save a SHA-256 hash of the path that a user gave, and that
        can be the name of the file, because the path should be unique to each
        file (there shouldn't be files that are named exactly the same way
        in the same folder)

        Can possibly do hash(user | pathToFile)

    username = username of user asking for this file
    diskLocations = where this file can be found
    configs = configs for the system (where the database is, RAID level, etc.)

    returns the path to the downloaded file

*/
func GetFile(filename string, username string, diskLocations []string, configs *types.Config) string {
    dataDiskCount := len(diskLocations) - configs.ParityDiskCount

    localDiskLocations := diskLocations
    for i := 0; i < len(diskLocations); i++ {
        // check if not already local here
        localDiskLocations[i] = diskLocations[i]
    }

    /*
        TODO: check if any of the disk locations are not localhost or EBS, if
        they are stored somewhere else other than where the server can directly
        access, then fetch them first and place them in the locations the
        basic reader/writers expect (parse the disk locations here)
    */
    for i := 0; i < len(diskLocations); i++ {
        // pass into this function a local locations where to temporarily save
        // the file, should possibly be doing load balancing instead of doing
        // the disk that the server is on - might burn it out faster**
        fetchDiskFileIfNotLocal(diskLocations[i], localDiskLocations[i], filename, username, configs);
    }

    // can delete this after sent in real model
    downloadedFilename := fmt.Sprintf("downloaded-%s", filename)

    fmt.Printf("Creating file: %s\n", downloadedFilename)
    outputFile, err := os.Create(downloadedFilename); check(err)

    completionChannel := make(chan int, dataDiskCount)
    canRecoverChannel := make(chan int, dataDiskCount)
    parityCompletionChannel := make(chan int, configs.ParityDiskCount)

    for i := 0; i < dataDiskCount; i++ {
        sliceFilename := fmt.Sprintf("%s/%s/%s_%d", localDiskLocations[i],
                                  username, filename, i)
        hasPadding := (i == int(dataDiskCount) - 1) // second to last disk has the padding
        go basicReaderWriter(sliceFilename, outputFile, i, hasPadding, 
                            completionChannel, canRecoverChannel,
                            localDiskLocations, username, configs)
    }

    // also create a basic reader to check the correctness of the redundant
    // bits stored on the parity disk
    parityFilename := fmt.Sprintf("%s/%s/%s_p", localDiskLocations[len(localDiskLocations) - 1],
                                  username, filename)
    go basicParityChecker(parityFilename, parityCompletionChannel, 
                        canRecoverChannel, localDiskLocations, username, configs)

    // wait for all of the writers to be done
    numberOfErrors := 0
    // brokenDrive := -1
    for i := 0; i < dataDiskCount; i++ {
        errorCode := <- completionChannel // may want to get error codes here
        if errorCode != 0 { // some drive had an issue
            numberOfErrors++
            // brokenDrive = errorCode
        }

        // got all of the other drives
        if i == dataDiskCount - numberOfErrors {
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

    return downloadedFilename
}

/*
    TODO: check for each file if it can be deleted locally, otherwise issue
    a separate command/function to delete the component on the different
    system
*/
func RemoveFile(filename string, username string, diskLocations []string,
                configs *types.Config) {
    dataDiskCount := len(diskLocations) - configs.ParityDiskCount

    for i := 0; i < dataDiskCount; i++ {
        sliceFilename := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i],
                                  username, filename, i)
        // remove it, if it exists (which it should)
        // TODO: this assumes that the file is stored locally, need to update
        // this later
        if _, err := os.Stat(sliceFilename); !(os.IsNotExist(err)) { // file exists
            os.Remove(sliceFilename)
        }
    }

    parityFilename := fmt.Sprintf("%s/%s/%s_p", diskLocations[len(diskLocations) - 1],
                                  username, filename)
    // remove it, if it exists (which it should)
    if _, err := os.Stat(parityFilename); !(os.IsNotExist(err)) { // file exists
        os.Remove(parityFilename)
    }

    fmt.Printf("Removed file %s\n", filename)
}


func fetchDiskFileIfNotLocal(diskLocation string, fetchLocation string, 
                            filename string, username string, configs *types.Config) {
    /*
        TODO: implement, fetch the disk file if the disk location is not
        accessible locally, put it in the place that writers and readers would
        expect the file if it was local. Return if the disk location is 
        accessible from the server.
    */
}