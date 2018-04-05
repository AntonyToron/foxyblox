/*******************************************************************************
* Author: Antony Toron
* File name: database.go
* Date created: 3/31/18
*
* Description: Defines a pseudo database, that is distributed across multiple
* hard disks for the purpose of redundancy - matching the level of reliability
* of user data in the overall system.
*******************************************************************************/

/*
    Database format:
        per user:
            file -> ([list of locations where components are stored], file size)

    Each user will have an entry on each of the (3) disks that hold this
    database information. The choice for 3 disks is arbitrary and can be scaled
    up later in the case that higher levels of RAID are used.

    Note: can attach up to 27 EBS volumes to an instance for AWS, so will store
    the database on separate disks from the user data.
    
*/

package database

import (
    "fmt"
    "os"
    "log"
    "math"
    "os/exec"
    "bytes"
    "encoding/binary"
    // "time"
)

// storageType
const LOCALHOST int = 0;
const EBS int = 1;

const DISK_COUNT int = 3;
const MAX_DISK_COUNT uint8 = 3;
const REGULAR_FILE_MODE os.FileMode = 0755; // owner can rwx, but everyone else rx but not w
const HEADER_SIZE int64 = 64;
const MAX_FILE_NAME_SIZE int16 = 256 // (in bytes), will only accept ASCII characters for now
const MAX_DISK_NAME_SIZE uint8 = 128
const NUM_PARITY_DISKS  = 1
const POINTER_SIZE = 8
const SIZE_OF_ENTRY = MAX_FILE_NAME_SIZE + 2*(POINTER_SIZE) + int16(MAX_DISK_COUNT) * int16(MAX_DISK_NAME_SIZE)

// entries in header
const HEADER_FILE_SIZE int = 2
const HEADER_DISK_SIZE int = 2
const HEADER_DISK_AMT int = 3 

const BUFFER_SIZE = 8192

type Header struct {
    FileNameSize int16
    DiskCount uint8
    DiskNameSize uint8
    RootPointer int64
    FreeList int64
    TrueDbSize int64
}

type TreeEntry struct {
    Filename string
    Left int64
    Right int64
    Disks []string
}

// check error, exit if non-nil
func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

// return true if either file or directory exists with given path
func pathExists(path string) (bool) {
    _, err := os.Stat(path)
    return !os.IsNotExist(err)
}

func InitializeDatabaseStructure(storageType int, diskLocations []string) (bool) {
    if storageType == LOCALHOST {
        /*
            Create the following structure if it doesn't already exist:

            storage
                drive1
                drive2
                drive3
                drivep

                dbdrive0
                dbdrive1
                dbdrive2
                dbdrivep
        */
        var madeChanges bool = false

        if !pathExists("./storage") {
            os.Mkdir("storage", REGULAR_FILE_MODE)
            madeChanges = true
        }

        for i := 0; i < DISK_COUNT; i++ {
            diskFolder := fmt.Sprintf("./storage/drive%d", i + 1)
            if !pathExists(diskFolder) {
                os.Mkdir(diskFolder, REGULAR_FILE_MODE)
                madeChanges = true
            }
            dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
            if !pathExists(dbdiskFolder) {
                os.Mkdir(dbdiskFolder, REGULAR_FILE_MODE)
                madeChanges = true
            }
        }

        parityFolder := fmt.Sprintf("./storage/drivep")
        dbParityFolder := fmt.Sprintf("./storage/dbdrivep")
        if !pathExists(parityFolder) {
            os.Mkdir(parityFolder, REGULAR_FILE_MODE)
            madeChanges = true
        }
        if !pathExists(dbParityFolder) {
            os.Mkdir(dbParityFolder, REGULAR_FILE_MODE)
            madeChanges = true
        }

        return madeChanges
    } else {
        fmt.Println("Not implemented yet.")
    }

    return false
}

/*
    Recursively remove all files (including stored data and database files)
*/
func RemoveDatabaseStructure(storageType int, diskLocations []string) {
    if storageType == LOCALHOST {

        if pathExists("./storage") {
            cmd := exec.Command("rm", "-rf", "./storage")

            var out bytes.Buffer
            var stderr bytes.Buffer
            cmd.Stdout = &out
            cmd.Stderr = &stderr
            err := cmd.Run()

            if err != nil {
                fmt.Printf("Diff stderr: %q\n", stderr.String())
            }

            fmt.Printf("Diff stdout: %q\n", out.String())
        }
    } else {
        fmt.Println("Not implemented yet.")
    }
}

/*
    Removes all database files relating to this user
*/
func DeleteDatabaseForUser(storageType int, dbdiskLocations []string,
                            username string) {
    if storageType == LOCALHOST {
        for i := 0; i < DISK_COUNT; i++ {
            // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
            dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)

            if pathExists(dbCompLocation) {
                os.Remove(dbCompLocation)
            }
        }

        dbParityFileName := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
        if pathExists(dbParityFileName) {
            os.Remove(dbParityFileName)
        }
    }
}

// should check to see if user already has a database before calling this
// dbdisklocations should be ordered with the IDs of the disks increasing,
// with parity disk(s) at the end
func CreateDatabaseForUser(storageType int, dbdiskLocations []string,
                           username string) {
    if storageType == LOCALHOST {
        parityBuf := make([]byte, HEADER_SIZE)
        for i := 0; i < DISK_COUNT; i++ { //- NUM_PARITY_DISKS
            // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
            dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)

            dbFile, err := os.Create(dbCompLocation)
            check(err)

            /*
                Each database component file should have the following format:

                Header:
                    - size of entry in tree (4 bytes):
                        can be computed from:
                        2 byte entry for size of file names in this system
                        1 byte entry for max amount of drives that a file can 
                        stored across
                        1 byte entry for max length (in bytes) of drive name
                        = (2 bytes + (max drives) * drive name size)
                    - 8 byte pointer to root of this tree (always the same) =
                    arbitrarily define root to be some null node just so that
                    don't have to deal with case where root is deleted
                    - 8 byte pointer to first page in free list,
                    where the free list is a singly-linked list of free entries
                    in the tree in this file 
                        whenever a node is deleted, readjust the links in the
                        tree so that it is stable, but add that freed up space
                        to the free list to use it next time you have to add a 
                        node

                        whenever you add a node, check to see if the free list
                        is pointing to anything other than the end of the file
                        (non-empty list), otherwise, take the root as your spot
                        and then reassign root of this list as the next entry
                        in the list (could be end of the file then)
                    - 8 byte true size of header (not including 0 bytes at end)
                    - (64 - previous entries) extra bytes to leave space for any
                    additional components might need to be added to the header
                    later

                    Altogether: 64 bytes (20 bytes of useful data, as of now)
            */

            h := Header{MAX_FILE_NAME_SIZE, MAX_DISK_COUNT, MAX_DISK_NAME_SIZE, 
                        HEADER_SIZE, HEADER_SIZE + int64(SIZE_OF_ENTRY),
                        HEADER_SIZE + int64(SIZE_OF_ENTRY)}

            buf := new(bytes.Buffer)
            err = binary.Write(buf, binary.LittleEndian, &h)
            check(err)

            header := buf.Bytes()

            // zero bytes
            zeroes := make([]byte, HEADER_SIZE - int64(len(header)))
            header = append(header, zeroes...)

            // write header to database file
            _, err = dbFile.WriteAt(header, 0)
            check(err)

            for j := 0; j < len(parityBuf); j++ {
                parityBuf[j] ^= header[j]
            }

            dbFile.Close()
        }

        // initialize parity drive as the exclusive OR of the header
        // note: all of the database files should be the same size, so whenever
        // writing into a database file, check if this will increase its size,
        // and double the size of all the database files if this is true
        dbParityFileName := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
        dbParityFile, err := os.Create(dbParityFileName); check(err)

        dbParityFile.WriteAt(parityBuf, 0)

        dbParityFile.Close()
    }
}

func resizeAllDbDisks(storageType int, dbdisklocations []string, username string) {
    if storageType == LOCALHOST {
        for i := 0; i < DISK_COUNT; i++ {
            dbFilename := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
            
            dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
            check(err)

            // double the size of the file (write zeroes into the file)
            fileStat, err := dbFile.Stat(); check(err);
            sizeOfDbFile := fileStat.Size(); // in bytes

            numWritten := int64(0)
            for numWritten != sizeOfDbFile {
                bufSize := int64(math.Min(float64(sizeOfDbFile - numWritten), float64(BUFFER_SIZE)))
                buf := make([]byte, bufSize)

                dbFile.WriteAt(buf, sizeOfDbFile + numWritten)
                numWritten += bufSize
            }

            dbFile.Close()
        }

        // add onto the parity file as well (just append 0s accordingly, b/c
        // exclusive OR of 0s is 0)
        dbFilename := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
            
        dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        // double the size of the file (write zeroes into the file)
        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes

        numWritten := int64(0)
        for numWritten != sizeOfDbFile {
            bufSize := int64(math.Min(float64(sizeOfDbFile - numWritten), float64(BUFFER_SIZE)))
            buf := make([]byte, bufSize)

            dbFile.WriteAt(buf, sizeOfDbFile + numWritten)
            numWritten += bufSize
        }

        dbFile.Close()

        fmt.Printf("Resized all of the disks\n")
    }
}

/*
    dbdisklocations = optional, can specify exactly where user wants to store
    the data
*/
func AddFileSpecsToDatabase(filename string, username string, storageType int,
            dbdisklocations []string) {
    if storageType == LOCALHOST {
        if !pathExists("./storage/dbdrive1/" + username + "_1") {
            // dbdisklocations := make([]string, DISK_COUNT)
            // for i := 0; i < DISK_COUNT; i++ {
            //     dl := fmt.Sprintf("./storage/dbdrive%d", i)
            //     dbdisklocations[i] = dl
            // }
            CreateDatabaseForUser(storageType, nil, username)
        }

        /*
            Roughly arbitrary decision in splitting the names across drives
            for somewhat balancing the load:
                0 - 85 = drive1
                86 - 112 = drive2
                113 - 256 = drive3

            sort into a drive based on the first ASCII character of the name,
            reject the name if not ASCII (can handle this later)
            also should check if name is empty

            can make this code more general later
        */

        // file, err := os.Open(filename); check(err) // don't need to open the file 
        // here, just putting specs in db

        dbFilename := ""
        if filename[0] >= 0 && filename[0] <= 85 {
            dbFilename = fmt.Sprintf("./storage/dbdrive0/%s_0", username)
        } else if filename[0] >= 86 && filename[0] <= 112 {
            dbFilename = fmt.Sprintf("./storage/dbdrive1/%s_1", username)
        } else {
            dbFilename = fmt.Sprintf("./storage/dbdrive2/%s_2", username)
        }

        // read in the database file and get root of the tree
        dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)
        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes

        buf := make([]byte, HEADER_SIZE)
        dbFile.ReadAt(buf, 0)

        header := Header{0, 0, 0, 0, 0, 0}
        b := bytes.NewReader(buf)
        err = binary.Read(b, binary.LittleEndian, &header)
        check(err)

        oldHeader := header

        /*
            Tree entry: 
            [256 bytes for file name] [pointer to left child] 
            [pointer to right child] [list of disks, each 128 bytes]
        */

        // entry we want to insert
        targetNode := make([]byte, SIZE_OF_ENTRY)
        for i := 0; i < len(filename); i++ {
            targetNode[i] = filename[i]
        }
        // copy in the file locations
        for i := 0; i < int(header.DiskCount); i++ {
            diskName := fmt.Sprintf("local_storage/drive%d", i + 1)
            for j := 0; j < len(diskName); j++ {
                offset := int(header.FileNameSize) + 2 * POINTER_SIZE + i * int(header.DiskNameSize)
                targetNode[offset + j] = diskName[j]
            }
        }        

        /*
            Traverse the tree until you find a spot that you can insert the
            node into, and then insert it (first priority) into the free list,
            but just append to end of the file if free list is empty or points
            to the end of the file
        */

        // Start at root
        currentNodeLocation := header.RootPointer

        foundInsertionPoint := false
        left := false
        entryBuf := make([]byte, SIZE_OF_ENTRY)
        for !foundInsertionPoint {
            // read in the current node
            dbFile.ReadAt(entryBuf, currentNodeLocation)

            currentFilename := bytes.Trim(entryBuf[0:header.FileNameSize], "\x00")
            currentNode := TreeEntry{string(currentFilename), 
                                     0, 0, []string(nil)}

            b := bytes.NewReader(entryBuf[header.FileNameSize: header.FileNameSize + POINTER_SIZE])
            err := binary.Read(b, binary.LittleEndian, &currentNode.Left); check(err)
            b = bytes.NewReader(entryBuf[header.FileNameSize + POINTER_SIZE: header.FileNameSize + 2 * POINTER_SIZE])
            err = binary.Read(b, binary.LittleEndian, &currentNode.Right); check(err)

            // go left if < (if equals, doesn't make sense to keep going)
            if filename < currentNode.Filename {
                // check if this node has a left child already, if not can add
                if currentNode.Left == 0 {
                    foundInsertionPoint = true
                    left = true
                    fmt.Printf("found insertion point at %s\n", currentNode.Filename)
                } else { // traverse down to left one
                    currentNodeLocation = currentNode.Left
                }
            } else if filename == currentNode.Filename {
                // entry already exists, likely just updating it
                // can handle this later

                foundInsertionPoint = true

            } else { // go right if >
                if currentNode.Right == 0 {
                    foundInsertionPoint = true
                    left = false
                    fmt.Printf("found insertion point at %s\n", currentNode.Filename)
                } else { // traverse down to right one
                    currentNodeLocation = currentNode.Right
                }
            }

            if !foundInsertionPoint {
                entryBuf = make([]byte, SIZE_OF_ENTRY)
            }
        }

        /*
            Actually insert the node into the tree
        */

        // resize the files if this insertion will increase the size of this
        // database file
        for (header.TrueDbSize + int64(SIZE_OF_ENTRY)) > sizeOfDbFile {
            resizeAllDbDisks(storageType, nil, username)

            fileStat, err := dbFile.Stat(); check(err);
            sizeOfDbFile = fileStat.Size(); // in bytes
        }

        // update true db size if will append to end of file
        // if header.FreeList == header.TrueDbSize {
        //     header.TrueHeaderSize += SIZE_OF_ENTRY
        // }

        binaryBuffer := new(bytes.Buffer)
        err = binary.Write(binaryBuffer, binary.LittleEndian, &header.FreeList)
        check(err)

        offsetToPointer := header.FileNameSize
        if !left { // determine which pointer to set it as based on loop
            offsetToPointer += POINTER_SIZE
        }
        _, err = dbFile.WriteAt(binaryBuffer.Bytes(), currentNodeLocation + int64(offsetToPointer))
        check(err)

        // update the true size of the database (we are going to enter a new entry)
        header.TrueDbSize += int64(SIZE_OF_ENTRY)

        // update free list to point to next entry in it
        // pointer to next in free list = first 8 bytes in the
        // entry in free list, if all 0s, then end of free list
        insertionPointBuf := make([]byte, POINTER_SIZE)
        fileStat, err = dbFile.Stat(); check(err);
        sizeOfDbFile = fileStat.Size(); // in bytes
        _, err = dbFile.ReadAt(insertionPointBuf, header.FreeList)
        check(err)
        var pointer int64 = 0
        bufferReader := bytes.NewReader(insertionPointBuf)
        err = binary.Read(bufferReader, binary.LittleEndian, &pointer)
        check(err)
        insertionPoint := header.FreeList
        if pointer == 0 {
            // set free list to point to end of file, empty
            header.FreeList = header.TrueDbSize; // represents end of file if nothing in free list
        } else {
            header.FreeList = pointer;
        }

        // copy in the actual entry now
        _, err = dbFile.WriteAt(targetNode, insertionPoint)
        check(err)

        // push any updates to header
        binaryBuffer = new(bytes.Buffer)
        err = binary.Write(binaryBuffer, binary.LittleEndian, &header)
        check(err)

        newHeaderBuf := binaryBuffer.Bytes()
        _, err = dbFile.WriteAt(newHeaderBuf, 0) // overwrite current header
        check(err)

        // update the parity file to reflect the changes to both the header and
        // the entry
        // need the old data and the new data: do Parity XOR old data XOR new data

        // only edited the parent node and this entered node location, as well as
        // the header (since added to true size of db), and also the part of the
        // free list that we modified

        dbParityFilename := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
        dbParityFile, err := os.OpenFile(dbParityFilename, os.O_RDWR, REGULAR_FILE_MODE)
        check(err)

        parityBuf := make([]byte, len(newHeaderBuf)) // not header_size, since didn't modify the zero bit parts

        // fix the header part
        _, err = dbParityFile.ReadAt(parityBuf, 0)
        check(err)
        x := new(bytes.Buffer)
        err = binary.Write(x, binary.LittleEndian, &oldHeader)
        check(err)
        oldHeaderBuf := x.Bytes()
        for i := 0; i < len(parityBuf); i++ {
            parityBuf[i] ^= oldHeaderBuf[i] // XOR with old data
            parityBuf[i] ^= newHeaderBuf[i] // XOR with new data
        }
        _, err = dbParityFile.WriteAt(parityBuf, 0)
        check(err)

        // fix the parent node part
        parityBuf = make([]byte, SIZE_OF_ENTRY)
        _, err = dbParityFile.ReadAt(parityBuf, currentNodeLocation)
        dataBuf := make([]byte, SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(dataBuf, currentNodeLocation)
        for i := 0; i < len(parityBuf); i++ {
            parityBuf[i] ^= entryBuf[i] // old data
            parityBuf[i] ^= dataBuf[i]  // updated data
        }
        _, err = dbParityFile.WriteAt(parityBuf, currentNodeLocation)
        check(err)

        // fix the new inserted node part
        parityBuf = make([]byte, SIZE_OF_ENTRY)
        _, err = dbParityFile.ReadAt(parityBuf, insertionPoint)
        dataBuf = make([]byte, SIZE_OF_ENTRY)
        for i := 0; i < len(insertionPointBuf); i++ {
            dataBuf[i] = insertionPointBuf[i]
        }
        for i := 0; i < len(parityBuf); i++ {
            parityBuf[i] ^= dataBuf[i]    // old data (i.e. pointer + zeroes)
            parityBuf[i] ^= targetNode[i] // new data
        }
        _, err = dbParityFile.WriteAt(parityBuf, insertionPoint)
        check(err)

        dbFile.Close()
        dbParityFile.Close()

        fmt.Printf("Successfully added filename: %s to the database\n", filename)
    }
}

// here, storageType is in reference to where the database is stored
func GetFileEntry(storageType int, filename string, username string) (*TreeEntry) {
    if storageType == LOCALHOST {
        dbFilename := ""
        if filename[0] >= 0 && filename[0] <= 85 {
            dbFilename = fmt.Sprintf("./storage/dbdrive0/%s_0", username)
        } else if filename[0] >= 86 && filename[0] <= 112 {
            dbFilename = fmt.Sprintf("./storage/dbdrive1/%s_1", username)
        } else {
            dbFilename = fmt.Sprintf("./storage/dbdrive2/%s_2", username)
        }

        // read in the database file and get root of the tree
        dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)

        buf := make([]byte, HEADER_SIZE)
        _, err = dbFile.ReadAt(buf, 0)
        check(err)

        header := Header{0, 0, 0, 0, 0, 0}
        binaryReader := bytes.NewReader(buf)
        err = binary.Read(binaryReader, binary.LittleEndian, &header)
        check(err)

        /*
            Traverse the tree until you find a spot that you can insert the
            node into, and then insert it (first priority) into the free list,
            but just append to end of the file if free list is empty or points
            to the end of the file
        */

        // Start at root
        currentNodeLocation := header.RootPointer

        foundFileOrLeaf := false
        foundFile := false
        currentNode := TreeEntry{"", 0, 0, nil}
        for !foundFileOrLeaf {
            // read in the current node
            buf := make([]byte, SIZE_OF_ENTRY)
            dbFile.ReadAt(buf, currentNodeLocation)

            currentFilename := bytes.Trim(buf[0:header.FileNameSize], "\x00")
            currentNode = TreeEntry{string(currentFilename), 
                                     0, 0, []string(nil)}

            fmt.Printf("Currently on node: %s\n", currentNode.Filename)

            b := bytes.NewReader(buf[header.FileNameSize: header.FileNameSize + POINTER_SIZE])
            err := binary.Read(b, binary.LittleEndian, &currentNode.Left); check(err)
            b = bytes.NewReader(buf[header.FileNameSize + POINTER_SIZE: header.FileNameSize + 2 * POINTER_SIZE])
            err = binary.Read(b, binary.LittleEndian, &currentNode.Right); check(err)

            // go left if < (if equals, doesn't make sense to keep going)
            if filename < currentNode.Filename {
                // reached leaf, file not here
                if currentNode.Left == 0 {
                    foundFileOrLeaf = true
                } else { // traverse down to left one
                    currentNodeLocation = currentNode.Left
                }
            } else if filename == currentNode.Filename {
                foundFileOrLeaf = true
                foundFile = true

                // if we really know we found it, then copy in the disk locations
                // now, weren't necessary until now
                currentNode.Disks = make([]string, MAX_DISK_COUNT)
                for i := 0; i < int(header.DiskCount); i++ {
                    upperBound := int(header.FileNameSize) + 2 * int(POINTER_SIZE) + (i + 1) * int(header.DiskNameSize)
                    lowerBound := int(header.FileNameSize) + 2 * int(POINTER_SIZE) + i * int(header.DiskNameSize)
                    currentNode.Disks[i] = string(bytes.Trim(buf[lowerBound:upperBound], "\x00"))
                }

            } else { // go right if >
                if currentNode.Right == 0 {
                    foundFileOrLeaf = true
                } else { // traverse down to right one
                    fmt.Printf("travelling right\n")
                    currentNodeLocation = currentNode.Right
                }
            }
        }

        dbFile.Close()

        if foundFile {
            fmt.Printf("Successfully got the file\n")
            return &currentNode
        } else {
            return nil
        }
    }

    return nil
}

/*
    Fix the tree first, and then add that spot into the free list, return
    errorcode (0 = success, 1 = did not find file)
*/
func DeleteFileEntry(storageType int, filename string, username string) (int) {
    if storageType == LOCALHOST {
        dbFilename := ""
        if filename[0] >= 0 && filename[0] <= 85 {
            dbFilename = fmt.Sprintf("./storage/dbdrive0/%s_0", username)
        } else if filename[0] >= 86 && filename[0] <= 112 {
            dbFilename = fmt.Sprintf("./storage/dbdrive1/%s_1", username)
        } else {
            dbFilename = fmt.Sprintf("./storage/dbdrive2/%s_2", username)
        }

        // read in the database file and get root of the tree
        dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        buf := make([]byte, HEADER_SIZE)
        dbFile.ReadAt(buf, 0)

        header := Header{0, 0, 0, 0, 0, 0}
        binaryReader := bytes.NewReader(buf)
        err = binary.Read(binaryReader, binary.LittleEndian, &header)
        check(err)

        oldHeader := header

        // Start at root
        currentNodeLocation := header.RootPointer
        currentNode := TreeEntry{"", 0, 0, nil}
        var parentNodeLocation int64 = 0

        foundFileOrLeaf := false
        foundFile := false
        rightChild := false
        currentNodeBuf := make([]byte, SIZE_OF_ENTRY)
        var parentNodeBuf []byte = nil
        for !foundFileOrLeaf {
            // read in the current node
            _, err = dbFile.ReadAt(currentNodeBuf, currentNodeLocation)
            check(err)

            currentFilename := bytes.Trim(currentNodeBuf[0:header.FileNameSize], "\x00")
            currentNode = TreeEntry{string(currentFilename), 
                                     0, 0, []string(nil)}

            b := bytes.NewReader(currentNodeBuf[header.FileNameSize: header.FileNameSize + POINTER_SIZE])
            err := binary.Read(b, binary.LittleEndian, &currentNode.Left); check(err)
            b = bytes.NewReader(currentNodeBuf[header.FileNameSize + POINTER_SIZE: header.FileNameSize + 2 * POINTER_SIZE])
            err = binary.Read(b, binary.LittleEndian, &currentNode.Right); check(err)

            // go left if < (if equals, doesn't make sense to keep going)
            if filename < currentNode.Filename {
                // reached leaf, file not here
                if currentNode.Left == 0 {
                    foundFileOrLeaf = true
                } else { // traverse down to left one
                    parentNodeLocation = currentNodeLocation
                    currentNodeLocation = currentNode.Left 
                    rightChild = false
                }
            } else if filename == currentNode.Filename {
                foundFileOrLeaf = true
                foundFile = true
            } else { // go right if >
                if currentNode.Right == 0 {
                    foundFileOrLeaf = true
                } else { // traverse down to right one
                    parentNodeLocation = currentNodeLocation
                    currentNodeLocation = currentNode.Right
                    rightChild = true
                }
            }

            if !foundFile {
                parentNodeBuf = currentNodeBuf
                currentNodeBuf = make([]byte, SIZE_OF_ENTRY)
            }
        }

        if !foundFile {
            fmt.Printf("Did not find the file\n")
            dbFile.Close()
            return 1
        }
        
        /*
            Delete the file:
                Reclaim that memory by adding it to the free list
                Fix the tree by making nodes move up in the tree
        */

        // reclaim the memory, by prepending to the list, update link to root
        // in header

        /*
            Update the free list pointer: prepend this space to the list

            TODO: prepending isn't actually the best thing to do here, the
            spaces all the way at the beginning of the file kind of start to
            starve
                -> could be shrinking the database is the file is too large
        */
        buf = make([]byte, SIZE_OF_ENTRY)
        p := new(bytes.Buffer)
        err = binary.Write(p, binary.LittleEndian, &header.FreeList)
        check(err)
        freeListPointer := p.Bytes()
        for i := 0; i < len(freeListPointer); i++ {
            buf[i] = freeListPointer[i]
        }

        oldEntry := make([]byte, SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(oldEntry, currentNodeLocation)
        check(err)

        _, err = dbFile.WriteAt(buf, currentNodeLocation)
        check(err)

        header.FreeList = currentNodeLocation

        // update parity file to reflect this change
        dbParityFilename := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
        dbParityFile, err := os.OpenFile(dbParityFilename, os.O_RDWR, REGULAR_FILE_MODE)
        check(err)
        parityBuf := make([]byte, SIZE_OF_ENTRY)
        _, err = dbParityFile.ReadAt(parityBuf, currentNodeLocation)
        check(err)
        for i := 0; i < len(parityBuf); i++ {
            if currentNodeLocation + int64(i) == 2952 {
                fmt.Printf("parityBuf = %x, oldEntry = %x, buf = %x\n", parityBuf[i], oldEntry[i], buf[i])
            }
            parityBuf[i] ^= oldEntry[i]
            parityBuf[i] ^= buf[i]
            if currentNodeLocation + int64(i) == 2952 {
                fmt.Printf("after: %x\n", parityBuf[i])
            }
        }
        fmt.Printf("WRITING PARITY BUF TO %d\n", currentNodeLocation)
        _, err = dbParityFile.WriteAt(parityBuf, currentNodeLocation)
        check(err)
        // fix the tree
        // https://en.wikipedia.org/wiki/Binary_search_tree#Deletion

        offsetInParent := header.FileNameSize
        if rightChild {
            offsetInParent += POINTER_SIZE
        }
        /*
            If node has no children, just remove it
            If node has only one child, then replace the node with that child
            If node has two children, find the leftmost node in the right
            subtree and replace the node with that node (and then if that left-
            most node has a right-child, replace it with that child)
        */
        if currentNode.Left == 0 && currentNode.Right == 0 {
            // remove from parent node's children
            buf := make([]byte, POINTER_SIZE)
            _, err  = dbFile.WriteAt(buf, parentNodeLocation + int64(offsetInParent))
            check(err)

            // update parity file
            parityBuf := make([]byte, POINTER_SIZE)
            _, err = dbParityFile.ReadAt(parityBuf, parentNodeLocation + int64(offsetInParent))
            check(err)
            for i := 0; i < len(parityBuf); i++ {
                parityBuf[i] ^= parentNodeBuf[i + int(offsetInParent)]
                parityBuf[i] ^= buf[i]
            }

            _, err = dbParityFile.WriteAt(parityBuf, parentNodeLocation + int64(offsetInParent))
            check(err)
        } else if currentNode.Left == 0 || currentNode.Right == 0 {
            childLocation := currentNode.Left
            if currentNode.Left == 0 {
                childLocation = currentNode.Right
            }

            buf := new(bytes.Buffer)
            err = binary.Write(buf, binary.LittleEndian, &childLocation)
            check(err)

            newPointer := buf.Bytes()
            _, err = dbFile.WriteAt(newPointer, parentNodeLocation + int64(offsetInParent))
            check(err)

            // update parity file
            parityBuf := make([]byte, POINTER_SIZE)
            _, err = dbParityFile.ReadAt(parityBuf, parentNodeLocation + int64(offsetInParent))
            check(err)
            newPointer = buf.Bytes()
            for i := 0; i < len(parityBuf); i++ {
                parityBuf[i] ^= parentNodeBuf[i + int(offsetInParent)]
                parityBuf[i] ^= newPointer[i]
            }

            _, err = dbParityFile.WriteAt(parityBuf, parentNodeLocation + int64(offsetInParent))
            check(err)
        } else {
            // find leftmost node in right subtree
            var candidateNodeLocation int64 = currentNode.Right
            var candidateParentLocation int64 = currentNodeLocation
            candidateNode := TreeEntry{"", 0, 0, nil}
            var foundLeftMost bool = false
            candidateBuf := make([]byte, SIZE_OF_ENTRY)
            candidateParentBuf := currentNodeBuf
            for !foundLeftMost {
                // read in the current node
                _, err = dbFile.ReadAt(buf, candidateNodeLocation)
                check(err)

                candidateNode := TreeEntry{string(buf[0:header.FileNameSize]), 
                                         0, 0, []string(nil)}

                b := bytes.NewReader(buf[header.FileNameSize: header.FileNameSize + POINTER_SIZE])
                err := binary.Read(b, binary.LittleEndian, &candidateNode.Left); check(err)
                b = bytes.NewReader(buf[header.FileNameSize + POINTER_SIZE: header.FileNameSize + 2 * POINTER_SIZE])
                err = binary.Read(b, binary.LittleEndian, &candidateNode.Right); check(err)

                // if doesn't have a left child, then it is the leftmost
                if candidateNode.Left == 0 {
                    foundLeftMost = true
                } else {
                    candidateParentLocation = candidateNodeLocation
                    candidateNodeLocation = candidateNode.Left
                }

                if !foundLeftMost {
                    candidateParentBuf = candidateBuf
                    candidateBuf = make([]byte, SIZE_OF_ENTRY)
                }
            }

            // overwrite original node that is being deleted with candidate node (link its parent to this new node)
            buf := new(bytes.Buffer)
            err = binary.Write(buf, binary.LittleEndian, &candidateNodeLocation)
            check(err)
            newPointer := buf.Bytes()
            _, err = dbFile.WriteAt(newPointer, parentNodeLocation + int64(offsetInParent))
            check(err)

            // update parity file
            parityBuf := make([]byte, POINTER_SIZE)
            _, err = dbParityFile.ReadAt(parityBuf, parentNodeLocation + int64(offsetInParent))
            check(err)
            newPointer = buf.Bytes()
            for i := 0; i < len(parityBuf); i++ {
                parityBuf[i] ^= parentNodeBuf[i + int(offsetInParent)]
                parityBuf[i] ^= newPointer[i]
            }

            _, err = dbParityFile.WriteAt(parityBuf, parentNodeLocation + int64(offsetInParent))
            check(err)

            // have the candidate node inherit the original node's links (left link now)
            buf = new(bytes.Buffer)
            err = binary.Write(buf, binary.LittleEndian, &currentNode.Left)
            check(err)
            fmt.Printf("Inheriting the following left link: %d\n", currentNode.Left)

            newPointer = buf.Bytes()
            _, err = dbFile.WriteAt(newPointer, candidateNodeLocation + int64(header.FileNameSize))
            check(err)

            // update parity file
            parityBuf = make([]byte, POINTER_SIZE)
            _, err = dbParityFile.ReadAt(parityBuf, candidateNodeLocation + int64(header.FileNameSize))
            check(err)
            newPointer = buf.Bytes()
            for i := 0; i < len(parityBuf); i++ {
                parityBuf[i] ^= candidateBuf[i + int(header.FileNameSize)]
                parityBuf[i] ^= newPointer[i]
            }

            _, err = dbParityFile.WriteAt(parityBuf, candidateNodeLocation + int64(header.FileNameSize))
            check(err)

            // have the candidate node inherit the original node's links (right link now)
            // HOWEVER, if candidate node is the immediate right child of currentNode,
            // we don't need to do this, since it's link to its child will already be correct
            // we will mess it up if we do this (TODO: can make this cleaner later)
            if (candidateParentLocation != currentNodeLocation) {
                buf = new(bytes.Buffer)
                err = binary.Write(buf, binary.LittleEndian, &currentNode.Right)
                check(err)
                newPointer = buf.Bytes()
                _, err = dbFile.WriteAt(newPointer, candidateNodeLocation + int64(header.FileNameSize) + int64(POINTER_SIZE))
                check(err)

                // update parity file
                parityBuf = make([]byte, POINTER_SIZE)
                _, err = dbParityFile.ReadAt(parityBuf, candidateNodeLocation + int64(header.FileNameSize) + int64(POINTER_SIZE))
                check(err)
                for i := 0; i < len(parityBuf); i++ {
                    parityBuf[i] ^= candidateBuf[i + int(header.FileNameSize) + int(POINTER_SIZE)]
                    parityBuf[i] ^= newPointer[i]
                }

                _, err = dbParityFile.WriteAt(parityBuf, candidateNodeLocation + int64(header.FileNameSize) + int64(POINTER_SIZE))
                check(err)
            }

            randomBuf := make([]byte, 1)
            _, err = dbParityFile.ReadAt(randomBuf, 2952)
            fmt.Printf("Afterx: %x\n", randomBuf[0])
            /*
                Last check: did that node you used as a replacement have any children?
                It couldn't have had a left child, but if it had a right child, then
                that child should replace you in that position

                caveat: if your parent, as that child, still should be the same, i.e.
                still the replacement node, then have to handle that (just putting
                that case in the if statement for now)

                that issue just comes up now because different node names are referring
                to the same person now, and sometimes to a node that won't exist anymore
                (i.e. the deleted one)

            */
            if candidateNode.Right != 0 && (candidateParentLocation != currentNodeLocation) {
                buf := new(bytes.Buffer)
                err = binary.Write(buf, binary.LittleEndian, &candidateNode.Right)
                check(err)

                fmt.Printf("Here for some reason\n")

                // since it will now be the left child of the parent of candidate node
                // ^ not true always (candidate node could be immediate to the right of currentNode)
                // need to check if the parent node of candidate node is still currentNode
                offsetInCandPar := header.FileNameSize

                newPointer := buf.Bytes()
                _, err = dbFile.WriteAt(newPointer, candidateParentLocation + int64(offsetInCandPar))
                check(err)

                // update parity file
                parityBuf := make([]byte, POINTER_SIZE)
                _, err = dbParityFile.ReadAt(parityBuf, candidateParentLocation + int64(offsetInCandPar))
                check(err)
                for i := 0; i < len(parityBuf); i++ {
                    parityBuf[i] ^= candidateParentBuf[i + int(offsetInParent)]
                    parityBuf[i] ^= newPointer[i]
                }

                _, err = dbParityFile.WriteAt(parityBuf, candidateParentLocation + int64(offsetInCandPar))
                check(err)
            } else if candidateNode.Right == 0 && (candidateParentLocation != currentNodeLocation) { // else, just overwrite that link with 0
                newPointer := make([]byte, POINTER_SIZE)

                // since it will now be the left child of the parent of candidate node
                // ^ not true always (candidate node could be immediate to the right of currentNode)
                // need to check if the parent node of candidate node is still currentNode
                offsetInCandPar := header.FileNameSize
                fmt.Printf("Here for some reason\n")
                _, err = dbFile.WriteAt(newPointer, candidateParentLocation + int64(offsetInCandPar))
                check(err)

                // update parity file
                parityBuf := make([]byte, POINTER_SIZE)
                _, err = dbParityFile.ReadAt(parityBuf, candidateParentLocation + int64(offsetInCandPar))
                check(err)
                for i := 0; i < len(parityBuf); i++ {
                    parityBuf[i] ^= candidateParentBuf[i + int(offsetInParent)]
                    parityBuf[i] ^= newPointer[i]
                }

                _, err = dbParityFile.WriteAt(parityBuf, candidateParentLocation + int64(offsetInCandPar))
                check(err)
            } // don't need to do anything in other case, because link from candidate to its child is already correct

            randomBuf = make([]byte, 1)
            _, err = dbParityFile.ReadAt(randomBuf, 2952)
            fmt.Printf("Afterx: %x\n", randomBuf[0])
        }

        // update the true size of the database (we removed an entry, so freed
        // up some space)
        header.TrueDbSize -= int64(SIZE_OF_ENTRY)

        // rewrite the header
        binaryBuffer := new(bytes.Buffer)
        err = binary.Write(binaryBuffer, binary.LittleEndian, &header)
        check(err)

        newHeaderBuf := binaryBuffer.Bytes()
        _, err = dbFile.WriteAt(newHeaderBuf, 0) // overwrite current header
        check(err)

        // update the parity file to reflect the changes to both the header and
        // the entry
        // need the old data and the new data: do Parity XOR old data XOR new data

        // only edited the parent node and this entered node location, as well as
        // the header (since added to true size of db), and also the part of the
        // free list that we modified

        parityBuf = make([]byte, len(newHeaderBuf))

        // fix the header part
        _, err = dbParityFile.ReadAt(parityBuf, 0)
        check(err)
        x := new(bytes.Buffer)
        err = binary.Write(x, binary.LittleEndian, &oldHeader)
        check(err)
        oldHeaderBuf := x.Bytes()
        for i := 0; i < len(parityBuf); i++ {
            parityBuf[i] ^= oldHeaderBuf[i] // XOR with old data
            parityBuf[i] ^= newHeaderBuf[i] // XOR with new data
        }

        _, err = dbParityFile.WriteAt(parityBuf, 0)
        check(err)

        // FOR SOME REASON, THE BELOW CODE BREAKS THINGS...
        // fileStat, err := dbParityFile.Stat(); check(err);
        // sizeOfDbFile := fileStat.Size(); // in bytes
        // fmt.Printf("size later: %d\n", sizeOfDbFile)
        // randomBuf := make([]byte, 1)
        // _, err = dbParityFile.ReadAt(randomBuf, 2952)
        // check(err)
        // fmt.Printf("After after: %x\n", randomBuf[0])

        dbFile.Close()
        dbParityFile.Close()

        fmt.Printf("Successfully deleted node with filename %s\n", currentNode.Filename)
    
        return 0 // success
    }

    return 1
}

func bufferToEntry(buf []byte, filenamesize int16, disknamesize uint8, diskcount uint8) (*TreeEntry) {
    currentFilename := bytes.Trim(buf[0:filenamesize], "\x00")
    currentNode := TreeEntry{string(currentFilename), 0, 0, []string(nil)}

    b := bytes.NewReader(buf[filenamesize: filenamesize + POINTER_SIZE])
    err := binary.Read(b, binary.LittleEndian, &currentNode.Left); check(err)
    b = bytes.NewReader(buf[filenamesize + POINTER_SIZE: filenamesize + 2 * POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &currentNode.Right); check(err)

    currentNode.Disks = make([]string, MAX_DISK_COUNT)
    for i := 0; i < int(diskcount); i++ {
        upperBound := int(filenamesize) + 2 * int(POINTER_SIZE) + (i + 1) * int(disknamesize)
        lowerBound := int(filenamesize) + 2 * int(POINTER_SIZE) + i * int(disknamesize)
        currentNode.Disks[i] = string(bytes.Trim(buf[lowerBound:upperBound], "\x00"))
    }

    return &currentNode
}

func printTree(entry *TreeEntry, header *Header, dbFile *os.File, arr []string, level int) {
    if entry != nil {
        // fmt.Printf("%s\n", entry.Filename)
        arr[level] += entry.Filename + " "
        if entry.Left != 0 {
            entryBuf := make([]byte, SIZE_OF_ENTRY)
            _, err := dbFile.ReadAt(entryBuf, entry.Left)
            check(err)
            child := bufferToEntry(entryBuf, header.FileNameSize, header.DiskNameSize, header.DiskCount)
        
            printTree(child, header, dbFile, arr, level + 1)
        } else {
            printTree(nil, nil, nil, arr, level + 1)
        }
        if entry.Right != 0 {
            entryBuf := make([]byte, SIZE_OF_ENTRY)
            _, err := dbFile.ReadAt(entryBuf, entry.Right)
            check(err)
            child := bufferToEntry(entryBuf, header.FileNameSize, header.DiskNameSize, header.DiskCount)

            printTree(child, header, dbFile, arr, level + 1)
        } else {
            printTree(nil, nil, nil, arr, level + 1)
        }

        // fmt.Printf("\n")
    } else {
        arr[level] += "X "
    }
}

func PrettyPrintTree(storageType int, username string) {
    for i := 0; i < DISK_COUNT; i++ {
        fmt.Printf("Pretty printing tree for disk %d:\n\n", i)
        // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
        dbFileName := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
        dbFile, err := os.Open(dbFileName)
        check(err)

        headerBuf := make([]byte, HEADER_SIZE)
        _, err = dbFile.ReadAt(headerBuf, 0)
        check(err)

        header := Header{0, 0, 0, 0, 0, 0}
        b := bytes.NewReader(headerBuf)
        err = binary.Read(b, binary.LittleEndian, &header)
        check(err)

        entryBuf := make([]byte, SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(entryBuf, header.RootPointer)
        check(err)

        entry := bufferToEntry(entryBuf, header.FileNameSize, header.DiskNameSize, header.DiskCount)
        
        arr := make([]string, 15)
        for i := 0; i < len(arr); i++ {
            arr[i] = ""
        }

        printTree(entry, &header, dbFile, arr, 0)
        for i := 0; i < len(arr); i++ {
            fmt.Printf("%s\n", arr[i])
        }

        dbFile.Close()

        fmt.Printf("\n\n")
    }
}

func PrettyPrintTreeGetString(storageType int, username string, disk int) ([]string) {
    // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
    dbFileName := fmt.Sprintf("./storage/dbdrive%d/%s_%d", disk, username, disk)
    dbFile, err := os.Open(dbFileName)
    check(err)

    headerBuf := make([]byte, HEADER_SIZE)
    _, err = dbFile.ReadAt(headerBuf, 0)
    check(err)

    header := Header{0, 0, 0, 0, 0, 0}
    b := bytes.NewReader(headerBuf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    entryBuf := make([]byte, SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(entryBuf, header.RootPointer)
    check(err)

    entry := bufferToEntry(entryBuf, header.FileNameSize, header.DiskNameSize, header.DiskCount)
    
    arr := make([]string, 11)
    for i := 0; i < len(arr); i++ {
        arr[i] = ""
    }

    printTree(entry, &header, dbFile, arr, 0)


    dbFile.Close()

    return arr
}