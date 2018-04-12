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
    // "math"
    "os/exec"
    "bytes"
    "encoding/binary"
    "foxyblox/database/transaction"
    "foxyblox/types"
    // "time"
)

type Header struct {
    FileNameSize int16
    DiskCount uint8
    DiskNameSize uint8
    RootPointer int64
    FreeList int64
    TrueDbSize int64
}

// type types.TreeEntry struct {
//     Filename string
//     Left int64
//     Right int64
//     Disks []string
// }

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

// return a tree entry corresponding to the read buffer
func bufferToEntry(buf []byte, header *Header, configs *types.Config) (*types.TreeEntry) {
    currentFilename := bytes.Trim(buf[0:header.FileNameSize], "\x00")
    currentNode := types.TreeEntry{string(currentFilename), 0, 0, []string(nil)}

    b := bytes.NewReader(buf[header.FileNameSize: header.FileNameSize + types.POINTER_SIZE])
    err := binary.Read(b, binary.LittleEndian, &currentNode.Left); check(err)
    b = bytes.NewReader(buf[header.FileNameSize + types.POINTER_SIZE: header.FileNameSize + 2 * types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &currentNode.Right); check(err)

    currentNode.Disks = make([]string, configs.DataDiskCount)
    for i := 0; i < int(header.DiskCount); i++ {
        upperBound := int(header.FileNameSize) + 2 * int(types.POINTER_SIZE) + (i + 1) * int(header.DiskNameSize)
        lowerBound := int(header.FileNameSize) + 2 * int(types.POINTER_SIZE) + i * int(header.DiskNameSize)
        currentNode.Disks[i] = string(bytes.Trim(buf[lowerBound:upperBound], "\x00"))
    }

    return &currentNode
}

// get the header in this dbFile
func getHeader(dbFile *os.File) (Header) {
    buf := make([]byte, types.HEADER_SIZE)
    _, err := dbFile.ReadAt(buf, 0)
    check(err)

    var header Header
    b := bytes.NewReader(buf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    return header
}

// get the database that this file is stored in
func getDbFilenameForFile(filename string, username string, configs *types.Config) string {
    var dbFilename string = ""
    if configs.DataDiskCount == 3 {
        if filename[0] >= 0 && filename[0] <= 85 {
            dbFilename = fmt.Sprintf("%s/%s_0", configs.Dbdisks[0], username)
        } else if filename[0] >= 86 && filename[0] <= 112 {
            dbFilename = fmt.Sprintf("%s/%s_1", configs.Dbdisks[1], username)
        } else {
            dbFilename = fmt.Sprintf("%s/%s_2", configs.Dbdisks[2], username)
        }
    } else {
        evenSplit := types.ASCII / configs.DataDiskCount // ASCII
        disk := int(filename[0]) / evenSplit // might break if not ASCII
        if disk < 0 || disk > configs.DataDiskCount {
            fmt.Printf("Weird character\n")
            disk = 0
        }
        dbFilename = fmt.Sprintf("%s/%s_%d", configs.Dbdisks[disk], username, disk)
    }
    return dbFilename
}

// get the locations of the database for the storage type given
// can store these in a config file (configured manually) and read from there
// the first entry in the array is the database that this file will be stored on
func getDbFilenames(username string, filename string, configs *types.Config) []string {
    var dbFilenames []string = make([]string, configs.DataDiskCount)

    rootFilename := getDbFilenameForFile(filename, username, configs)
    dbFilenames[0] = rootFilename
    count := 1
    for i := 0; i < configs.DataDiskCount; i++ {
        dbFilename := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)
        if dbFilename != rootFilename {
            dbFilenames[count] = dbFilename
            count++
        }
    }

    return dbFilenames
}

// return parity disk this is stored on based on storage
func getParityFilename(username string, filename string, configs *types.Config) string {
    dbFilename := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username)

    return dbFilename
}

func createIntermediateMkdir(path string) {
    cmd := exec.Command("mkdir", "-p", path)

    var out bytes.Buffer
    var stderr bytes.Buffer
    cmd.Stdout = &out
    cmd.Stderr = &stderr
    err := cmd.Run()
    check(err)
}

func InitializeDatabaseStructureLocal() bool {
    var madeChanges bool = false

    if !pathExists("./storage") {
        os.Mkdir("storage", types.REGULAR_FILE_MODE)
        madeChanges = true
    }

    for i := 0; i < int(types.MAX_DISK_COUNT) + 1; i++ {
        // diskFolder := fmt.Sprintf("./storage/drive%d", i)
        // if !pathExists(diskFolder) {
        //     os.Mkdir(diskFolder, types.REGULAR_FILE_MODE)
        //     madeChanges = true
        // }
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if !pathExists(dbdiskFolder) {
            os.Mkdir(dbdiskFolder, types.REGULAR_FILE_MODE)
            madeChanges = true
        }
    }

    return madeChanges
}

// maybe get rid of storageType and just pass in the locations (since always
// will be localhost or EBS anyway)
// note: dbdisklocations here can be subfolders in the drives
func InitializeDatabaseStructure(dbdiskLocations []string) (bool) {
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

    for i := 0; i < len(dbdiskLocations); i++ {
        if !pathExists(dbdiskLocations[i]) {
            createIntermediateMkdir(dbdiskLocations[i])
            madeChanges = true
        }
    }

    return madeChanges
}

/*
    Recursively remove all files (including stored data and database files)
*/
func RemoveDatabaseStructureLocal() {
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
}

func RemoveDatabaseStructure(diskLocations []string) {
    for i := 0; i < len(diskLocations); i++ {
        cmd := exec.Command("rm", "-rf", diskLocations[i])

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
}

/*
    Removes all database files relating to this user
*/
func DeleteDatabaseForUser(username string, configs *types.Config) {
    for i := 0; i < configs.DataDiskCount; i++ {
        // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
        dbCompLocation := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)

        if pathExists(dbCompLocation) {
            os.Remove(dbCompLocation)
        }
    }

    dbParityFileName := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username)
    if pathExists(dbParityFileName) {
        os.Remove(dbParityFileName)
    }
}

// should check to see if user already has a database before calling this
// dbdisklocations should be ordered with the IDs of the disks increasing,
// with parity disk(s) at the end
// Note: no transactions used here because this is database creation, not all
// of the files even exist yet. This can be updated later to have a more robust
// way of determining if there was an unexpected server crash in this function
func CreateDatabaseForUser(username string, configs *types.Config) {
    parityBuf := make([]byte, types.HEADER_SIZE)
    for i := 0; i < configs.DataDiskCount; i++ { //- NUM_PARITY_DISKS
        // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
        // can make configs.Dbdisks have paths within the disk too, not just
        // root of the disk (so that it looks nicer in some folder)
        dbCompLocation := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)

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
                Edit: possibly forcing this to be same size as entry, so that
                adding actions to a transaction is easier (all the same size)
                ^ might not need this though, can decrease later if possible
        */

        // used to be MAX_DISK_COUNT
        h := Header{types.MAX_FILE_NAME_SIZE, uint8(configs.DataDiskCount), types.MAX_DISK_NAME_SIZE, 
                    types.HEADER_SIZE, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY)}

        buf := new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &h)
        check(err)

        header := buf.Bytes()

        // zero bytes
        zeroes := make([]byte, types.HEADER_SIZE - int64(len(header)))
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
    dbParityFileName := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username)
    dbParityFile, err := os.Create(dbParityFileName); check(err)

    dbParityFile.WriteAt(parityBuf, 0)

    dbParityFile.Close()
}

/*
    Used to keep all disks consistent same size, just so that XORing and updating
    the parity disk is easier

    TODO: make this a transaction in some way, maybe with one "action" that
    doesn't really do anything and when you recover, you just check if all of
    the disks are the same
*/
func resizeAllDbDisks(username string, configs *types.Config) {
    // add onto the parity file (just append 0s accordingly, b/c
    // exclusive OR of 0s is 0) - NOTE: resizing the parity disk first,
    // because then if resizing is interrupted, the future writes to
    // the regular disks won't cause EOF on the parity disk
    dbParityFilename := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username)
        
    dbParityFile, err := os.OpenFile(dbParityFilename, os.O_RDWR, 0755)
    check(err)

    // double the size of the file (write zeroes into the file)
    fileStat, err := dbParityFile.Stat(); check(err);
    sizeOfDbFile := fileStat.Size(); // in bytes

    buf := make([]byte, types.POINTER_SIZE)
    _, err = dbParityFile.WriteAt(buf, sizeOfDbFile*2 - types.POINTER_SIZE)
    check(err)

    dbParityFile.Close()

    // resize all of the other disks
    for i := 0; i < configs.DataDiskCount; i++ {
        dbFilename := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)

        dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        // double the size of the file (write zeroes into the file)
        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes

        // can just write a small buffer to the location where we want
        // and will resize for us
        buf := make([]byte, types.POINTER_SIZE)
        _, err = dbFile.WriteAt(buf, sizeOfDbFile*2 - types.POINTER_SIZE)
        check(err)

        dbFile.Close()
    }

    fmt.Printf("Resized all of the disks\n")
}

/*
    dbdisklocations = the disks that the file is spread out across
    padding file = second to last, parity file = last

    TODO: make this use the config object
*/
func AddFileSpecsToDatabase(filename string, username string, diskLocations []string,
                            configs *types.Config) {
    if !pathExists(configs.Dbdisks[0] + "/" + username + "_0") { // check if at least one disk exists
        // InitializeDatabaseStructure(LOCALHOST, nil)
        CreateDatabaseForUser(username, configs)
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

    dbFilenames := getDbFilenames(username, filename, configs)
    dbFilename := dbFilenames[0] //getDbFilenameForFile(filename, username)
    dbParityFilename := getParityFilename(username, filename, configs)

    /*
        Begin transaction
    */
    t := transaction.New(dbFilenames, dbParityFilename)

    // read in the database file and get root of the tree
    dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
    check(err)
    fileStat, err := dbFile.Stat(); check(err);
    sizeOfDbFile := fileStat.Size(); // in bytes

    header := getHeader(dbFile)
    oldHeader := header

    /*
        Tree entry: 
        [256 bytes for file name] [pointer to left child] 
        [pointer to right child] [list of disks, each 1H28 bytes]
    */

    // entry we want to insert
    targetNode := make([]byte, types.SIZE_OF_ENTRY)
    for i := 0; i < len(filename); i++ {
        targetNode[i] = filename[i]
    }
    // copy in the file locations
    for i := 0; i < len(diskLocations); i++ {
        diskName := diskLocations[i]
        for j := 0; j < len(diskName); j++ {
            offset := int(header.FileNameSize) + 2 * types.POINTER_SIZE + i * int(header.DiskNameSize)
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
    entryBuf := make([]byte, types.SIZE_OF_ENTRY)
    for !foundInsertionPoint {
        // read in the current node
        dbFile.ReadAt(entryBuf, currentNodeLocation)
        currentNode := bufferToEntry(entryBuf, &header, configs)

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
            entryBuf = make([]byte, types.SIZE_OF_ENTRY)
        }
    }

    /*
        Actually insert the node into the tree
    */

    // resize the files if this insertion will increase the size of this
    // database file
    // TODO: can technically just only resize this disk, and make it
    // just another transaction action, and wehn you have to recover,
    // you just check if the parity disk is the same size as the largest
    // disk, if not then extend it after you replay the log
    for (header.TrueDbSize + int64(types.SIZE_OF_ENTRY)) > sizeOfDbFile {
        // TODO: not sure if need to add resizing to transaction
        resizeAllDbDisks(username, configs)

        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile = fileStat.Size(); // in bytes
    }

    /*
        Make the parent node point to this new entry
    */
    binaryBuffer := new(bytes.Buffer)
    err = binary.Write(binaryBuffer, binary.LittleEndian, &header.FreeList)
    check(err)

    offsetToPointer := int(header.FileNameSize)
    if !left { // determine which pointer to set it as based on loop
        offsetToPointer += types.POINTER_SIZE
    }

    newData := make([]byte, types.SIZE_OF_ENTRY)
    for i := 0; i < len(entryBuf); i++ {
        newData[i] = entryBuf[i]
    }
    newParentLink := binaryBuffer.Bytes()
    for i := 0; i < len(newParentLink); i++ {
        newData[offsetToPointer + i] = newParentLink[i]
    }

    errCode := transaction.AddAction(t, entryBuf, newData, currentNodeLocation)
    transaction.HandleActionError(errCode)

    // update the true size of the database (we are going to enter a new entry)
    header.TrueDbSize += int64(types.SIZE_OF_ENTRY)

    // update free list to point to next entry in it
    // pointer to next in free list = first 8 bytes in the
    // entry in free list, if all 0s, then end of free list
    insertionPointBuf := make([]byte, types.SIZE_OF_ENTRY) //types.POINTER_SIZE
    _, err = dbFile.ReadAt(insertionPointBuf, header.FreeList)
    check(err)

    // check(err)
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
    errCode = transaction.AddAction(t, insertionPointBuf, targetNode, insertionPoint)
    transaction.HandleActionError(errCode)

    // push any updates to header
    binaryBuffer = new(bytes.Buffer)
    err = binary.Write(binaryBuffer, binary.LittleEndian, &header)
    check(err)

    newHeaderBuf := binaryBuffer.Bytes() // get new header

    binaryBuffer = new(bytes.Buffer)
    err = binary.Write(binaryBuffer, binary.LittleEndian, &oldHeader); check(err)
    oldHeaderBuf := binaryBuffer.Bytes()
    errCode = transaction.AddAction(t, oldHeaderBuf, newHeaderBuf, 0)
    transaction.HandleActionError(errCode)


    dbFile.Close()
    transaction.Commit(t)

    fmt.Printf("Successfully added filename: %s to the database\n", filename)
}


// here, storageType is in reference to where the database is stored
func GetFileEntry(filename string, username string, configs *types.Config) (*types.TreeEntry) {
    if !pathExists(configs.Dbdisks[0] + "/" + username + "_0") {
        return nil
    }

    dbFilename := getDbFilenameForFile(filename, username, configs)

    // read in the database file and get root of the tree
    dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
    check(err)

    header := getHeader(dbFile)

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
    var currentNode *types.TreeEntry = nil
    for !foundFileOrLeaf {
        // read in the current node
        buf := make([]byte, types.SIZE_OF_ENTRY)
        dbFile.ReadAt(buf, currentNodeLocation)

        currentNode = bufferToEntry(buf, &header, configs)

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
        } else { // go right if >
            if currentNode.Right == 0 {
                foundFileOrLeaf = true
            } else { // traverse down to right one
                currentNodeLocation = currentNode.Right
            }
        }
    }

    dbFile.Close()

    if foundFile {
        fmt.Printf("Successfully got the file\n")
        return currentNode
    } else {
        return nil
    }
}

/*
    Fix the tree first, and then add that spot into the free list, return
    errorcode (0 = success, 1 = did not find file)
*/
func DeleteFileEntry(filename string, username string, configs *types.Config) (int) {
    if !pathExists(configs.Dbdisks[0] + "/" + username + "_0") {
        return 1
    }

    dbFilenames := getDbFilenames(username, filename, configs)
    dbFilename := dbFilenames[0] //getDbFilenameForFile(filename, username)
    dbParityFilename := getParityFilename(username, filename, configs)

    /*
        Begin transaction
    */
    t := transaction.New(dbFilenames, dbParityFilename)

    // read in the database file and get root of the tree
    dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
    check(err)

    header := getHeader(dbFile)
    oldHeader := header

    // Start at root
    currentNodeLocation := header.RootPointer
    // currentNode := types.TreeEntry{"", 0, 0, nil}
    var currentNode *types.TreeEntry = nil
    var parentNodeLocation int64 = 0

    foundFileOrLeaf := false
    foundFile := false
    rightChild := false
    currentNodeBuf := make([]byte, types.SIZE_OF_ENTRY)
    var parentNodeBuf []byte = nil
    for !foundFileOrLeaf {
        // read in the current node
        _, err = dbFile.ReadAt(currentNodeBuf, currentNodeLocation)
        check(err)

        currentNode = bufferToEntry(currentNodeBuf, &header, configs)

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
            currentNodeBuf = make([]byte, types.SIZE_OF_ENTRY)
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
        Update the free list pointer: prepend this space to the list
    */

    newEntry := make([]byte, types.SIZE_OF_ENTRY)
    p := new(bytes.Buffer)
    err = binary.Write(p, binary.LittleEndian, &header.FreeList)
    check(err)
    freeListPointer := p.Bytes()
    for i := 0; i < len(freeListPointer); i++ {
        newEntry[i] = freeListPointer[i]
    }

    oldEntry := make([]byte, types.SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(oldEntry, currentNodeLocation)
    check(err)

    errCode := transaction.AddAction(t, oldEntry, newEntry, currentNodeLocation)
    transaction.HandleActionError(errCode)

    // update free list to point here now, since freed up memory
    header.FreeList = currentNodeLocation

    // fix the tree
    // https://en.wikipedia.org/wiki/Binary_search_tree#Deletion

    offsetInParent := header.FileNameSize
    if rightChild {
        offsetInParent += types.POINTER_SIZE
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
        buf := make([]byte, types.POINTER_SIZE)
        errCode = transaction.AddAction(t, parentNodeBuf[offsetInParent:offsetInParent + types.POINTER_SIZE], 
                                        buf, parentNodeLocation + int64(offsetInParent))
        transaction.HandleActionError(errCode)

    } else if currentNode.Left == 0 || currentNode.Right == 0 {
        childLocation := currentNode.Left
        if currentNode.Left == 0 {
            childLocation = currentNode.Right
        }

        buf := new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &childLocation)
        check(err)

        newPointer := buf.Bytes()
        errCode = transaction.AddAction(t, parentNodeBuf[offsetInParent:offsetInParent + types.POINTER_SIZE], 
                                        newPointer, parentNodeLocation + int64(offsetInParent))
        transaction.HandleActionError(errCode)
        
    } else {
        // find leftmost node in right subtree
        var candidateNodeLocation int64 = currentNode.Right
        var candidateParentLocation int64 = currentNodeLocation
        var candidateNode *types.TreeEntry = nil // &types.TreeEntry{"", 0, 0, []string(nil)}
        var foundLeftMost bool = false
        candidateBuf := make([]byte, types.SIZE_OF_ENTRY)
        candidateParentBuf := currentNodeBuf
        for !foundLeftMost {
            // read in the current node
            buf := make([]byte, types.SIZE_OF_ENTRY)
            _, err = dbFile.ReadAt(buf, candidateNodeLocation)
            check(err)

            candidateNode = bufferToEntry(buf, &header, configs)
            
            // if doesn't have a left child, then it is the leftmost
            if candidateNode.Left == 0 {
                foundLeftMost = true
            } else {
                candidateParentLocation = candidateNodeLocation
                candidateNodeLocation = candidateNode.Left
            }

            if !foundLeftMost {
                candidateParentBuf = candidateBuf
                candidateBuf = make([]byte, types.SIZE_OF_ENTRY)
            }
        }

        // overwrite original node that is being deleted with candidate node (link its parent to this new node)
        buf := new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &candidateNodeLocation)
        check(err)
        newPointer := buf.Bytes()
        errCode = transaction.AddAction(t, parentNodeBuf[offsetInParent:offsetInParent + types.POINTER_SIZE], 
                                        newPointer, parentNodeLocation + int64(offsetInParent))
        transaction.HandleActionError(errCode)

        // have the candidate node inherit the original node's links (left link now)
        buf = new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &currentNode.Left)
        check(err)

        newPointer = buf.Bytes()
        errCode = transaction.AddAction(t, candidateBuf[int(header.FileNameSize):int(header.FileNameSize) + types.POINTER_SIZE], 
                                        newPointer, candidateNodeLocation + int64(header.FileNameSize))
        transaction.HandleActionError(errCode)

        // have the candidate node inherit the original node's links (right link now)
        // HOWEVER, if candidate node is the immediate right child of currentNode,
        // we don't need to do this, since it's link to its child will already be correct
        // we will mess it up if we do this (TODO: can make this cleaner later)
        if (candidateParentLocation != currentNodeLocation) {
            buf = new(bytes.Buffer)
            err = binary.Write(buf, binary.LittleEndian, &currentNode.Right)
            check(err)
            newPointer = buf.Bytes()
            errCode = transaction.AddAction(t, candidateBuf[int(header.FileNameSize) + types.POINTER_SIZE:int(header.FileNameSize) + 2*types.POINTER_SIZE], 
                                        newPointer, candidateNodeLocation + int64(header.FileNameSize) + int64(types.POINTER_SIZE))
            transaction.HandleActionError(errCode)
        }

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

            // since it will now be the left child of the parent of candidate node
            // ^ not true always (candidate node could be immediate to the right of currentNode)
            // need to check if the parent node of candidate node is still currentNode
            offsetInCandPar := header.FileNameSize

            newPointer := buf.Bytes()
            errCode = transaction.AddAction(t, candidateParentBuf[offsetInCandPar:offsetInCandPar + types.POINTER_SIZE], 
                                        newPointer, candidateParentLocation + int64(offsetInCandPar))
            transaction.HandleActionError(errCode)

        } else if candidateNode.Right == 0 && (candidateParentLocation != currentNodeLocation) { // else, just overwrite that link with 0
            newPointer := make([]byte, types.POINTER_SIZE)
            offsetInCandPar := header.FileNameSize

            // since it will now be the left child of the parent of candidate node
            // ^ not true always (candidate node could be immediate to the right of currentNode)
            // need to check if the parent node of candidate node is still currentNode
            errCode = transaction.AddAction(t, candidateParentBuf[offsetInCandPar:offsetInCandPar + types.POINTER_SIZE], 
                                        newPointer, candidateParentLocation + int64(offsetInCandPar))
            transaction.HandleActionError(errCode)

        } // don't need to do anything in other case, because link from candidate to its child is already correct
    }

    // update the true size of the database (we removed an entry, so freed
    // up some space)
    header.TrueDbSize -= int64(types.SIZE_OF_ENTRY)

    // rewrite the header
    binaryBuffer := new(bytes.Buffer)
    err = binary.Write(binaryBuffer, binary.LittleEndian, &header)
    check(err)

    newHeaderBuf := binaryBuffer.Bytes()

    x := new(bytes.Buffer)
    err = binary.Write(x, binary.LittleEndian, &oldHeader)
    check(err)
    oldHeaderBuf := x.Bytes()

    errCode = transaction.AddAction(t, oldHeaderBuf, newHeaderBuf, 0)
    transaction.HandleActionError(errCode)

    // update the parity file to reflect the changes to both the header and
    // the entry
    // need the old data and the new data: do Parity XOR old data XOR new data

    // only edited the parent node and this entered node location, as well as
    // the header (since added to true size of db), and also the part of the
    // free list that we modified

    // FOR SOME REASON, THE BELOW CODE BREAKS THINGS...
    // fileStat, err := dbParityFile.Stat(); check(err);
    // sizeOfDbFile := fileStat.Size(); // in bytes
    // fmt.Printf("size later: %d\n", sizeOfDbFile)
    // randomBuf := make([]byte, 1)
    // _, err = dbParityFile.ReadAt(randomBuf, 2952)
    // check(err)
    // fmt.Printf("After after: %x\n", randomBuf[0])

    dbFile.Close()

    transaction.Commit(t)

    fmt.Printf("Successfully deleted node with filename %s\n", currentNode.Filename)

    return 0 // success
}

func printTree(entry *types.TreeEntry, header *Header, dbFile *os.File, arr []string, level int) {
    if entry != nil {
        // fmt.Printf("%s\n", entry.Filename)
        arr[level] += entry.Filename + " "
        if entry.Left != 0 {
            entryBuf := make([]byte, types.SIZE_OF_ENTRY)
            _, err := dbFile.ReadAt(entryBuf, entry.Left)
            check(err)
            child := bufferToEntry(entryBuf, header, configs)
        
            printTree(child, header, dbFile, arr, level + 1)
        } else {
            printTree(nil, nil, nil, arr, level + 1)
        }
        if entry.Right != 0 {
            entryBuf := make([]byte, types.SIZE_OF_ENTRY)
            _, err := dbFile.ReadAt(entryBuf, entry.Right)
            check(err)
            child := bufferToEntry(entryBuf, header, configs)

            printTree(child, header, dbFile, arr, level + 1)
        } else {
            printTree(nil, nil, nil, arr, level + 1)
        }

        // fmt.Printf("\n")
    } else {
        // if level >= len(arr) {
        //     fmt.Printf("Level is really large: %d\n", level)
        // }
        arr[level] += "X "
    }
}

func PrettyPrintTree(username string, depth int, configs *types.Config) {
    for i := 0; i < configs.DataDiskCount; i++ {
        fmt.Printf("Pretty printing tree for disk %d:\n\n", i)
        // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
        dbFileName := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)
        dbFile, err := os.Open(dbFileName)
        check(err)

        headerBuf := make([]byte, types.HEADER_SIZE)
        _, err = dbFile.ReadAt(headerBuf, 0)
        check(err)

        header := Header{0, 0, 0, 0, 0, 0}
        b := bytes.NewReader(headerBuf)
        err = binary.Read(b, binary.LittleEndian, &header)
        check(err)

        entryBuf := make([]byte, types.SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(entryBuf, header.RootPointer)
        check(err)

        entry := bufferToEntry(entryBuf, &header, configs)
        
        arr := make([]string, depth)
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

func PrettyPrintTreeGetString(username string, disk int, depth int, configs *types.Config) ([]string) {
    // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
    dbFileName := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[disk], username, disk)
    dbFile, err := os.Open(dbFileName)
    check(err)

    headerBuf := make([]byte, types.HEADER_SIZE)
    _, err = dbFile.ReadAt(headerBuf, 0)
    check(err)

    header := Header{0, 0, 0, 0, 0, 0}
    b := bytes.NewReader(headerBuf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    entryBuf := make([]byte, types.SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(entryBuf, header.RootPointer)
    check(err)

    entry := bufferToEntry(entryBuf, &header, configs)
    
    arr := make([]string, depth)
    for i := 0; i < len(arr); i++ {
        arr[i] = ""
    }

    printTree(entry, &header, dbFile, arr, 0)


    dbFile.Close()

    return arr
}