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
    "crypto/md5"
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
    currentNode := types.TreeEntry{string(currentFilename), 0, 0, []string(nil), nil}

    b := bytes.NewReader(buf[header.FileNameSize: header.FileNameSize + types.POINTER_SIZE])
    err := binary.Read(b, binary.LittleEndian, &currentNode.Left); check(err)
    b = bytes.NewReader(buf[header.FileNameSize + types.POINTER_SIZE: header.FileNameSize + 2 * types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &currentNode.Right); check(err)

    // header disk count is inherited from configs file, and is more accurate to
    // this file specifically
    currentNode.Disks = make([]string, header.DiskCount + 1) // + 1 for parity disk***
    i := 0
    for i = 0; i < int(header.DiskCount) + 1; i++ { // + 1 for parity disk***
        upperBound := int(header.FileNameSize) + 2 * int(types.POINTER_SIZE) + (i + 1) * int(header.DiskNameSize)
        lowerBound := int(header.FileNameSize) + 2 * int(types.POINTER_SIZE) + i * int(header.DiskNameSize)
        currentNode.Disks[i] = string(bytes.Trim(buf[lowerBound:upperBound], "\x00"))
        if currentNode.Disks[i] == "" {
            break
        }
    }

    // trim the slice from empty entries (didn't use all of the distribution disk count)
    if i != int(header.DiskCount) {
        currentNode.Disks = currentNode.Disks[0:i]
    }

    // get the hash at the end, and verify it, return nil if something went wrong
    originalHash := buf[len(buf) - types.MD5_SIZE:len(buf)]
    h := md5.New()
    h.Write(buf[0:len(buf) - types.MD5_SIZE])
    computedHash := h.Sum(nil)
    currentNode.Hash = make([]byte, types.MD5_SIZE)
    for i := 0; i < len(originalHash); i++ {
        currentNode.Hash[i] = originalHash[i]
        if computedHash[i] != originalHash[i] {
            // error in hash recomputation!, this disk is messed up
            // fmt.Printf("Found an error in a hash, original = %d, computed = %d\n", originalHash[i], computedHash[i])s
            return nil
        }
    }

    return &currentNode
}

// returns the newly modified entry, with updated hash
func modifyEntry(oldEntry []byte, newData []byte, locationOfChange int) []byte {
    newEntry := make([]byte, len(oldEntry))
    for i := 0; i < len(oldEntry); i++ {
        newEntry[i] = oldEntry[i]
    }
    for i := 0; i < len(newData); i++ {
        newEntry[locationOfChange + i] = newData[i]
    }

    // compute the hash for this modified parent entry
    h := md5.New()
    h.Write(newEntry[0:len(oldEntry) - types.MD5_SIZE])
    newHash := h.Sum(nil)
    for i := 0; i < types.MD5_SIZE; i++ {
        newEntry[len(newEntry) - types.MD5_SIZE + i] = newHash[i]
    }

    return newEntry
}

func verifyFreeListEntry(freeListEntryBuf []byte) []byte {
    // compute the hash for this modified parent entry
    h := md5.New()
    h.Write(freeListEntryBuf[0:len(freeListEntryBuf) - types.MD5_SIZE])
    newHash := h.Sum(nil)
    oldHash := freeListEntryBuf[len(freeListEntryBuf) - types.MD5_SIZE: len(freeListEntryBuf)]
    for i := 0; i < types.MD5_SIZE; i++ {
        if newHash[i] != oldHash[i] {
            fmt.Printf("Found an error in free list entry\n")
            return nil
        }
    }

    return freeListEntryBuf
}

// get the header in this dbFile
func getHeader(dbFile *os.File) (Header, int) {
    buf := make([]byte, types.HEADER_SIZE)
    _, err := dbFile.ReadAt(buf, 0)
    check(err)

    var header Header
    b := bytes.NewReader(buf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    // check the hash on the header here, recover if not correct
    headerHash := md5.New()
    sizeOfRawHeader := binary.Size(header)
    headerHash.Write(buf[0:sizeOfRawHeader])
    computedHeaderHash := headerHash.Sum(nil)

    originalHash := buf[sizeOfRawHeader:sizeOfRawHeader + types.MD5_SIZE]
    for i := 0; i < types.MD5_SIZE; i++ {
        if originalHash[i] != computedHeaderHash[i] {
            return header, -1
            // log.Fatal("Error in header")
            // fmt.Printf("Error in header\n")
        }
    }

    return header, 0
}

// get the database that this file is stored in
func getDbFilenameForFile(filename string, username string, configs *types.Config) string {
    var dbFilename string = ""
    dbDiskCount := len(configs.Dbdisks) - 1
    if dbDiskCount == 3 { // regular RAID 4
        if filename[0] >= 0 && filename[0] <= 85 {
            dbFilename = fmt.Sprintf("%s/%s_0", configs.Dbdisks[0], username)
        } else if filename[0] >= 86 && filename[0] <= 112 {
            dbFilename = fmt.Sprintf("%s/%s_1", configs.Dbdisks[1], username)
        } else {
            dbFilename = fmt.Sprintf("%s/%s_2", configs.Dbdisks[2], username)
        }
    } else {
        evenSplit := types.ASCII / dbDiskCount // ASCII
        disk := int(filename[0]) / evenSplit // might break if not ASCII
        if disk < 0 || disk >= dbDiskCount {
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
    var dbFilenames []string = make([]string, len(configs.Dbdisks) - 1)

    rootFilename := getDbFilenameForFile(filename, username, configs)
    dbFilenames[0] = rootFilename
    count := 1
    for i := 0; i < len(configs.Dbdisks) - 1; i++ {
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
    for i := 0; i < len(configs.Dbdisks) - 1; i++ {
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
    var SIZE_OF_ENTRY int16 = types.MAX_FILE_NAME_SIZE + 2*(types.POINTER_SIZE) + int16(configs.DataDiskCount + 1) * int16(types.MAX_DISK_NAME_SIZE) + types.MD5_SIZE
    parityBuf := make([]byte, types.HEADER_SIZE + int64(SIZE_OF_ENTRY))
    for i := 0; i < len(configs.Dbdisks) - 1; i++ { //- NUM_PARITY_DISKS
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

        // used to be MAX_DISK_COUNT, now takes the value from configs, and then
        // is stored in the header for future use
        h := Header{types.MAX_FILE_NAME_SIZE, uint8(configs.DataDiskCount), types.MAX_DISK_NAME_SIZE, 
                    types.HEADER_SIZE, types.HEADER_SIZE + int64(SIZE_OF_ENTRY),
                    types.HEADER_SIZE + int64(SIZE_OF_ENTRY)}

        buf := new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &h)
        check(err)

        header := buf.Bytes()

        // TODO: add hash on the header
        headerHash := md5.New()
        headerHash.Write(header)
        computedHeaderHash := headerHash.Sum(nil)

        // append it to the end of the raw header
        header = append(header, computedHeaderHash...)

        // zero bytes
        zeroes := make([]byte, types.HEADER_SIZE - int64(len(header)))
        header = append(header, zeroes...)

        // write header to database file
        _, err = dbFile.WriteAt(header, 0)
        check(err)

        for j := 0; j < len(header); j++ {
            parityBuf[j] ^= header[j]
        }

        // put in the the hash for the root node: hash of all zeroes of length entrysize - md5_size
        hash := md5.New()
        root := make([]byte, SIZE_OF_ENTRY - types.MD5_SIZE)
        hash.Write(root)
        rootHash := hash.Sum(nil)
        _, err = dbFile.WriteAt(rootHash, types.HEADER_SIZE + int64(SIZE_OF_ENTRY) - types.MD5_SIZE)
        check(err)

        for j := 0; j < len(rootHash); j++ {
            parityBuf[types.HEADER_SIZE + int64(SIZE_OF_ENTRY) - types.MD5_SIZE + int64(j)] ^= rootHash[j]
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
    for i := 0; i < len(configs.Dbdisks) - 1; i++ {
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
    TODO: should run a daemon goroutine that wakes up every day at night to
    check if the parity disk is an accurate reflection of the XOR of the databases

    If not, then manually fix that drive by recomputing it

    TODO: maybe need to add the recovery as part of the transaction log (can do this later)

    TODO: might also need to run fsck here, probably a good idea if failure detected,
    but it is system dependent I guess

*/
func recoverFromDbDiskFailure(dbFilename string, nodeLocation int64, username string, 
                            configs *types.Config) {
    fmt.Printf("Detected an error in drive: %s, location: %d\n", dbFilename, nodeLocation)

    dataDiskCount := len(configs.Dbdisks) - configs.ParityDiskCount

    /*
        Delete the offending file, and recreate it with the correct data
    */
    os.Remove(dbFilename)
    fixedFile, err := os.OpenFile(dbFilename, os.O_RDWR | os.O_CREATE, 0755)
    check(err)

    // read all of the other disks besides this one, and XOR with the parity
    // disk bit by bit and reconstruct the file
    // NOTE: this can be done much more efficiently by issuing more IO requests
    // and using a similar approach as the original saving of the file, but
    // when recovering the file, performance isn't as big of an issue because
    // of the rarity of the occasion (temporary implementation)

    otherDriveFiles := make([]*os.File, dataDiskCount - 1)
    parityDriveFileName := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username) 

    parityDriveFile, err := os.Open(parityDriveFileName)
    check(err)
    
    count := 0
    for i := 0; i < dataDiskCount; i++ {
        tmpName := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)
        if tmpName != dbFilename {
            otherDriveFiles[count], err = os.Open(tmpName); check(err)
            count++
        }
    }

    fileStat, err := parityDriveFile.Stat(); check(err);
    size := fileStat.Size(); // in bytes
    // fmt.Printf("\n\n\n\nSize: %d\n\n\n\n", size)

    trueParityStrip := make([]byte, types.MAX_BUFFER_SIZE)
    buf := make([]byte, types.MAX_BUFFER_SIZE)

    var currentLocation int64 = 0
    for currentLocation != size {
        // check if need to resize the buffers
        if (size - currentLocation) < int64(types.MAX_BUFFER_SIZE) {
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
        _, err = fixedFile.WriteAt(trueParityStrip, currentLocation)
        check(err)

        // update location
        currentLocation += int64(len(trueParityStrip))
    }

    for i := 0; i < len(otherDriveFiles); i++ {
        otherDriveFiles[i].Close()
    }
    parityDriveFile.Close()
    fixedFile.Close()
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
    t := transaction.New(dbFilenames, dbParityFilename, configs)

    // read in the database file and get root of the tree
    dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
    check(err)
    fileStat, err := dbFile.Stat(); check(err);
    sizeOfDbFile := fileStat.Size(); // in bytes

    header, errCode := getHeader(dbFile)
    retries := 0
    for errCode != 0 && retries != types.RETRY_COUNT { // error in computed hash
        dbFile.Close()

        recoverFromDbDiskFailure(dbFilename, 0, username, configs)

        // reopen the database file
        dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        header, errCode = getHeader(dbFile)

        retries++
    }
    oldHeader := header

    var SIZE_OF_ENTRY int16 = header.FileNameSize + 2*(types.POINTER_SIZE) + int16(header.DiskCount + 1) * int16(header.DiskNameSize) + types.MD5_SIZE

    /*
        Tree entry: 
        [256 bytes for file name] [pointer to left child] 
        [pointer to right child] [list of disks, each 1H28 bytes]
    */

    // entry we want to insert
    targetNode := make([]byte, SIZE_OF_ENTRY)
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
    // write the hash into the end of the entry
    h := md5.New()
    h.Write(targetNode[0:SIZE_OF_ENTRY - types.MD5_SIZE])        
    targetNodeHash := h.Sum(nil)
    for i := int64(0); i < types.MD5_SIZE; i++ {
        targetNode[int64(SIZE_OF_ENTRY) - types.MD5_SIZE + i] = targetNodeHash[i]
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
        _, err = dbFile.ReadAt(entryBuf, currentNodeLocation)
        check(err)
        currentNode := bufferToEntry(entryBuf, &header, configs)

        /*
            Check if the currentNode had an error in reading (hash was
            incorrect) -> fix this disk and re-write this entry to the location
            where it was supposed to be
        */
        retries := 0
        for currentNode == nil && retries != types.RETRY_COUNT {
            dbFile.Close()

            recoverFromDbDiskFailure(dbFilename, currentNodeLocation, username, configs)

            // reopen the database file
            dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
            check(err)

            // get the currentNode again
            _, err = dbFile.ReadAt(entryBuf, currentNodeLocation)
            check(err)

            currentNode = bufferToEntry(entryBuf, &header, configs)

            retries++
        }

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

            // TODO: handle the case when saving the file again, just update
            // the current entry and that's it

            // copy in the actual entry now
            errCode = transaction.AddAction(t, entryBuf, targetNode, currentNodeLocation)
            transaction.HandleActionError(errCode)

            dbFile.Close()
            transaction.Commit(t)

            fmt.Printf("Updated entry for %s\n", filename)

            return

            // foundInsertionPoint = true

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
    // TODO: can technically just only resize this disk, and make it
    // just another transaction action, and wehn you have to recover,
    // you just check if the parity disk is the same size as the largest
    // disk, if not then extend it after you replay the log
    for (header.TrueDbSize + int64(SIZE_OF_ENTRY)) > sizeOfDbFile {
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

    newParentLink := binaryBuffer.Bytes()
    newEntry := modifyEntry(entryBuf, newParentLink, offsetToPointer)

    errCode = transaction.AddAction(t, entryBuf, newEntry, currentNodeLocation)
    transaction.HandleActionError(errCode)

    // update the true size of the database (we are going to enter a new entry)
    header.TrueDbSize += int64(SIZE_OF_ENTRY)

    // update free list to point to next entry in it
    // pointer to next in free list = first 8 bytes in the
    // entry in free list, if all 0s, then end of free list
    insertionPointBuf := make([]byte, SIZE_OF_ENTRY) //types.POINTER_SIZE
    _, err = dbFile.ReadAt(insertionPointBuf, header.FreeList)
    check(err)

    // don't verify the pointer if it is to the end of the file (no entry
    // there to check)
    if header.FreeList != (header.TrueDbSize - int64(SIZE_OF_ENTRY)) {
        freeListEntry := verifyFreeListEntry(insertionPointBuf)
        retries := 0
        for freeListEntry == nil && retries != types.RETRY_COUNT {
            dbFile.Close()

            recoverFromDbDiskFailure(dbFilename, header.FreeList, username, configs)

            // reopen the database file
            dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
            check(err)

            // get the currentNode again
            insertionPointBuf := make([]byte, SIZE_OF_ENTRY)
            _, err = dbFile.ReadAt(insertionPointBuf, header.FreeList)
            check(err)

            freeListEntry = verifyFreeListEntry(insertionPointBuf)

            retries++
        }
    }

    // check(err)
    var pointer int64 = 0
    bufferReader := bytes.NewReader(insertionPointBuf[0:types.POINTER_SIZE])
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

    // add the new hash of the header** TODO

    newHeaderBuf := binaryBuffer.Bytes() // get new header

    binaryBuffer = new(bytes.Buffer)
    err = binary.Write(binaryBuffer, binary.LittleEndian, &oldHeader); check(err)
    oldHeaderBuf := binaryBuffer.Bytes()
    errCode = transaction.AddAction(t, oldHeaderBuf, newHeaderBuf, 0)
    transaction.HandleActionError(errCode)

    // update the hash of the header
    oldHash := md5.New()
    oldHash.Write(oldHeaderBuf)
    computedOldHash := oldHash.Sum(nil)

    newHash := md5.New()
    newHash.Write(newHeaderBuf)
    computedNewHash := newHash.Sum(nil)

    errCode = transaction.AddAction(t, computedOldHash, computedNewHash, int64(len(newHeaderBuf)))
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

    header, errCode := getHeader(dbFile)
    retries := 0
    for errCode != 0 && retries != types.RETRY_COUNT { // error in computed hash
        dbFile.Close()

        recoverFromDbDiskFailure(dbFilename, 0, username, configs)

        // reopen the database file
        dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        header, errCode = getHeader(dbFile)

        retries++
    }
    var SIZE_OF_ENTRY int16 = header.FileNameSize + 2*(types.POINTER_SIZE) + int16(header.DiskCount + 1) * int16(header.DiskNameSize) + types.MD5_SIZE

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
        buf := make([]byte, SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(buf, currentNodeLocation)
        check(err)

        currentNode = bufferToEntry(buf, &header, configs)
        /*
            Check if the currentNode had an error in reading (hash was
            incorrect) -> fix this disk and re-write this entry to the location
            where it was supposed to be
        */
        retries := 0
        for currentNode == nil && retries != types.RETRY_COUNT {
            dbFile.Close() // close the file first, since recover will delete it**

            recoverFromDbDiskFailure(dbFilename, currentNodeLocation, username, configs)

            // reopen the database file
            dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
            check(err)

            // get the currentNode again
            buf := make([]byte, SIZE_OF_ENTRY)
            _, err = dbFile.ReadAt(buf, currentNodeLocation)
            check(err)

            currentNode = bufferToEntry(buf, &header, configs)

            retries++
        }

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
func DeleteFileEntry(filename string, username string, configs *types.Config) *types.TreeEntry {
    if !pathExists(configs.Dbdisks[0] + "/" + username + "_0") {
        return nil
    }

    dbFilenames := getDbFilenames(username, filename, configs)
    dbFilename := dbFilenames[0] //getDbFilenameForFile(filename, username)
    dbParityFilename := getParityFilename(username, filename, configs)

    /*
        Begin transaction
    */
    t := transaction.New(dbFilenames, dbParityFilename, configs)

    // read in the database file and get root of the tree
    dbFile, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
    check(err)

    header, errCode := getHeader(dbFile)
    retries := 0
    for errCode != 0 && retries != types.RETRY_COUNT { // error in computed hash
        dbFile.Close()

        recoverFromDbDiskFailure(dbFilename, 0, username, configs)

        // reopen the database file
        dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        // get the header again
        header, errCode = getHeader(dbFile)

        retries++
    }
    oldHeader := header

    // Start at root
    currentNodeLocation := header.RootPointer
    // currentNode := types.TreeEntry{"", 0, 0, nil}
    var currentNode *types.TreeEntry = nil
    var parentNodeLocation int64 = 0
    var SIZE_OF_ENTRY int16 = header.FileNameSize + 2*(types.POINTER_SIZE) + int16(header.DiskCount + 1) * int16(header.DiskNameSize) + types.MD5_SIZE

    foundFileOrLeaf := false
    foundFile := false
    rightChild := false
    currentNodeBuf := make([]byte, SIZE_OF_ENTRY)
    var parentNodeBuf []byte = nil
    for !foundFileOrLeaf {
        // read in the current node
        _, err = dbFile.ReadAt(currentNodeBuf, currentNodeLocation)
        check(err)

        currentNode = bufferToEntry(currentNodeBuf, &header, configs)
        /*
            Check if the currentNode had an error in reading (hash was
            incorrect) -> fix this disk and re-write this entry to the location
            where it was supposed to be
        */
        retries := 0
        for currentNode == nil && retries != types.RETRY_COUNT {
            dbFile.Close()

            recoverFromDbDiskFailure(dbFilename, currentNodeLocation, username, configs)

            // reopen the database file
            dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
            check(err)

            // get the currentNode again
            _, err = dbFile.ReadAt(currentNodeBuf, currentNodeLocation)
            check(err)

            currentNode = bufferToEntry(currentNodeBuf, &header, configs)
            retries++
        }

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
        fmt.Printf("Did not find the file %s\n", filename)
        dbFile.Close()
        return nil
    }
    
    /*
        Delete the file:
            Reclaim that memory by adding it to the free list
            Fix the tree by making nodes move up in the tree
        Update the free list pointer: prepend this space to the list
    */

    zeroBuf := make([]byte, SIZE_OF_ENTRY)
    p := new(bytes.Buffer)
    err = binary.Write(p, binary.LittleEndian, &header.FreeList)
    check(err)
    freeListPointer := p.Bytes()

    newEntry := modifyEntry(zeroBuf, freeListPointer, 0)
    oldEntryBuf := make([]byte, SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(oldEntryBuf, currentNodeLocation)
    check(err)

    // do this just for the hash check
    oldEntry := bufferToEntry(oldEntryBuf, &header, configs)
    retries = 0
    for oldEntry == nil && retries != types.RETRY_COUNT {
        dbFile.Close()

        recoverFromDbDiskFailure(dbFilename, currentNodeLocation, username, configs)

        // reopen the database file
        dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        // get the currentNode again
        oldEntryBuf = make([]byte, SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(oldEntryBuf, currentNodeLocation)
        check(err)

        oldEntry = bufferToEntry(oldEntryBuf, &header, configs)

        retries++
    }

    errCode = transaction.AddAction(t, oldEntryBuf, newEntry, currentNodeLocation)
    transaction.HandleActionError(errCode)

    // update free list to point here now, since freed up memory
    header.FreeList = currentNodeLocation

    // fix the tree
    // https://en.wikipedia.org/wiki/Binary_search_tree#Deletion

    offsetInParent := int(header.FileNameSize)
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

        newEntry := modifyEntry(parentNodeBuf, buf, offsetInParent)

        errCode = transaction.AddAction(t, parentNodeBuf, newEntry, parentNodeLocation)
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
        newEntry := modifyEntry(parentNodeBuf, newPointer, offsetInParent)

        errCode = transaction.AddAction(t, parentNodeBuf, newEntry, parentNodeLocation)
        transaction.HandleActionError(errCode)
        
    } else {
        // find leftmost node in right subtree
        var candidateNodeLocation int64 = currentNode.Right
        var candidateParentLocation int64 = currentNodeLocation
        var candidateNode *types.TreeEntry = nil // &types.TreeEntry{"", 0, 0, []string(nil)}
        var foundLeftMost bool = false
        candidateBuf := make([]byte, SIZE_OF_ENTRY)
        candidateParentBuf := currentNodeBuf
        for !foundLeftMost {
            // read in the current node
            candidateBuf = make([]byte, SIZE_OF_ENTRY)
            _, err = dbFile.ReadAt(candidateBuf, candidateNodeLocation)
            check(err)

            candidateNode = bufferToEntry(candidateBuf, &header, configs)

            retries := 0
            for candidateNode == nil && retries != types.RETRY_COUNT {
                dbFile.Close()

                recoverFromDbDiskFailure(dbFilename, candidateNodeLocation, username, configs)

                // reopen the database file
                dbFile, err = os.OpenFile(dbFilename, os.O_RDWR, 0755)
                check(err)
                
                // get the currentNode again
                candidateBuf = make([]byte, SIZE_OF_ENTRY)
                _, err = dbFile.ReadAt(candidateBuf, currentNodeLocation)
                check(err)

                candidateNode = bufferToEntry(candidateBuf, &header, configs)

                retries++
            }
            
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

        /*
            overwrite original node that is being deleted with candidate node (link its parent to this new node)
        */
        buf := new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &candidateNodeLocation)
        check(err)
        newPointer := buf.Bytes()
        newEntry := modifyEntry(parentNodeBuf, newPointer, offsetInParent)

        errCode = transaction.AddAction(t, parentNodeBuf, newEntry, parentNodeLocation)
        transaction.HandleActionError(errCode)

        /*
            have the candidate node inherit the original node's links (left link now)
        */
        buf = new(bytes.Buffer)
        err = binary.Write(buf, binary.LittleEndian, &currentNode.Left)
        check(err)

        newPointer = buf.Bytes()
        newEntry = modifyEntry(candidateBuf, newPointer, int(header.FileNameSize))

        // errCode = transaction.AddAction(t, candidateBuf, newEntry, candidateNodeLocation)
        // transaction.HandleActionError(errCode)

        // have the candidate node inherit the original node's links (right link now)
        // HOWEVER, if candidate node is the immediate right child of currentNode,
        // we don't need to do this, since it's link to its child will already be correct
        // we will mess it up if we do this (TODO: can make this cleaner later)
        if (candidateParentLocation != currentNodeLocation) {
            buf = new(bytes.Buffer)
            err = binary.Write(buf, binary.LittleEndian, &currentNode.Right)
            check(err)
            newPointer = buf.Bytes()

            temp := modifyEntry(newEntry, newPointer, int(header.FileNameSize) + types.POINTER_SIZE)
            newEntry = temp

            // errCode = transaction.AddAction(t, candidateBuf, newEntry, candidateNodeLocation)
            // transaction.HandleActionError(errCode)
        }

        errCode = transaction.AddAction(t, candidateBuf, newEntry, candidateNodeLocation)
        transaction.HandleActionError(errCode)

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
            offsetInCandPar := int(header.FileNameSize)

            newPointer := buf.Bytes()
            newEntry := modifyEntry(candidateParentBuf, newPointer, offsetInCandPar)

            errCode = transaction.AddAction(t, candidateParentBuf, newEntry, candidateParentLocation)
            transaction.HandleActionError(errCode)

        } else if candidateNode.Right == 0 && (candidateParentLocation != currentNodeLocation) { // else, just overwrite that link with 0
            newPointer := make([]byte, types.POINTER_SIZE)
            offsetInCandPar := int(header.FileNameSize)
            newEntry := modifyEntry(candidateParentBuf, newPointer, offsetInCandPar)

            // since it will now be the left child of the parent of candidate node
            // ^ not true always (candidate node could be immediate to the right of currentNode)
            // need to check if the parent node of candidate node is still currentNode
            errCode = transaction.AddAction(t, candidateParentBuf, newEntry, candidateParentLocation)
            transaction.HandleActionError(errCode)

        } // don't need to do anything in other case, because link from candidate to its child is already correct
    }

    // update the true size of the database (we removed an entry, so freed
    // up some space)
    header.TrueDbSize -= int64(SIZE_OF_ENTRY)

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

    // update the hash of the header
    oldHash := md5.New()
    oldHash.Write(oldHeaderBuf)
    computedOldHash := oldHash.Sum(nil)

    newHash := md5.New()
    newHash.Write(newHeaderBuf)
    computedNewHash := newHash.Sum(nil)

    errCode = transaction.AddAction(t, computedOldHash, computedNewHash, int64(len(newHeaderBuf)))
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

    return currentNode // success
}

func printTree(entry *types.TreeEntry, header *Header, dbFile *os.File, arr []string, level int, configs *types.Config) {
    if entry != nil {
        var SIZE_OF_ENTRY int16 = header.FileNameSize + 2*(types.POINTER_SIZE) + int16(header.DiskCount + 1) * int16(header.DiskNameSize) + types.MD5_SIZE
        // fmt.Printf("%s\n", entry.Filename)
        arr[level] += entry.Filename + " "
        if entry.Left != 0 {
            entryBuf := make([]byte, SIZE_OF_ENTRY)
            _, err := dbFile.ReadAt(entryBuf, entry.Left)
            check(err)
            child := bufferToEntry(entryBuf, header, configs)
        
            printTree(child, header, dbFile, arr, level + 1, configs)
        } else {
            printTree(nil, nil, nil, arr, level + 1, configs)
        }
        if entry.Right != 0 {
            entryBuf := make([]byte, SIZE_OF_ENTRY)
            _, err := dbFile.ReadAt(entryBuf, entry.Right)
            check(err)
            child := bufferToEntry(entryBuf, header, configs)

            printTree(child, header, dbFile, arr, level + 1, configs)
        } else {
            printTree(nil, nil, nil, arr, level + 1, configs)
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
    for i := 0; i < len(configs.Dbdisks) - 1; i++ {
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
        var SIZE_OF_ENTRY int16 = header.FileNameSize + 2*(types.POINTER_SIZE) + int16(header.DiskCount + 1) * int16(header.DiskNameSize) + types.MD5_SIZE

        entryBuf := make([]byte, SIZE_OF_ENTRY)
        _, err = dbFile.ReadAt(entryBuf, header.RootPointer)
        check(err)

        entry := bufferToEntry(entryBuf, &header, configs)
        
        arr := make([]string, depth)
        for i := 0; i < len(arr); i++ {
            arr[i] = ""
        }

        printTree(entry, &header, dbFile, arr, 0, configs)
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
    var SIZE_OF_ENTRY int16 = header.FileNameSize + 2*(types.POINTER_SIZE) + int16(header.DiskCount + 1) * int16(header.DiskNameSize) + types.MD5_SIZE

    entryBuf := make([]byte, SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(entryBuf, header.RootPointer)
    check(err)

    entry := bufferToEntry(entryBuf, &header, configs)
    
    arr := make([]string, depth)
    for i := 0; i < len(arr); i++ {
        arr[i] = ""
    }

    printTree(entry, &header, dbFile, arr, 0, configs)

    dbFile.Close()

    return arr
}