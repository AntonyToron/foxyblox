/*******************************************************************************
* Author: Antony Toron
* File name: database_test.go
* Date created: 5/4/18
*
* Description: tests database for correctness
*******************************************************************************/

package database

import (
    "testing"
    "math/rand"
    "math"
    "os"
    "fmt"
    "bytes"
    "encoding/binary"
    // "os/exec"
    "time"
    "foxyblox/types"
)

const SMALL_FILE_SIZE int = 1024
const BUF_SIZE int = 1024
const VERY_SMALL_FILE_SIZE = 6 // currently 1, 3 aren't working perfectly
const REGULAR_FILE_SIZE int = 8192
const ROUNDS = 100
const TESTING_DISK_COUNT = 3

// 24
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(18))) //int64(math.Pow(2, float64(30))) // 1 GB
var configs *types.Config
var diskLocations []string

func TestMain(m *testing.M) {
    fmt.Println("Setting up for tests")

    rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time

    // initialize the configs for the system (level of RAID, database location, etc.)
    dbDisks := make([]string, TESTING_DISK_COUNT + 1)
    for i := 0; i < len(dbDisks); i++ {
        dbDisks[i] = fmt.Sprintf(types.LOCALHOST_DBDISK, i)
    }

    diskLocations = make([]string, TESTING_DISK_COUNT + 1)
    for i := 0; i < len(diskLocations); i++ {
        diskLocations[i] = fmt.Sprintf("./storage/drive%d", i)
    }

    configs = &types.Config{Sys: types.LOCALHOST, Dbdisks: dbDisks,
                       Datadisks: diskLocations,
                       DataDiskCount: TESTING_DISK_COUNT, 
                       ParityDiskCount: 1}

    retCode := m.Run()

    fmt.Println("Finished tests")

    os.Exit(retCode)
}

func removeDatabaseStructureAndCheck(t *testing.T) {
    RemoveDatabaseStructureLocal()

    if pathExists("./storage") {
        t.Errorf("Did not delete a folder desired")
    }
    
    for i := 0; i < TESTING_DISK_COUNT + 1; i++ {
        // diskFolder := fmt.Sprintf("./storage/drive%d", i + 1)
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if pathExists(dbdiskFolder) { // pathExists(diskFolder) ||
            t.Errorf("Did not delete a folder desired")
        }
    }

    // parityFolder := fmt.Sprintf("./storage/drivep")
    // dbParityFolder := fmt.Sprintf("./storage/dbdrivep")
    // if pathExists(dbParityFolder) { // pathExists(parityFolder) ||
    //     t.Errorf("Did not delete a folder desired")
    // }
}

func TestDatabaseInitializationAndRemoval(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    if !pathExists("./storage") {
        t.Errorf("Did not create a folder desired")
    }
    
    for i := 0; i < TESTING_DISK_COUNT + 1; i++ {
        // diskFolder := fmt.Sprintf("./storage/drive%d", i)
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if !pathExists(dbdiskFolder) { // !pathExists(diskFolder) ||
            t.Errorf("Did not create a folder desired")
        }
    }

    // // parityFolder := fmt.Sprintf("./storage/drivep")
    // dbParityFolder := fmt.Sprintf("./storage/dbdrivep")
    // if !pathExists(dbParityFolder) { // !pathExists(parityFolder) ||
    //     t.Errorf("Did not create a folder desired")
    // }

    removeDatabaseStructureAndCheck(t)
}

func TestDatabaseCreationAndRemoval(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"

    CreateDatabaseForUser(username, configs)

    for i := 0; i < TESTING_DISK_COUNT; i++ {
        dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
        if !pathExists(dbCompLocation) {
            t.Errorf("Did not create all of the database files")
            return
        }
    }

    dbParityFileName := fmt.Sprintf("./storage/dbdrive%d/%s_p", TESTING_DISK_COUNT, username)
    dbParityFile, err := os.Open(dbParityFileName); check(err)
    parityBuf := make([]byte, types.HEADER_SIZE)
    _, err = dbParityFile.ReadAt(parityBuf, 0)

    testBuf := make([]byte, types.HEADER_SIZE)

    // check that headers are correct, and that the parity disk is correct as well
    for i := 0; i < TESTING_DISK_COUNT; i++ {
        dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
        dbFile, err := os.Open(dbCompLocation)
        check(err)

        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes
        if sizeOfDbFile != types.HEADER_SIZE {
            t.Errorf("Incorrect database file size")
        }

        buf := make([]byte, types.HEADER_SIZE)
        _, err = dbFile.ReadAt(buf, 0)
        check(err)

        header := Header{0, 0, 0, 0, 0, 0}
        b := bytes.NewReader(buf)
        err = binary.Read(b, binary.LittleEndian, &header)
        check(err)

        if header.FileNameSize != types.MAX_FILE_NAME_SIZE || header.DiskCount != types.MAX_DISK_COUNT {
            t.Errorf("Part of the header is incorrect")
        }
        if header.DiskNameSize != types.MAX_DISK_NAME_SIZE || header.RootPointer != types.HEADER_SIZE {
            t.Errorf("Part of the header is incorrect")
        }
        if header.FreeList != types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY) || header.TrueDbSize != types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY) {
            t.Errorf("Part of the header is incorrect")
        }

        for j := 0; j < len(testBuf); j++ {
            testBuf[j] ^= buf[j]
        }

        dbFile.Close()
    }

    for i := 0; i < len(parityBuf); i++ {
        if parityBuf[i] != testBuf[i] {
            t.Errorf("Incorrect comparison for XOR at %d", i)
            break
        }
    }

    dbParityFile.Close()

    DeleteDatabaseForUser(username, configs)

    for i := 0; i < TESTING_DISK_COUNT; i++ {
        // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
        dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)

        if pathExists(dbCompLocation) {
            t.Errorf("Did not remove all the database files")
        }
    }

    dbParityFileName = fmt.Sprintf("./storage/dbdrive%d/%s_p", TESTING_DISK_COUNT, username)
    if pathExists(dbParityFileName) {
        t.Errorf("Did not remove all the database files")
    }

    removeDatabaseStructureAndCheck(t)
}

func addFileHelper(t *testing.T, filename string, username string, 
                shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
                addedSoFar int, driveAddedTo int) {
    AddFileSpecsToDatabase(filename, username, configs.Datadisks, configs)

    // check that the entry is in the correct spot, and that the header was
    // updated accordingly
    // should have gone into 2 (b/c it is >= 113, and t = 116)
    dbFilename := fmt.Sprintf("./storage/dbdrive%d/%s_%d", driveAddedTo, username, driveAddedTo)
    dbFile, err := os.Open(dbFilename)
    check(err)

    buf := make([]byte, types.SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(buf, shouldBeAddedAt)
    check(err)

    entryFilename := bytes.Trim(buf[0:types.MAX_FILE_NAME_SIZE], "\x00")
    entry := types.TreeEntry{string(entryFilename), 0, 0, []string(nil)}

    b := bytes.NewReader(buf[types.MAX_FILE_NAME_SIZE: types.MAX_FILE_NAME_SIZE + types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Left); check(err)
    b = bytes.NewReader(buf[types.MAX_FILE_NAME_SIZE + types.POINTER_SIZE: types.MAX_FILE_NAME_SIZE + 2 * types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Right); check(err)

    if entry.Filename != filename {
        t.Errorf("Filename is incorrect in entry, it is %s, should be %s", entry.Filename, filename)
        fmt.Printf("Filename is %s, should be %s\n", entry.Filename, filename)
        fmt.Printf("Length of filename is %d, should be %d\n", len(entry.Filename), len(filename))
    }

    entry.Disks = make([]string, types.MAX_DISK_COUNT)
    for i := 0; i < int(types.MAX_DISK_COUNT); i++ {
        upperBound := int(types.MAX_FILE_NAME_SIZE) + 2 * int(types.POINTER_SIZE) + (i + 1) * int(types.MAX_DISK_NAME_SIZE)
        lowerBound := int(types.MAX_FILE_NAME_SIZE) + 2 * int(types.POINTER_SIZE) + i * int(types.MAX_DISK_NAME_SIZE)
        entry.Disks[i] = string(bytes.Trim(buf[lowerBound:upperBound], "\x00"))
        // shouldBe := fmt.Sprintf("local_storage/drive%d", i + 1)
        shouldBe := configs.Datadisks[i]
        if entry.Disks[i] != shouldBe {
            t.Errorf("One of the disk locations is incorrect, it is %s", entry.Disks[i])
        }
    }

    if entry.Left != 0 && entry.Right != 0 {
        t.Errorf("The pointers for entry are not right")
    }

    // check the parent entry now (root, in this case)
    buf = make([]byte, types.SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(buf, parentShouldBeAt)
    check(err)

    entry = types.TreeEntry{string(buf[0:types.MAX_FILE_NAME_SIZE]), 0, 0, []string(nil)}

    b = bytes.NewReader(buf[types.MAX_FILE_NAME_SIZE: types.MAX_FILE_NAME_SIZE + types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Left); check(err)
    b = bytes.NewReader(buf[types.MAX_FILE_NAME_SIZE + types.POINTER_SIZE: types.MAX_FILE_NAME_SIZE + 2 * types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Right); check(err)

    if !shouldBeLeft && entry.Right != shouldBeAddedAt {
        t.Errorf("Link to entry from parent is incorrect")
        fmt.Printf("right = %d, should be added at = %d\n", entry.Right, shouldBeAddedAt)
    }
    if shouldBeLeft && entry.Left != shouldBeAddedAt {
        t.Errorf("Link to entry from parent is incorrect")
    }
    // if !shouldBeLeft && entry.Left != 0 {
    //     t.Errorf("Right link is not 0")
    // }
    // if shouldBeLeft && entry.Right != 0 {
    //     t.Errorf("Right link is not 0")
    // }

    // check the header now
    buf = make([]byte, types.HEADER_SIZE)
    _, err = dbFile.ReadAt(buf, 0)
    check(err)

    header := Header{0, 0, 0, 0, 0, 0}
    b = bytes.NewReader(buf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    // addedSoFar + 2 because include the root too
    if header.TrueDbSize != types.HEADER_SIZE + int64(addedSoFar + 2)*int64(types.SIZE_OF_ENTRY) {
        t.Errorf("Truedbsize did not update properly")
    }
    // change this to freeListShouldBe
    if header.FreeList != types.HEADER_SIZE + int64(addedSoFar + 2)*int64(types.SIZE_OF_ENTRY) {
        t.Errorf("Free list pointer in header did not update properly, it is %d", header.FreeList)
    }

    // check that the file is the right size


    // also check that the parity disk is correct
    dbParityFileName := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username)
    dbParityFile, err := os.Open(dbParityFileName); check(err)

    fileStat, err := dbParityFile.Stat(); check(err);
    sizeOfParityFile := fileStat.Size(); // in bytes

    parityBuf := make([]byte, sizeOfParityFile)
    _, err = dbParityFile.ReadAt(parityBuf, 0)
    check(err)

    testBuf := make([]byte, sizeOfParityFile)

    for i := 0; i < TESTING_DISK_COUNT; i++ {
        dbFilename := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)
        dbFile, err := os.Open(dbFilename)
        check(err)

        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes
        if sizeOfDbFile != sizeOfParityFile {
            t.Errorf("The parity and db files are different lengths")
            break
        }

        buf := make([]byte, sizeOfDbFile)
        _, err = dbFile.ReadAt(buf, 0)
        check(err)

        for j := 0; j < int(sizeOfDbFile); j++ {
            testBuf[j] ^= buf[j]
        }

        dbFile.Close()
    }

    for i := 0; i < int(sizeOfParityFile); i++ {
        if parityBuf[i] != testBuf[i] {
            t.Errorf("Incorrect XOR at location %d\n", i)
            break
        }
    }

    dbParityFile.Close()
    dbFile.Close()
}

func TestAddingAFile(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"
    filename := "testingFile.txt"

    CreateDatabaseForUser(username, configs)

    addFileHelper(t, filename, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 2)

    removeDatabaseStructureAndCheck(t)
    fmt.Println("finished testaddingfile")
}

func TestAddingMultipleFiles(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"
    filename1 := "testingFile.txt"
    filename2 := "1TestingFile.txt"
    filename3 := "fTestingFile.txt"
    filename1_1 := "testingFile2.txt"
    filename2_2 := "0TestingFile.txt"
    filename1_2 := "sestingFile.txt"
    filename1_3 := "sastingFile.txt"
    filename1_4 := "saatingFile.txt"
    filename1_5 := "sattingFile.txt"

    CreateDatabaseForUser(username, configs)
    // t *testing.T, filename string, username string, 
    //             shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
    //             addedSoFar int, driveAddedTo int

    addFileHelper(t, filename1, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 2)
    addFileHelper(t, filename2, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 0) // added so far is for an individual drive
    addFileHelper(t, filename3, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 1)

    addFileHelper(t, filename1_1, username, types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), false, 1, 2)
    addFileHelper(t, filename2_2, username, types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), true, 1, 0)
    addFileHelper(t, filename1_2, username, types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), true, 2, 2)
    /*
                0
               / \
              2   1
             /
            3
    */
    // note that 2 is stored at header + 3* (size of entry) because of the "root"
    // in the header
    addFileHelper(t, filename1_3, username, types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY), true, 3, 2)

    /*
                0
               / \
              2   1
             /
            3
           /
          4
    */
    addFileHelper(t, filename1_4, username, types.HEADER_SIZE + 5*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY), true, 4, 2)
    /*
                0
               / \
              2   1
             /
            3
           / \
          4   5
    */
    addFileHelper(t, filename1_5, username, types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY), false, 5, 2)


    removeDatabaseStructureAndCheck(t)
}

func TestGettingFile(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"
    filename := "testingFile.txt"

    CreateDatabaseForUser(username, configs)
    // t *testing.T, filename string, username string, 
    //             shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
    //             addedSoFar int, driveAddedTo int

    addFileHelper(t, filename, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 2)

    entry := GetFileEntry(filename, username, configs)
    if entry == nil {
        t.Errorf("The entry returned is nil")
        removeDatabaseStructureAndCheck(t)
        return
    }

    if entry.Filename != filename {
        t.Errorf("Filename is incorrect in entry, it is %s", entry.Filename)
        fmt.Printf("Filename is %s, should be %s\n", entry.Filename, filename)
        fmt.Printf("Length of filename is %d, should be %d\n", len(entry.Filename), len(filename))
    }

    for i := 0; i < int(types.MAX_DISK_COUNT); i++ {
        // shouldBe := fmt.Sprintf("local_storage/drive%d", i + 1)
        shouldBe := configs.Datadisks[i]
        if entry.Disks[i] != shouldBe {
            t.Errorf("One of the disk locations is incorrect, it is %s", entry.Disks[i])
        }
    }

    if entry.Left != 0 && entry.Right != 0 {
        t.Errorf("The pointers for entry are not right")
    }

    removeDatabaseStructureAndCheck(t)
}

func deleteFileHelper(t *testing.T, filename string, username string,
                      shouldFindTheFile bool, parentWasAt int64, nodeWasAt int64,
                      freeListShouldBe int64, shouldNowPointTo int64, wasLeft bool,
                      parentShouldPointTo int64, addedSoFar int, drive int) {
    // note: in the context of the database, localhost just means that it will
    // be stored on the same machine but with the file structure, not really
    // separate drives (will be simulated with separate folders)
    errCode := DeleteFileEntry(filename, username, configs)

    if shouldFindTheFile && errCode != 0 {
        t.Errorf("Error code is incorrect, should have found the file")
        return
    }

    // check that the entry is no longer there, and that the header was
    // updated accordingly
    dbFilename := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[drive], username, drive)
    dbFile, err := os.Open(dbFilename)
    check(err)

    buf := make([]byte, types.SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(buf, nodeWasAt)
    check(err)

    freeListPointer := int64(0)
    b := bytes.NewReader(buf[0:types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &freeListPointer); check(err)

    if freeListPointer != shouldNowPointTo {
        t.Errorf("Deleted entry does not point to right free list entry now")
    }

    // check the parent entry now (root, in this case)
    buf = make([]byte, types.SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(buf, parentWasAt)
    check(err)

    entryFilename := bytes.Trim(buf[0:types.MAX_FILE_NAME_SIZE], "\x00")
    entry := types.TreeEntry{string(entryFilename), 0, 0, []string(nil)}

    if entry.Filename == filename {
        t.Errorf("Parent has same filename as deleted node for some reason")
    }

    b = bytes.NewReader(buf[types.MAX_FILE_NAME_SIZE: types.MAX_FILE_NAME_SIZE + types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Left); check(err)
    b = bytes.NewReader(buf[types.MAX_FILE_NAME_SIZE + types.POINTER_SIZE: types.MAX_FILE_NAME_SIZE + 2 * types.POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Right); check(err)

    if !wasLeft && entry.Right != parentShouldPointTo {
        t.Errorf("Link to replacement entry from parent is incorrect")
    }
    if wasLeft && entry.Left != parentShouldPointTo {
        t.Errorf("Link to replacement entry from parent is incorrect")
        fmt.Printf("Is: %d, should be %d\n", entry.Left, parentShouldPointTo)
    }

    // check the header now
    buf = make([]byte, types.HEADER_SIZE)
    _, err = dbFile.ReadAt(buf, 0)
    check(err)

    header := Header{0, 0, 0, 0, 0, 0}
    b = bytes.NewReader(buf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    // addedSofar because have to account for root
    if header.TrueDbSize != types.HEADER_SIZE + int64(addedSoFar)*int64(types.SIZE_OF_ENTRY) {
        t.Errorf("Truedbsize did not update properly")
    }
    if header.FreeList != freeListShouldBe {
        t.Errorf("Free list pointer in header did not update properly, it is %d", header.FreeList)
    }

    // check that the file is the right size


    // also check that the parity disk is correct
    dbParityFileName := fmt.Sprintf("%s/%s_p", configs.Dbdisks[len(configs.Dbdisks) - 1], username)
    dbParityFile, err := os.Open(dbParityFileName); check(err)

    fileStat, err := dbParityFile.Stat(); check(err);
    sizeOfParityFile := fileStat.Size(); // in bytes

    parityBuf := make([]byte, sizeOfParityFile)
    _, err = dbParityFile.ReadAt(parityBuf, 0)
    check(err)

    testBuf := make([]byte, sizeOfParityFile)

    for i := 0; i < TESTING_DISK_COUNT; i++ {
        dbFilename := fmt.Sprintf("%s/%s_%d", configs.Dbdisks[i], username, i)
        dbFile, err := os.Open(dbFilename)
        check(err)

        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes
        if sizeOfDbFile != sizeOfParityFile {
            t.Errorf("The parity and db files are different lengths")
            break
        }

        buf := make([]byte, sizeOfDbFile)
        _, err = dbFile.ReadAt(buf, 0)
        check(err)

        for j := 0; j < int(sizeOfDbFile); j++ {
            testBuf[j] ^= buf[j]
        }

        dbFile.Close()
    }

    for i := 0; i < int(sizeOfParityFile); i++ {
        if parityBuf[i] != testBuf[i] {
            t.Errorf("Incorrect XOR at location %d\n", i)
            fmt.Printf("Parity = %x, should be %x\n", parityBuf[i], testBuf[i])
            break
        }
    }

    dbParityFile.Close()
    dbFile.Close()
}

func TestDeletingAFile(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"
    filename1 := "testingFile.txt"
    // filename2 := "1TestingFile.txt"
    // filename3 := "fTestingFile.txt"
    // filename1_1 := "testingFile2.txt"
    // filename2_2 := "0TestingFile.txt"
    // filename1_2 := "sestingFile.txt"
    // filename1_3 := "sastingFile.txt"
    // filename1_4 := "saatingFile.txt"
    // filename1_5 := "sattingFile.txt"

    CreateDatabaseForUser(username, configs)
    // t *testing.T, filename string, username string, 
    //             shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
    //             addedSoFar int, driveAddedTo int

    addFileHelper(t, filename1, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 2)

    // t *testing.T, filename string, username string,
    //                   shouldFindTheFile bool, parentWasAt int64, nodeWasAt int64,
    //                   freeListShouldBe int64, shouldNowPointTo int64, wasLeft bool,
    //                   parentShouldPointTo int64, addedSoFar int, drive int

    // shouldNowPointTo should point to types.HEADER_SIZE + 2*(size of entry) because it
    // points to what the free list used to point to before it got deleted
    deleteFileHelper(t, filename1, username, true, types.HEADER_SIZE,
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                    types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY), false, 0, 1, 2)

    removeDatabaseStructureAndCheck(t)
}

func TestDeletingMultipleFiles(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"
    filename1 := "testingFile.txt"
    filename2 := "1TestingFile.txt"
    filename3 := "fTestingFile.txt"
    filename1_1 := "testingFile2.txt"
    filename2_2 := "0TestingFile.txt"
    filename1_2 := "sestingFile.txt"
    filename1_3 := "sastingFile.txt"
    filename1_4 := "saatingFile.txt"
    filename1_5 := "sattingFile.txt"
    filename1_6 := "sabtingFile.txt"

    CreateDatabaseForUser(username, configs)
    // t *testing.T, filename string, username string, 
    //             shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
    //             addedSoFar int, driveAddedTo int

    addFileHelper(t, filename1, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 2)

    // t *testing.T, filename string, username string,
    //                   shouldFindTheFile bool, parentWasAt int64, nodeWasAt int64,
    //                   freeListShouldBe int64, shouldNowPointTo int64, wasLeft bool,
    //                   parentShouldPointTo int64, addedSoFar int, drive int

    // shouldNowPointTo should point to types.HEADER_SIZE + 2*(size of entry) because it
    // points to what the free list used to point to before it got deleted
    deleteFileHelper(t, filename1, username, true, types.HEADER_SIZE,
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                    types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY), false, 0, 1, 2)

    addFileHelper(t, filename2, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 0)

    addFileHelper(t, filename3, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 1)

    deleteFileHelper(t, filename2, username, true, types.HEADER_SIZE,
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                    types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                    types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY), false, 0, 1, 0)


    // re-add files, drive 2 should be empty now (as well as drive 0)

    addFileHelper(t, filename1, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), 
                  types.HEADER_SIZE, false, 0, 2)
    addFileHelper(t, filename1_1, username, types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), false, 1, 2)
    // add at header + (size of entry) now because the entry that was there before
    // was deleted (i.e. the parent of 2_2))
    addFileHelper(t, filename2_2, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE, false, 0, 0)
    addFileHelper(t, filename1_2, username, types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), true, 2, 2)
    /*
                0
               / \
              2   1
             /
            3
    */
    // note that 2 is stored at header + 3* (size of entry) because of the "root"
    // in the header
    addFileHelper(t, filename1_3, username, types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY), true, 3, 2)

    /*
                0
               / \
              2   1
             /
            3
           /
          4
    */
    addFileHelper(t, filename1_4, username, types.HEADER_SIZE + 5*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY), true, 4, 2)
    /*
                0
               / \
              2   1
             /
            3
           / \
          4   5
    */
    addFileHelper(t, filename1_5, username, types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY), false, 5, 2)
    /*
        Try deleting some of the intermediate entries now

        ex: after deleting 2, the tree should look like this:
                0
               / \
              3   1
             / \
            4   5
    */

    // t *testing.T, filename string, username string,
    //                   shouldFindTheFile bool, parentWasAt int64, nodeWasAt int64,
    //                   freeListShouldBe int64, shouldNowPointTo int64, wasLeft bool,
    //                   parentShouldPointTo int64, addedSoFar int, drive int

    deleteFileHelper(t, filename1_2, username, true, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
                    types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY), types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY),
                    types.HEADER_SIZE + 7*int64(types.SIZE_OF_ENTRY), true, types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY),
                    6, 2)

    // adding it back should put it in same spot physically, but not same spot in tree,
    // should be to the right of 5
    // t *testing.T, filename string, username string, 
    //             shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
    //             addedSoFar int, driveAddedTo int
    addFileHelper(t, filename1_2, username, types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY), false, 5, 2)
    /*
        Added back 2:
                0
               / \
              3   1
             / \
            4   5
                 \
                  2

        Now add file to go to right of 4, and then delete 3, to see what happens
    */

    /*
        Adding in 6 now:
                0
               / \
              3   1
             / \
            4   5
             \   \
              6   2
    */

    addFileHelper(t, filename1_6, username, types.HEADER_SIZE + 7*int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 5*int64(types.SIZE_OF_ENTRY), false, 6, 2)

    // since replacement = leftmost in right-hand tree, 5 would replace it
    deleteFileHelper(t, filename1_3, username, true, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
                    types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY), types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY),
                    types.HEADER_SIZE + 8*int64(types.SIZE_OF_ENTRY), true, types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY),
                    7, 2)

    /*
        Looks like this now:
                0
               / \
              5   1
             / \
            4   2
             \   
              6  
    */
    // 0 should point to 2 after this deletion
    deleteFileHelper(t, filename1_5, username, true, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
            types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY), types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY),
            types.HEADER_SIZE + 4*int64(types.SIZE_OF_ENTRY), true, types.HEADER_SIZE + 3*int64(types.SIZE_OF_ENTRY),
            6, 2)

    /*
        Looks like this now:
                0
               / \
              2   1
             /
            4
             \   
              6  
    */
    // root should point to 1 after this deletion
    deleteFileHelper(t, filename1, username, true, types.HEADER_SIZE,
            types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY), types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
            types.HEADER_SIZE + 6*int64(types.SIZE_OF_ENTRY), false, types.HEADER_SIZE + 2*int64(types.SIZE_OF_ENTRY),
            5, 2)

    // add in a file and see if it goes to right place
    /*
        Looks like this now:
                1
               /
              2
             /
            4
             \   
              6  
    */
              // t *testing.T, filename string, username string, 
    //             shouldBeAddedAt int64, parentShouldBeAt int64, shouldBeLeft bool,
    //             addedSoFar int, driveAddedTo int
    addFileHelper(t, filename1_5, username, types.HEADER_SIZE + int64(types.SIZE_OF_ENTRY),
                  types.HEADER_SIZE + 7*int64(types.SIZE_OF_ENTRY), false, 4, 2)

    /*
        Should look like this now:
                1
               /
              2
             /
            4
             \   
              6
               \
                5  
    */

    removeDatabaseStructureAndCheck(t)
}

func TestOverallAddingGettingDeleting(t *testing.T) {
    InitializeDatabaseStructure(configs.Dbdisks)

    username := "atoron"
    // filename1 := "1"
    // filename2 := "2"
    // filename3 := "3"
    // filename4 := "4"
    // filename5 := "5"
    // filename6 := "6"
    // filename7 := "7"
    // filename8 := "8"
    // filename9 := "9"

    amountOfFiles := 50
    filenames := make([]string, amountOfFiles)
    for i := 0; i < len(filenames); i++ {
        filenames[i] = fmt.Sprintf("%d", i)
    }

    CreateDatabaseForUser(username, configs)

    inDatabase := make([]bool, amountOfFiles)
    for i := 0; i < len(inDatabase); i++ {
        inDatabase[i] = false
    }

    previousTree := []string(nil)
    currentTree := []string(nil)

    // initialize the database somewhat
    added := 0
    for added != (amountOfFiles / 2) {
        num := rand.Intn(amountOfFiles - 1) + 1
        if !inDatabase[num] {
            AddFileSpecsToDatabase(filenames[num], username, configs.Datadisks, configs)
            inDatabase[num] = true
            added++
        }
    }

    currentTree = PrettyPrintTreeGetString(username, 0, amountOfFiles, configs)
    middleTree := currentTree
    previousAddition := 0
    previousDeletion := 0

    r := 0
    for r != ROUNDS {
        previousTree = currentTree

        // add one
        num := rand.Intn(amountOfFiles - 1) + 1
        for inDatabase[num] {
            num = rand.Intn(amountOfFiles - 1) + 1
        }
        AddFileSpecsToDatabase(filenames[num], username, configs.Datadisks, configs)
        inDatabase[num] = true
        previousAddition = num
        // previousTree = currentTree
        // currentTree = PrettyPrintTreeGetString(LOCALHOST, username)
        middleTree = PrettyPrintTreeGetString(username, 0, amountOfFiles, configs)
        currentTree = PrettyPrintTreeGetString(username, 0, amountOfFiles, configs)

        // get one
        num = rand.Intn(amountOfFiles - 1) + 1
        for !inDatabase[num] {
            num = rand.Intn(amountOfFiles - 1) + 1
        }
        entry := GetFileEntry(filenames[num], username, configs)
        // previousTree = currentTree
        // currentTree = PrettyPrintTreeGetString(LOCALHOST, username)
        currentTree = PrettyPrintTreeGetString(username, 0, amountOfFiles, configs)
        if entry == nil {
            t.Errorf("Did not get entry %d at all", num)
            fmt.Printf("Added %d, deleted %d\n", previousAddition, previousDeletion)
            fmt.Printf("Previous tree: \n")
            for i := 0; i < len(currentTree); i++ {
                fmt.Printf("%s\n", previousTree[i])
            }
            fmt.Printf("Middle (after addition) tree: \n")
            for i := 0; i < len(currentTree); i++ {
                fmt.Printf("%s\n", middleTree[i])
            }
            fmt.Printf("Current tree: \n")
            for i := 0; i < len(currentTree); i++ {
                fmt.Printf("%s\n", currentTree[i])
            }
            fmt.Printf("In database: ")
            for i := 0; i < len(inDatabase); i++ {
                if inDatabase[i] {
                    fmt.Printf("%d,", i)
                }
            }
            fmt.Printf("\n")
            break
        }
        if entry.Filename != filenames[num] {
            t.Errorf("Did not get correct entry")
            break
        }

        // remove one
        num = rand.Intn(amountOfFiles - 1) + 1
        for !inDatabase[num] {
            num = rand.Intn(amountOfFiles - 1) + 1
        }
        errCode := DeleteFileEntry(filenames[num], username, configs)
        previousDeletion = num
        // previousTree = currentTree
        // currentTree = PrettyPrintTreeGetString(LOCALHOST, username)
        currentTree = PrettyPrintTreeGetString(username, 0, amountOfFiles, configs)
        if errCode != 0 {
            t.Errorf("There was an error in deletion")
            fmt.Printf("Added %d, deleted %d\n", previousAddition, previousDeletion)
            fmt.Printf("Previous tree: \n")
            for i := 0; i < len(currentTree); i++ {
                fmt.Printf("%s\n", previousTree[i])
            }
            fmt.Printf("Middle (after addition) tree: \n")
            for i := 0; i < len(currentTree); i++ {
                fmt.Printf("%s\n", middleTree[i])
            }

            fmt.Printf("Current tree: \n")
            for i := 0; i < len(currentTree); i++ {
                fmt.Printf("%s\n", currentTree[i])
            }
            fmt.Printf("In database: ")
            for i := 0; i < len(inDatabase); i++ {
                if inDatabase[i] {
                    fmt.Printf("%d,", i)
                }
            }
            fmt.Printf("\n")

            break
        }
        inDatabase[num] = false
        currentTree = PrettyPrintTreeGetString(username, 0, amountOfFiles, configs)

        r++
    }
    
    removeDatabaseStructureAndCheck(t)
}

// TODO, have to figure out a way to stop the function halfway through
// can manually extract one of the WAL files that happens in the tests above
// and run ReplayLog to see if it makes the database do the same thing
// can maybe add a boolean into commit that keeps the file or not
func TestRecoveringFromSystemCrash(t *testing.T) {

}