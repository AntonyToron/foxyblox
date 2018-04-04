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
)

const SMALL_FILE_SIZE int = 1024
const BUF_SIZE int = 1024
const VERY_SMALL_FILE_SIZE = 6 // currently 1, 3 aren't working perfectly
const REGULAR_FILE_SIZE int = 8192

// 24
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(18))) //int64(math.Pow(2, float64(30))) // 1 GB

func TestMain(m *testing.M) {
    fmt.Println("Setting up for tests")

    rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time

    retCode := m.Run()

    fmt.Println("Finished tests")

    os.Exit(retCode)
}

func removeDatabaseStructureAndCheck(t *testing.T) {
    RemoveDatabaseStructure(LOCALHOST, nil)

    if pathExists("./storage") {
        t.Errorf("Did not delete a folder desired")
    }
    
    for i := 0; i < DISK_COUNT; i++ {
        diskFolder := fmt.Sprintf("./storage/drive%d", i + 1)
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if pathExists(diskFolder) || pathExists(dbdiskFolder) {
            t.Errorf("Did not delete a folder desired")
        }
    }

    parityFolder := fmt.Sprintf("./storage/drivep")
    dbParityFolder := fmt.Sprintf("./storage/dbdrivep")
    if pathExists(parityFolder) || pathExists(dbParityFolder) {
        t.Errorf("Did not delete a folder desired")
    }
}

func TestDatabaseInitializationAndRemoval(t *testing.T) {
    InitializeDatabaseStructure(LOCALHOST, nil)

    if !pathExists("./storage") {
        t.Errorf("Did not create a folder desired")
    }
    
    for i := 0; i < DISK_COUNT; i++ {
        diskFolder := fmt.Sprintf("./storage/drive%d", i + 1)
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if !pathExists(diskFolder) || !pathExists(dbdiskFolder) {
            t.Errorf("Did not create a folder desired")
        }
    }

    parityFolder := fmt.Sprintf("./storage/drivep")
    dbParityFolder := fmt.Sprintf("./storage/dbdrivep")
    if !pathExists(parityFolder) || !pathExists(dbParityFolder) {
        t.Errorf("Did not create a folder desired")
    }

    removeDatabaseStructureAndCheck(t)
}

func TestDatabaseCreationAndRemoval(t *testing.T) {
    InitializeDatabaseStructure(LOCALHOST, nil)

    username := "atoron"

    CreateDatabaseForUser(LOCALHOST, nil, username)

    for i := 0; i < DISK_COUNT; i++ {
        dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
        if !pathExists(dbCompLocation) {
            t.Errorf("Did not create all of the database files")
            return
        }
    }

    dbParityFileName := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
    dbParityFile, err := os.Open(dbParityFileName); check(err)
    parityBuf := make([]byte, HEADER_SIZE)
    _, err = dbParityFile.ReadAt(parityBuf, 0)

    testBuf := make([]byte, HEADER_SIZE)

    // check that headers are correct, and that the parity disk is correct as well
    for i := 0; i < DISK_COUNT; i++ {
        dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
        dbFile, err := os.Open(dbCompLocation)
        check(err)

        fileStat, err := dbFile.Stat(); check(err);
        sizeOfDbFile := fileStat.Size(); // in bytes
        if sizeOfDbFile != HEADER_SIZE {
            t.Errorf("Incorrect database file size")
        }

        buf := make([]byte, HEADER_SIZE)
        _, err = dbFile.ReadAt(buf, 0)
        check(err)

        header := Header{0, 0, 0, 0, 0, 0}
        b := bytes.NewReader(buf)
        err = binary.Read(b, binary.LittleEndian, &header)
        check(err)

        if header.FileNameSize != MAX_FILE_NAME_SIZE || header.DiskCount != MAX_DISK_COUNT {
            t.Errorf("Part of the header is incorrect")
        }
        if header.DiskNameSize != MAX_DISK_NAME_SIZE || header.RootPointer != HEADER_SIZE {
            t.Errorf("Part of the header is incorrect")
        }
        if header.FreeList != HEADER_SIZE + int64(SIZE_OF_ENTRY) || header.TrueDbSize != HEADER_SIZE + int64(SIZE_OF_ENTRY) {
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

    DeleteDatabaseForUser(LOCALHOST, nil, username)

    for i := 0; i < DISK_COUNT; i++ {
        // dbCompLocation := fmt.Sprintf("%s/%s_%d", dbdisklocations[i], username, i)
        dbCompLocation := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)

        if pathExists(dbCompLocation) {
            t.Errorf("Did not remove all the database files")
        }
    }

    dbParityFileName = fmt.Sprintf("./storage/dbdrivep/%s_p", username)
    if pathExists(dbParityFileName) {
        t.Errorf("Did not remove all the database files")
    }

    removeDatabaseStructureAndCheck(t)
}

func TestAddingAFile(t *testing.T) {
    InitializeDatabaseStructure(LOCALHOST, nil)

    username := "atoron"
    filename := "testingFile.txt"

    CreateDatabaseForUser(LOCALHOST, nil, username)

    AddFileSpecsToDatabase(filename, username, LOCALHOST, nil)

    // check that the entry is in the correct spot, and that the header was
    // updated accordingly
    // should have gone into 2 (b/c it is >= 113, and t = 116)
    dbFilename := fmt.Sprintf("./storage/dbdrive2/%s_2", username)
    dbFile, err := os.Open(dbFilename)
    check(err)

    buf := make([]byte, SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(buf, HEADER_SIZE + int64(SIZE_OF_ENTRY))
    check(err)

    entryFilename := bytes.Trim(buf[0:MAX_FILE_NAME_SIZE], "\x00")
    entry := TreeEntry{string(entryFilename), 0, 0, []string(nil)}

    b := bytes.NewReader(buf[MAX_FILE_NAME_SIZE: MAX_FILE_NAME_SIZE + POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Left); check(err)
    b = bytes.NewReader(buf[MAX_FILE_NAME_SIZE + POINTER_SIZE: MAX_FILE_NAME_SIZE + 2 * POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Right); check(err)

    if entry.Filename != filename {
        t.Errorf("Filename is incorrect in entry, it is %s", entry.Filename)
        fmt.Printf("Filename is %s, should be %s\n", entry.Filename, filename)
        fmt.Printf("Length of filename is %d, should be %d\n", len(entry.Filename), len(filename))
    }

    entry.Disks = make([]string, MAX_DISK_COUNT)
    for i := 0; i < int(MAX_DISK_COUNT); i++ {
        upperBound := int(MAX_FILE_NAME_SIZE) + 2 * int(POINTER_SIZE) + (i + 1) * int(MAX_DISK_NAME_SIZE)
        lowerBound := int(MAX_FILE_NAME_SIZE) + 2 * int(POINTER_SIZE) + i * int(MAX_DISK_NAME_SIZE)
        entry.Disks[i] = string(bytes.Trim(buf[lowerBound:upperBound], "\x00"))
        shouldBe := fmt.Sprintf("local_storage/drive%d", i + 1)
        if entry.Disks[i] != shouldBe {
            t.Errorf("One of the disk locations is incorrect, it is %s", entry.Disks[i])
        }
    }

    if entry.Left != 0 && entry.Right != 0 {
        t.Errorf("The pointers for entry are not right")
    }

    // check the parent entry now (root, in this case)
    buf = make([]byte, SIZE_OF_ENTRY)
    _, err = dbFile.ReadAt(buf, HEADER_SIZE)
    check(err)

    entry = TreeEntry{string(buf[0:MAX_FILE_NAME_SIZE]), 0, 0, []string(nil)}

    b = bytes.NewReader(buf[MAX_FILE_NAME_SIZE: MAX_FILE_NAME_SIZE + POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Left); check(err)
    b = bytes.NewReader(buf[MAX_FILE_NAME_SIZE + POINTER_SIZE: MAX_FILE_NAME_SIZE + 2 * POINTER_SIZE])
    err = binary.Read(b, binary.LittleEndian, &entry.Right); check(err)

    if entry.Right != HEADER_SIZE + int64(SIZE_OF_ENTRY) {
        t.Errorf("Link to entry from root is incorrect")
    }
    if entry.Left != 0 {
        t.Errorf("Left link is not 0")
    }

    // check the header now
    buf = make([]byte, HEADER_SIZE)
    _, err = dbFile.ReadAt(buf, 0)
    check(err)

    header := Header{0, 0, 0, 0, 0, 0}
    b = bytes.NewReader(buf)
    err = binary.Read(b, binary.LittleEndian, &header)
    check(err)

    if header.TrueDbSize != HEADER_SIZE + 2*int64(SIZE_OF_ENTRY) {
        t.Errorf("Truedbsize did not update properly")
    }
    if header.FreeList != HEADER_SIZE + 2*int64(SIZE_OF_ENTRY) {
        t.Errorf("Free list pointer in header did not update properly, it is %d", header.FreeList)
    }

    // check that the file is the right size


    // also check that the parity disk is correct
    dbParityFileName := fmt.Sprintf("./storage/dbdrivep/%s_p", username)
    dbParityFile, err := os.Open(dbParityFileName); check(err)

    fileStat, err := dbParityFile.Stat(); check(err);
    sizeOfParityFile := fileStat.Size(); // in bytes

    parityBuf := make([]byte, sizeOfParityFile)
    _, err = dbParityFile.ReadAt(parityBuf, 0)
    check(err)

    testBuf := make([]byte, sizeOfParityFile)

    for i := 0; i < DISK_COUNT; i++ {
        dbFilename := fmt.Sprintf("./storage/dbdrive%d/%s_%d", i, username, i)
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

    removeDatabaseStructureAndCheck(t)
}

