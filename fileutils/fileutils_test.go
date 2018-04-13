/*******************************************************************************
* Author: Antony Toron
* File name: fileutils_test.go
* Date created: 2/16/18
*
* Description: tests file utilities - benchmarking tests + correctness
*******************************************************************************/

package fileutils

import (
    "testing"
    "math/rand"
    "math"
    "os"
    "fmt"
    "bytes"
    "os/exec"
    "time"
    "foxyblox/types"
)

const SMALL_FILE_SIZE int = 1024
const BUFFER_SIZE int = 1024
const VERY_SMALL_FILE_SIZE = 6 // currently 1, 3 aren't working perfectly
const REGULAR_FILE_SIZE int = 8192
const TESTING_DISK_COUNT int = 3
// 24
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(18))) //int64(math.Pow(2, float64(30))) // 1 GB
var configs *types.Config
var diskLocations []string

func TestMain(m *testing.M) {
    fmt.Println("Setting up for tests")

    rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time
    // os.Chdir("../") // go back to home directory

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

    initializeDatabaseStructureLocal()

    retCode := m.Run()

    fmt.Println("Finished tests")

    removeDatabaseStructureLocal()

    // clean up
    cmd := exec.Command("rm", "-rf", "./downloaded*", "./testing*")

    var out bytes.Buffer
    var stderr bytes.Buffer
    cmd.Stdout = &out
    cmd.Stderr = &stderr
    err := cmd.Run()

    if err != nil {
        fmt.Printf("Diff stderr: %q\n", stderr.String())
    }

    fmt.Printf("Diff stdout: %q\n", out.String())

    os.Exit(retCode)
}

func initializeDatabaseStructureLocal() bool {
    var madeChanges bool = false

    if !pathExists("./storage") {
        os.Mkdir("storage", types.REGULAR_FILE_MODE)
        madeChanges = true
    }

    for i := 0; i < TESTING_DISK_COUNT + 1; i++ {
        diskFolder := fmt.Sprintf("./storage/drive%d", i)
        if !pathExists(diskFolder) {
            os.Mkdir(diskFolder, types.REGULAR_FILE_MODE)
            madeChanges = true
        }
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if !pathExists(dbdiskFolder) {
            os.Mkdir(dbdiskFolder, types.REGULAR_FILE_MODE)
            madeChanges = true
        }
    }

    return madeChanges
}

func removeDatabaseStructureLocal() {
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

func testSavingCorrectnessHelper(t *testing.T, size int, testingFilename string, username string) {
    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    fileData := make([]byte, size)
    rand.Read(fileData)

    _, err = testingFile.WriteAt(fileData, 0)
    check(err)
    testingFile.Close()

    startTime := time.Now()

    // call saveFile
    SaveFile(testingFilename, username, diskLocations, configs)

    elapsed := time.Since(startTime)
    fmt.Printf("Saving file of size %d took %s\n", size, elapsed)

    // check if the XOR of the components is correct
    currentXOR := make([]byte, size)
    for i := 0; i < TESTING_DISK_COUNT; i++ {
        filename := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i], username, testingFilename, i)
        file, err := os.Open(filename)
        if err != nil {
            t.Errorf("Could not open %d\n", filename)
        }
        fileStat, err := file.Stat()
        if err != nil {
            t.Errorf("Could not check stat of %d\n", filename)
        }

        size := fileStat.Size()
        size -= types.MD5_SIZE // strip off the hash at the end

        fileBuffer := make([]byte, size)
        file.ReadAt(fileBuffer, 0)
        for j := 0; j < int(size); j++ {
            currentXOR[j] ^= fileBuffer[j]
        }

        file.Close()
    }

    filename := fmt.Sprintf("%s/%s/%s_p", diskLocations[len(diskLocations) - 1], username, testingFilename)
    file, err := os.Open(filename)
    if err != nil {
        t.Errorf("Could not open %d\n", filename)
    }
    fileStat, err := file.Stat()
    if err != nil {
        t.Errorf("Could not check stat of %d\n", filename)
    }

    fileSize := fileStat.Size()
    fileSize -= types.MD5_SIZE // strip off the hash at the end

    fileBuffer := make([]byte, fileSize)
    file.ReadAt(fileBuffer, 0)

    for i := 0; i < int(fileSize); i++ {
        if currentXOR[i] != fileBuffer[i] {
            t.Errorf("Found a difference between computed XOR and file XOR at byte %d\n", i)
        }
    }

    file.Close()

    // testingFilename = fmt.Sprintf("./%s", testingFilename)
    // os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, username, diskLocations, configs)
}

func TestSavingCorrectnessVerySmallFile(t *testing.T) {
    testingFilename := "testingFileVerySmall.txt"
    username := "atoron"

    testSavingCorrectnessHelper(t, VERY_SMALL_FILE_SIZE, testingFilename, username)
}

func TestSavingCorrectnessSmallFile(t *testing.T) {
    // rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time
    // os.Chdir("../") // go back to home directory

    // create sample file with random binary data
    testingFilename := "testingFile.txt"
    username := "atoron"

    testSavingCorrectnessHelper(t, SMALL_FILE_SIZE, testingFilename, username)
}

func TestSavingCorrectnessLargeFile(t *testing.T) {
    //os.Chdir("../") // go back to home directory

    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    username := "atoron"

    testSavingCorrectnessHelper(t, int(LARGE_FILE_SIZE), testingFilename, username)
}

func TestGettingFile(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileRegular.txt"
    username := "atoron"

    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    for i := int64(0); i < (LARGE_FILE_SIZE / int64(REGULAR_FILE_SIZE)); i++ {
        smallData := make([]byte, REGULAR_FILE_SIZE)
        rand.Read(smallData)

        _, err = testingFile.WriteAt(smallData, i * int64(REGULAR_FILE_SIZE))
        if err != nil {
            t.Errorf("Could not write to %d\n", testingFilename)
        }
    }

    // call saveFile
    SaveFile(testingFilename, username, diskLocations, configs)

    GetFile(testingFilename, username, diskLocations, configs)

    cmd := exec.Command("diff", testingFilename, "downloaded-" + testingFilename)

    var out bytes.Buffer
    var stderr bytes.Buffer
    cmd.Stdout = &out
    cmd.Stderr = &stderr
    err = cmd.Run()
    check(err)

    fmt.Printf("Diff stdout: %q\n", out.String())

    if err != nil {
        fmt.Printf("Diff stderr: %q\n", stderr.String())
        t.Errorf("Diff stderr not empty")
    }

    fmt.Printf("Diff stdout: %q\n", out.String())

    if out.String() != "" {
        t.Errorf("Diff output was not empty")
    }

    // testingFilename = fmt.Sprintf("./%s", testingFilename)
    // os.Remove(testingFilename)
    // testingFilename = fmt.Sprintf("./downloaded-%s", testingFilename)
    // os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, username, diskLocations, configs)
}

func testSimulatedDiskCorruptionHelper(t *testing.T, size int, testingFilename string, fileToCorrupt string) {
    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    username := "atoron"

    smallFileData := make([]byte, size)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)
    check(err)
    testingFile.Close()

    // call saveFile
    SaveFile(testingFilename, username, diskLocations, configs)

    // insert some faulty bits into the file
    file, err := os.OpenFile(fileToCorrupt, os.O_RDWR, 0755)
    check(err)

    amountOfErrors := 50
    if size < 50 {
        amountOfErrors = 16
    }
    locationOfErrors := 5
    if size < 50 {
        locationOfErrors = 0
    }

    buf := make([]byte, amountOfErrors)
    rand.Read(buf)
    for i:= 0; i < len(buf); i++ {
        fmt.Printf("%x ", buf[i])
    }
    fmt.Printf("\n")
    _, err = file.WriteAt(buf, int64(locationOfErrors)) //int64(SMALL_FILE_SIZE - 50)
    check(err)

    fmt.Printf("Wrote some faulty bits\n")

    file.Close()

    GetFile(testingFilename, username, diskLocations, configs)

    // diff should still be fine, because recovered

    cmd := exec.Command("diff", testingFilename, "downloaded-" + testingFilename)

    var out bytes.Buffer
    var stderr bytes.Buffer
    cmd.Stdout = &out
    cmd.Stderr = &stderr
    err = cmd.Run()
    fmt.Printf("About to run command\n")
    // check(err)
    fmt.Printf("Ran command\n")
    if err != nil {
        fmt.Printf("Diff stderr: %q\n", stderr.String())
        t.Errorf("Diff stderr not empty")
    }

    fmt.Printf("Diff stdout: %q\n", out.String())

    if out.String() != "" {
        t.Errorf("Diff output was not empty")
    }

    // testingFilename = fmt.Sprintf("./%s", testingFilename)
    // os.Remove(testingFilename)
    // testingFilename = fmt.Sprintf("./downloaded-%s", testingFilename)
    // os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, username, diskLocations, configs)
}

func TestSimulatedDataDiskCorruption(t *testing.T) {
    fmt.Printf("\n\n\n SIMULATING DISK CORRUPTION \n\n\n")

    // create sample file with random binary data
    testingFilename := "testingFile.txt"
    username := "atoron"
    fileToCorrupt := fmt.Sprintf("./storage/drive1/%s/%s_1", username, testingFilename)
    testSimulatedDiskCorruptionHelper(t, SMALL_FILE_SIZE, testingFilename, fileToCorrupt)
}

func TestSimulatedDataDiskCorruptionLarge(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    username := "atoron"
    fileToCorrupt := fmt.Sprintf("./storage/drive1/%s/%s_1", username, testingFilename)
    testSimulatedDiskCorruptionHelper(t, int(LARGE_FILE_SIZE), testingFilename, fileToCorrupt)
}

func TestSimulatedDataDiskCorruptionLargeAndPadding(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    username := "atoron"
    fileToCorrupt := fmt.Sprintf("./storage/drive%d/%s/%s_%d", TESTING_DISK_COUNT - 1, username, testingFilename, TESTING_DISK_COUNT - 1)
    testSimulatedDiskCorruptionHelper(t, int(LARGE_FILE_SIZE), testingFilename, fileToCorrupt)
}

func TestSimulatedParityDiskCorruption(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    username := "atoron"

    smallFileData := make([]byte, LARGE_FILE_SIZE)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)
    check(err)
    testingFile.Close()

    // call saveFile
    SaveFile(testingFilename, username, diskLocations, configs)

    // insert some faulty bits into the file
    parityFilename := fmt.Sprintf("./storage/drive%d/%s/%s_p", len(diskLocations) - 1, username, testingFilename)
    file, err := os.OpenFile(parityFilename, os.O_RDWR, 0755)
    check(err)

    buf := make([]byte, 50)
    rand.Read(buf)
    for i:= 0; i < len(buf); i++ {
        fmt.Printf("%x ", buf[i])
    }
    fmt.Printf("\n")
    _, err = file.WriteAt(buf, 5) //int64(SMALL_FILE_SIZE - 50)
    check(err)

    fmt.Printf("Wrote some faulty bits\n")

    file.Close()

    GetFile(testingFilename, username, diskLocations, configs)

    // check that the parity disk is now correct
    // check if the XOR of the components is correct
    currentXOR := make([]byte, LARGE_FILE_SIZE) // / 3
    for i := 0; i < TESTING_DISK_COUNT; i++ {
        filename := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i], username, testingFilename, i)
        file, err := os.Open(filename)
        if err != nil {
            t.Errorf("Could not open %d\n", filename)
        }
        fileStat, err := file.Stat()
        if err != nil {
            t.Errorf("Could not check stat of %d\n", filename)
        }

        size := fileStat.Size()
        fmt.Printf("Size : %d\n", size)
        size -= types.MD5_SIZE // strip off the hash at the end
        fileBuffer := make([]byte, size)
        file.ReadAt(fileBuffer, 0)
        for j := 0; j < int(size); j++ {
            currentXOR[j] ^= fileBuffer[j]
        }

        file.Close()
    }

    file, err = os.Open(parityFilename)
    if err != nil {
        t.Errorf("Could not open %d\n", parityFilename)
    }
    fileStat, err := file.Stat()
    if err != nil {
        t.Errorf("Could not check stat of %d\n", parityFilename)
    }

    size := fileStat.Size()
    size -= types.MD5_SIZE // strip off the hash at the end
    fmt.Printf("Going to check size %d bytes\n", size)

    fileBuffer := make([]byte, size)
    _, err = file.ReadAt(fileBuffer, 0)
    check(err)

    for i := 0; i < int(size); i++ {
        if currentXOR[i] != fileBuffer[i] {
            t.Errorf("Found a difference between computed XOR and file XOR at byte %d\n", i)
            break
        }
    }

    file.Close()

    // testingFilename = fmt.Sprintf("./%s", testingFilename)
    // os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, username, diskLocations, configs)
}

func TestRemoveFile(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFile.txt"
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    username := "atoron"

    smallFileData := make([]byte, SMALL_FILE_SIZE)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)
    check(err)

    testingFile.Close()

    // call saveFile
    SaveFile(testingFilename, username, diskLocations, configs)

    // check that the files are there in the first place
    for i := 0; i < TESTING_DISK_COUNT; i++ {
        stripFile := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i], username, testingFilename, i)
        if _, err := os.Stat(stripFile); (os.IsNotExist(err)) { // file does not exist
            t.Errorf("One of the components does not exist")
        }
    }   
    parityfile := fmt.Sprintf("%s/%s/%s_p", diskLocations[len(diskLocations) - 1], username, testingFilename)
    if _, err := os.Stat(parityfile); (os.IsNotExist(err)) { // file does not exist
        t.Errorf("One of the components does not exist (parity)")
    }

    // remove the file
    RemoveFile(testingFilename, username, diskLocations, configs)

    // files should no longer be there
    for i := 0; i < 3; i++ {
        stripFile := fmt.Sprintf("%s/%s/%s_%d", diskLocations[i], username, testingFilename, i)
        if _, err := os.Stat(stripFile); !(os.IsNotExist(err)) { // file does not exist
            t.Errorf("One of the components still exists")
        }
    }   
    parityfile = fmt.Sprintf("%s/%s/%s_p", diskLocations[len(diskLocations) - 1], username, testingFilename)
    if _, err := os.Stat(parityfile); !(os.IsNotExist(err)) { // file does not exist
        t.Errorf("One of the components still exists (parity)")
    }
}

func TestAddFileToLessThanFourLocations(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFile.txt"
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    fmt.Println("Starting TEST: Add file to less than four locations\n")

    username := "atoron"

    smallFileData := make([]byte, SMALL_FILE_SIZE)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)
    check(err)

    testingFile.Close()

    for i := 2; i < TESTING_DISK_COUNT + 1; i++ {
        // call saveFile
        SaveFile(testingFilename, username, diskLocations[0:i], configs)

        // check that the files are there in the first place
        smallerLocations := diskLocations[0:i]
        for j := 0; j < len(smallerLocations) - 1; j++ {
            stripFile := fmt.Sprintf("%s/%s/%s_%d", smallerLocations[j], username, testingFilename, j)
            if _, err := os.Stat(stripFile); (os.IsNotExist(err)) { // file does not exist
                t.Errorf("One of the components does not exist")
            }
        }   
        parityfile := fmt.Sprintf("%s/%s/%s_p", smallerLocations[len(smallerLocations) - 1], username, testingFilename)
        if _, err := os.Stat(parityfile); (os.IsNotExist(err)) { // file does not exist
            t.Errorf("One of the components does not exist (parity)")
        }

        // check that getting the file will yield the correct result
        GetFile(testingFilename, username, smallerLocations, configs)
        cmd := exec.Command("diff", testingFilename, "downloaded-" + testingFilename)

        var out bytes.Buffer
        var stderr bytes.Buffer
        cmd.Stdout = &out
        cmd.Stderr = &stderr
        err = cmd.Run()
        if err != nil {
            fmt.Printf("Diff stderr: %q\n", stderr.String())
            t.Errorf("Diff stderr not empty")
        }

        fmt.Printf("Diff stdout: %q\n", out.String())

        if out.String() != "" {
            t.Errorf("Diff output was not empty")
        }

        // remove the file
        RemoveFile(testingFilename, username, smallerLocations, configs)

        // files should no longer be there
        for j := 0; j < len(smallerLocations) - 1; j++ {
            stripFile := fmt.Sprintf("%s/%s/%s_%d", smallerLocations[j], username, testingFilename, j)
            if _, err := os.Stat(stripFile); !(os.IsNotExist(err)) { // file does not exist
                t.Errorf("One of the components still exists")
            }
        }   
        parityfile = fmt.Sprintf("%s/%s/%s_p", smallerLocations[len(smallerLocations) - 1], username, testingFilename)
        if _, err := os.Stat(parityfile); !(os.IsNotExist(err)) { // file does not exist
            t.Errorf("One of the components still exists (parity)")
        }
    }

    // os.Remove(testingFilename) above cleans up anyway (in TestMain)
}

func TestMultipleUsers(t *testing.T) {
    testingFilename := "testingFile.txt"
    username := "atoron"
    username2 := "atoron2"

    testSavingCorrectnessHelper(t, SMALL_FILE_SIZE, testingFilename, username)
    testSavingCorrectnessHelper(t, SMALL_FILE_SIZE, testingFilename, username2)

    // remove the file
    RemoveFile(testingFilename, username, diskLocations, configs)

    // remove the file
    RemoveFile(testingFilename, username2, diskLocations, configs)

}


// benchmarking test, modifying buffer size each time