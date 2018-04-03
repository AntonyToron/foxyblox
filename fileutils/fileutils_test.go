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
)

const SMALL_FILE_SIZE int = 1024
const BUFFER_SIZE int = 1024
const VERY_SMALL_FILE_SIZE = 6 // currently 1, 3 aren't working perfectly
const REGULAR_FILE_SIZE int = 8192

// 24
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(18))) //int64(math.Pow(2, float64(30))) // 1 GB

func TestMain(m *testing.M) {
    fmt.Println("Setting up for tests")

    rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time
    os.Chdir("../") // go back to home directory

    retCode := m.Run()

    fmt.Println("Finished tests")

    os.Exit(retCode)
}

func testSavingCorrectnessHelper(t *testing.T, size int, testingFilename string) {
    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    fileData := make([]byte, size)
    rand.Read(fileData)

    _, err = testingFile.WriteAt(fileData, 0)

    startTime := time.Now()

    // call saveFile
    SaveFile(testingFilename, LOCALHOST)

    elapsed := time.Since(startTime)
    fmt.Printf("Saving file of size %d took %s\n", size, elapsed)

    // check if the XOR of the components is correct
    currentXOR := make([]byte, size)
    for i := 0; i < 3; i++ {
        filename := fmt.Sprintf("./storage/drive%d/%s_%d", i + 1, testingFilename, i + 1)
        file, err := os.Open(filename)
        if err != nil {
            t.Errorf("Could not open %d\n", filename)
        }
        fileStat, err := file.Stat()
        if err != nil {
            t.Errorf("Could not check stat of %d\n", filename)
        }

        size := fileStat.Size()
        size -= MD5_SIZE // strip off the hash at the end

        fileBuffer := make([]byte, size)
        file.ReadAt(fileBuffer, 0)
        for j := 0; j < int(size); j++ {
            currentXOR[j] ^= fileBuffer[j]
        }
    }

    filename := fmt.Sprintf("./storage/drivep/%s_p", testingFilename)
    file, err := os.Open(filename)
    if err != nil {
        t.Errorf("Could not open %d\n", filename)
    }
    fileStat, err := file.Stat()
    if err != nil {
        t.Errorf("Could not check stat of %d\n", filename)
    }

    fileSize := fileStat.Size()
    fileSize -= MD5_SIZE // strip off the hash at the end

    fileBuffer := make([]byte, fileSize)
    file.ReadAt(fileBuffer, 0)

    for i := 0; i < int(fileSize); i++ {
        if currentXOR[i] != fileBuffer[i] {
            t.Errorf("Found a difference between computed XOR and file XOR at byte %d\n", i)
        }
    }

    testingFilename = fmt.Sprintf("./%s", testingFilename)
    os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, LOCALHOST)
}

func TestSavingCorrectnessVerySmallFile(t *testing.T) {
    testingFilename := "testingFileVerySmall.txt"

    testSavingCorrectnessHelper(t, VERY_SMALL_FILE_SIZE, testingFilename)
}

func TestSavingCorrectnessSmallFile(t *testing.T) {
    // rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time
    // os.Chdir("../") // go back to home directory

    // create sample file with random binary data
    testingFilename := "testingFile.txt"

    testSavingCorrectnessHelper(t, SMALL_FILE_SIZE, testingFilename)
}

func TestSavingCorrectnessLargeFile(t *testing.T) {
    //os.Chdir("../") // go back to home directory

    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"

    testSavingCorrectnessHelper(t, int(LARGE_FILE_SIZE), testingFilename)
}

func TestGettingFile(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileRegular.txt"
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
    SaveFile(testingFilename, LOCALHOST)

    GetFile(testingFilename, LOCALHOST)

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

    testingFilename = fmt.Sprintf("./%s", testingFilename)
    os.Remove(testingFilename)
    testingFilename = fmt.Sprintf("./downloaded-%s", testingFilename)
    os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, LOCALHOST)
}

func testSimulatedDiskCorruptionHelper(t *testing.T, size int, testingFilename string, fileToCorrupt string) {
    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    smallFileData := make([]byte, size)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)

    // call saveFile
    SaveFile(testingFilename, LOCALHOST)

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

    GetFile(testingFilename, LOCALHOST)

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

    testingFilename = fmt.Sprintf("./%s", testingFilename)
    os.Remove(testingFilename)
    testingFilename = fmt.Sprintf("./downloaded-%s", testingFilename)
    os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, LOCALHOST)
}

func TestSimulatedDataDiskCorruption(t *testing.T) {
    fmt.Printf("\n\n\n SIMULATING DISK CORRUPTION \n\n\n")

    // create sample file with random binary data
    testingFilename := "testingFile.txt"
    fileToCorrupt := fmt.Sprintf("./storage/drive1/%s_1", testingFilename)
    testSimulatedDiskCorruptionHelper(t, SMALL_FILE_SIZE, testingFilename, fileToCorrupt)
}

func TestSimulatedDataDiskCorruptionLarge(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    fileToCorrupt := fmt.Sprintf("./storage/drive1/%s_1", testingFilename)
    testSimulatedDiskCorruptionHelper(t, int(LARGE_FILE_SIZE), testingFilename, fileToCorrupt)
}

func TestSimulatedDataDiskCorruptionLargeAndPadding(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    fileToCorrupt := fmt.Sprintf("./storage/drive3/%s_3", testingFilename)
    testSimulatedDiskCorruptionHelper(t, int(LARGE_FILE_SIZE), testingFilename, fileToCorrupt)
}

func TestSimulatedParityDiskCorruption(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    smallFileData := make([]byte, LARGE_FILE_SIZE)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)

    // call saveFile
    SaveFile(testingFilename, LOCALHOST)

    // insert some faulty bits into the file
    file, err := os.OpenFile("./storage/drivep/" + testingFilename + "_p",
                            os.O_RDWR, 0755)
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

    GetFile(testingFilename, LOCALHOST)

    // check that the parity disk is now correct
    // check if the XOR of the components is correct
    currentXOR := make([]byte, LARGE_FILE_SIZE) // / 3
    for i := 0; i < 3; i++ {
        filename := fmt.Sprintf("./storage/drive%d/%s_%d", i + 1, testingFilename, i + 1)
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
        size -= MD5_SIZE // strip off the hash at the end
        fileBuffer := make([]byte, size)
        file.ReadAt(fileBuffer, 0)
        for j := 0; j < int(size); j++ {
            currentXOR[j] ^= fileBuffer[j]
        }
    }

    filename := fmt.Sprintf("./storage/drivep/%s_p", testingFilename)
    file, err = os.Open(filename)
    if err != nil {
        t.Errorf("Could not open %d\n", filename)
    }
    fileStat, err := file.Stat()
    if err != nil {
        t.Errorf("Could not check stat of %d\n", filename)
    }

    size := fileStat.Size()
    size -= MD5_SIZE // strip off the hash at the end
    fmt.Printf("Going to check size %d bytes\n", size)

    fileBuffer := make([]byte, size)
    file.ReadAt(fileBuffer, 0)

    for i := 0; i < int(size); i++ {
        if currentXOR[i] != fileBuffer[i] {
            t.Errorf("Found a difference between computed XOR and file XOR at byte %d\n", i)
            break
        }
    }

    testingFilename = fmt.Sprintf("./%s", testingFilename)
    os.Remove(testingFilename)
    // remove the file
    RemoveFile(testingFilename, LOCALHOST)
}

func TestRemoveFile(t *testing.T) {
    // create sample file with random binary data
    testingFilename := "testingFile.txt"
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    smallFileData := make([]byte, SMALL_FILE_SIZE)
    rand.Read(smallFileData)

    _, err = testingFile.WriteAt(smallFileData, 0)

    // call saveFile
    SaveFile(testingFilename, LOCALHOST)

    // check that the files are there in the first place
    for i := 0; i < 3; i++ {
        stripFile := fmt.Sprintf("./storage/drive%d/%s_%d", i + 1, testingFilename, i + 1)
        if _, err := os.Stat(stripFile); (os.IsNotExist(err)) { // file does not exist
            t.Errorf("One of the components does not exist")
        }
    }   
    parityfile := fmt.Sprintf("./storage/drivep/%s_p", testingFilename)
    if _, err := os.Stat(parityfile); (os.IsNotExist(err)) { // file does not exist
        t.Errorf("One of the components does not exist (parity)")
    }

    // remove the file
    RemoveFile(testingFilename, LOCALHOST)

    // files should no longer be there
    for i := 0; i < 3; i++ {
        stripFile := fmt.Sprintf("./storage/drive%d/%s_%d", i + 1, testingFilename, i + 1)
        if _, err := os.Stat(stripFile); !(os.IsNotExist(err)) { // file does not exist
            t.Errorf("One of the components still exists")
        }
    }   
    parityfile = fmt.Sprintf("./storage/drivep/%s_p", testingFilename)
    if _, err := os.Stat(parityfile); !(os.IsNotExist(err)) { // file does not exist
        t.Errorf("One of the components still exists (parity)")
    }
}


// benchmarking test, modifying buffer size each time