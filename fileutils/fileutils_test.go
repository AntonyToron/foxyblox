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
const REGULAR_FILE_SIZE int = 8192

// 24
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(24))) //int64(math.Pow(2, float64(30))) // 1 GB

func TestSavingCorrectnessSmallFile(t *testing.T) {
    rand.Seed(time.Now().UTC().UnixNano()) // necessary to seed differently almost every time
    os.Chdir("../") // go back to home directory

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

    // check if the XOR of the components is correct
    currentXOR := make([]byte, SMALL_FILE_SIZE)
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

    size := fileStat.Size()
    size -= MD5_SIZE // strip off the hash at the end

    fileBuffer := make([]byte, size)
    file.ReadAt(fileBuffer, 0)

    for i := 0; i < int(size); i++ {
        if currentXOR[i] != fileBuffer[i] {
            t.Errorf("Found a difference between computed XOR and file XOR at byte %d\n", i)
        }
    }

    testingFilename = fmt.Sprintf("./%s", testingFilename)
    os.Remove(testingFilename)

}

func TestSavingCorrectnessLargeFile(t *testing.T) {
    //os.Chdir("../") // go back to home directory

    // create sample file with random binary data
    testingFilename := "testingFileLarge.txt"
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    for i := int64(0); i < (LARGE_FILE_SIZE / int64(SMALL_FILE_SIZE)); i++ {
        smallData := make([]byte, SMALL_FILE_SIZE)
        rand.Read(smallData)

        _, err = testingFile.WriteAt(smallData, i * int64(SMALL_FILE_SIZE))
        if err != nil {
            t.Errorf("Could not write to %d\n", testingFilename)
        }
    }

    // call saveFile
    startTime := time.Now()

    SaveFile(testingFilename, LOCALHOST)

    elapsed := time.Since(startTime)
    fmt.Printf("Save file took %s\n", elapsed)

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

    size := fileStat.Size()
    size -= MD5_SIZE // strip off the hash at the end

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
    cmd.Stdout = &out
    err = cmd.Run()
    check(err)

    fmt.Printf("Diff stdout: %q\n", out.String())

    if out.String() != "" {
        t.Errorf("Diff output was not empty")
    }

    testingFilename = fmt.Sprintf("./%s", testingFilename)
    os.Remove(testingFilename)
    testingFilename = fmt.Sprintf("./downloaded-%s", testingFilename)
    os.Remove(testingFilename)
}

func TestSimulatedDataDiskCorruption(t *testing.T) {
    fmt.Printf("\n\n\n SIMULATING DISK CORRUPTION \n\n\n")

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

    // insert some faulty bits into the file
    file, err := os.OpenFile("./storage/drive1/" + testingFilename + "_1",
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
}

func TestSimulatedDataDiskCorruptionLarge(t *testing.T) {
    fmt.Printf("\n\n\n SIMULATING LARGE FILE DISK CORRUPTION \n\n\n")

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
    file, err := os.OpenFile("./storage/drive1/" + testingFilename + "_1",
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
}

func TestSimulatedDataDiskCorruptionLargeAndPadding(t *testing.T) {
    fmt.Printf("\n\n\n SIMULATING LARGE FILE DISK CORRUPTION \n\n\n")

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
    file, err := os.OpenFile("./storage/drive3/" + testingFilename + "_3",
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
}

func TestSimulatedParityDiskCorruption(t *testing.T) {
    fmt.Printf("\n\n\n SIMULATING PARITY DISK CORRUPTION \n\n\n")

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
}


// benchmarking test, modifying buffer size each time