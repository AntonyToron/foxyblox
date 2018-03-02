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
)

const SMALL_FILE_SIZE int = 1024
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(24))) //int64(math.Pow(2, float64(30))) // 1 GB

func TestSavingCorrectnessSmallFile(t *testing.T) {
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
    SaveFile(testingFilename, LOCALHOST)

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