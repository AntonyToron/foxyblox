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
    // "bytes"
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

func TestDatabaseCreation(t *testing.T) {

}

