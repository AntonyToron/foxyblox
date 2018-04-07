/*******************************************************************************
* Author: Antony Toron
* File name: system.go
* Date created: 4/7/18
*
* Description: Defines an interface with interacting with the system in general,
* by combining the file utilities and the database.
*******************************************************************************/

package system

import (
    "fmt"
    "os"
    "log"
    // "math"
    "os/exec"
    "bytes"
    "encoding/binary"
    "foxyblox/database"
    "foxyblox/fileutils"
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


// check error, exit if non-nil
func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

func AddFile(filename string, username string) {

}
