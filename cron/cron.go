/*******************************************************************************
* Author: Antony Toron
* File name: cron.go
* Date created: 5/2/18
*
* Description: Defines some functionality that can be called from the main 
* command line starting tool, to run as individual cron tasks.
*******************************************************************************/

package cron

import (
    "fmt"
    "os"
    "log"
    "io/ioutil"
    "foxyblox/types"
    "foxyblox/system"
)

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

/*
    have to do this for each user
*/
func checkDbParityForUser(username string, elog *log.Logger, configs *types.Config) bool {
    // configFile, err := os.OpenFile(configFileName, os.O_RDONLY, 0755)
    dbDisks := configs.Dbdisks

    dbParityFilename := fmt.Sprintf("%s/%s_p", dbDisks[len(dbDisks) - 1], username)
    dbParityFile, err := os.OpenFile(dbParityFilename, os.O_RDWR, 0755)
    check(err)

    fileStat, err := dbParityFile.Stat()
    check(err)

    size := fileStat.Size()


    otherDriveFiles := make([]*os.File, len(dbDisks) - 1)
    for i := 0; i < len(otherDriveFiles); i++ {
        dbFilename := fmt.Sprintf("%s/%s_%d", dbDisks[i], username, i)
        file, err := os.OpenFile(dbFilename, os.O_RDWR, 0755)
        check(err)

        otherDriveFiles[i] = file
    }

    var currentPosition int64 = 0
    buf := make([]byte, types.MAX_BUFFER_SIZE)
    trueParityStrip := make([]byte, types.MAX_BUFFER_SIZE)
    checkParityStrip := make([]byte, types.MAX_BUFFER_SIZE)
    errorFound := false
    for currentPosition != size {
        // check if need to resize the buffers
        if (size - currentPosition) < int64(types.MAX_BUFFER_SIZE) {
            newSize := size - currentPosition

            trueParityStrip = make([]byte, newSize)
            checkParityStrip = make([]byte, newSize)
            buf = make([]byte, newSize)
        } else {
            trueParityStrip = make([]byte, types.MAX_BUFFER_SIZE)
        }

        // true parity strip
        _, err = dbParityFile.ReadAt(trueParityStrip, currentPosition)
        check(err)

        // compute the missing piece by XORing all of the other strips
        for i := 0; i < len(otherDriveFiles); i++ {
            file := otherDriveFiles[i]

            _, err = file.ReadAt(buf, currentPosition)
            check(err)

            for j := 0; j < len(trueParityStrip); j++ {
                checkParityStrip[j] ^= buf[j]
            }
        }

        // check if they are the same
        for i := 0; i < len(checkParityStrip); i++ {
            if checkParityStrip[i] != trueParityStrip[i] {
                elog.Printf("There was an error in the database parity file: %s\n", dbParityFilename)
                errorFound = true

                // fix the parity drive (just overwrite it with true parity)
                // TODO: run fsck or something here, to fix the drive
                _, err = dbParityFile.WriteAt(checkParityStrip, currentPosition)
                check(err)

                break
            }
        }

        // update location
        currentPosition += int64(len(trueParityStrip))
    }

    return errorFound
}

func CheckDbParity(configFileName string) bool {
    configs := system.GetConfigs()

    dbDisks := configs.Dbdisks
    elog := log.New(os.Stderr, "", log.Ldate | log.Ltime | log.Lshortfile) // 0 = no timestamps

    dbParityDiskFolder := dbDisks[len(dbDisks) - 1]

    files, err := ioutil.ReadDir(dbParityDiskFolder)
    check(err)

    foundError := false
    for _, f := range files {
        fmt.Println(f.Name())
        username := f.Name()[0:len(f.Name()) - 2] // chop off the _p part

        errForUser := checkDbParityForUser(username, elog, configs)
        if errForUser == true {
            foundError = errForUser
        }
    }

    return foundError
}