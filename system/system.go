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
    // "os/exec"
    // "bytes"
    // "encoding/binary"
    "foxyblox/database"
    "foxyblox/fileutils"
    "foxyblox/types"
    "encoding/json"
    // "time"
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
    Reads in the file local to this server (configs.txt), and determines the
    manually configured values for this server to run. The contents of the
    config file are as follows:

        sys = LOCALHOST (default), EBS
        dbdisks = ["storage/dbdrive<i>"] (default),
            ["/dev/sda1", "/dev/sda2", etc.] - should be 4 of them

    TODO: another option here would be to just set environment variables instead
    of having to pass it through, not clear which is better
*/
func GetConfigs() *types.Config {
    if !pathExists(types.CONFIG_FILE) {
        // add default values, and return config object
        configFile, err := os.OpenFile(types.CONFIG_FILE, os.O_RDWR | os.O_CREATE, 0755)
        check(err)

        dbDisks := make([]string, types.DBDISK_COUNT + types.DBDISK_PARITY_COUNT)
        for i := 0; i < len(dbDisks); i++ { // parity disk in this already
            dbDisks[i] = fmt.Sprintf(types.LOCALHOST_DBDISK, i)
        }

        diskLocations := make([]string, types.DISK_COUNT + types.NUM_PARITY_DISKS)
        for i := 0; i < len(diskLocations); i++ {
            diskLocations[i] = fmt.Sprintf(types.LOCALHOST_DATADISK, i)
        }

        configs := &types.Config{Sys: types.LOCALHOST, Dbdisks: dbDisks, 
                           Datadisks: diskLocations,
                           DataDiskCount: types.DBDISK_COUNT, 
                           ParityDiskCount: types.DBDISK_PARITY_COUNT}
        obj, err := json.Marshal(configs)
        check(err)

        jsonString := string(obj)
        jsonBytes := []byte(jsonString)

        _, err = configFile.WriteAt(jsonBytes, 0)
        check(err)

        configFile.Close()
    }

    configFile, err := os.OpenFile(types.CONFIG_FILE, os.O_RDWR, 0755)
    check(err)

    fileStat, err := configFile.Stat(); check(err)
    sizeOfFile := fileStat.Size()

    buf := make([]byte, sizeOfFile)
    _, err = configFile.ReadAt(buf, 0)
    check(err)
    // fmt.Printf()

    var configs types.Config
    err = json.Unmarshal(buf, &configs)
    check(err)

    configFile.Close()

    return &configs
}

/*
    Manually sets the configs for the configuration file, overwrites the
    configurations that exist now, if there are any
*/
func SetConfigs(newConfigs *types.Config) {
    // should re-create file and write into it if going to change the configs
    // add default values, and return config object
    configFile, err := os.OpenFile(types.CONFIG_FILE, os.O_RDWR | os.O_CREATE, 0755)
    check(err)

    configs, err := json.Marshal(newConfigs)
    check(err)

    jsonString := string(configs)
    jsonBytes := []byte(jsonString)

    _, err = configFile.WriteAt(jsonBytes, 0)
    check(err)

    configFile.Close()
}


// get better error checking for this: maybe pass back errors to this
// ex: if file name size is too large, should not do anything on server side
// just return an error - so maybe just treat those as special cases and return
// error then, don't do log.Fatal for user malfeason
// disklocations = where to store file (including parity disk, doesn't matter
// to user which disk is treated as the parity disk, preferrably pass in a
// nice format to this function, and parse in another file)
func AddFile(filename string, username string, diskLocations []string) {
    // read configs from a file
    configs := GetConfigs() // TODO: can cache these while running

    // save file to system first
    // should pass in username here, and save into a directory titled <username>
    // in each respective drive

    // maybe better to add to database first, and then later groom the system
    // to make sure that the database doesn't have unecessary entries? either
    // is ok
    fileutils.SaveFile(filename, username, diskLocations, configs)

    // add file to database (diskLocations = location that the file was stored at)
    database.AddFileSpecsToDatabase(filename, username, diskLocations, configs)

    fmt.Printf("Added file %s to system, for user %s\n", filename, username)
}

// returns the location at which the downloaded and assembled file is temporarily stored now
func GetFile(filename string, username string) string {
    // read configs from file
    configs := GetConfigs()

    // first fetch where it is stored in database
    entry := database.GetFileEntry(filename, username, configs)
    if entry == nil {
        fmt.Printf("Did not find the file %s\n", filename)
        return ""
    }

    // trim entry.Disks if not saved on max
    newLength := len(entry.Disks)
    for i := len(entry.Disks) - 1; i > 0; i-- {
        if entry.Disks[i] == "" {
            newLength--
        }
    }
    entry.Disks = entry.Disks[0:newLength]

    // get the actual file from those locations
    downloadedTo := fileutils.GetFile(filename, username, entry.Disks, configs)

    return downloadedTo
}

func DeleteFile(filename string, username string) *types.TreeEntry {
    // read configs from file
    configs := GetConfigs()   

    // delete from database first
    entry := database.DeleteFileEntry(filename, username, configs)
    if entry == nil {
        return nil
    }

    // trim entry.Disks if not saved on max
    newLength := len(entry.Disks)
    for i := len(entry.Disks) - 1; i > 0; i-- {
        if entry.Disks[i] == "" {
            newLength--
        }
    }
    entry.Disks = entry.Disks[0:newLength]

    // now actually remove the saved file (if crash during this, can just
    // occasionally skim the database and remove files that don't exist
    // in the database from the system)
    fileutils.RemoveFile(filename, username, entry.Disks, configs)

    return entry
}

func InitLocal() {
    if !pathExists("./storage") {
        os.Mkdir("storage", types.REGULAR_FILE_MODE)
    }

    for i := 0; i < types.DISK_COUNT + 1; i++ {
        diskFolder := fmt.Sprintf("./storage/drive%d", i)
        if !pathExists(diskFolder) {
            os.Mkdir(diskFolder, types.REGULAR_FILE_MODE)
        }
        dbdiskFolder := fmt.Sprintf("./storage/dbdrive%d", i)
        if !pathExists(dbdiskFolder) {
            os.Mkdir(dbdiskFolder, types.REGULAR_FILE_MODE)
        }
    }
}