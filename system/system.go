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
*/
func GetConfigs() *Config {
    if !pathExists(CONFIG_FILE) {
        // add default values, and return config object
        configFile, err := os.OpenFile(CONFIG_FILE, os.O_RDWR | os.O_CREATE, 0755)
        check(err)

        dbDisks := make([]string, DBDISK_COUNT + DBDISK_PARITY_COUNT)
        for i := 0; i < len(dbDisks); i++ { // parity disk in this already
            dbDisks[i] = fmt.Sprintf(LOCALHOST_DBDISK, i)
        }

        configs := &Config{Sys: LOCALHOST, Dbdisks: dbDisks, 
                           DataDiskCount: DBDISK_COUNT, 
                           ParityDiskCount: DBDISK_PARITY_COUNT}
        obj, err := json.Marshal(configs)
        check(err)

        jsonString := string(obj)
        jsonBytes := []byte(jsonString)

        _, err = configFile.WriteAt(jsonBytes, 0)
        check(err)

        configFile.Close()
    }

    configFile, err := os.OpenFile(CONFIG_FILE, os.O_RDWR, 0755)
    check(err)

    fileStat, err := configFile.Stat(); check(err)
    sizeOfFile := fileStat.Size()

    buf := make([]byte, sizeOfFile)
    _, err = configFile.ReadAt(buf, 0)
    check(err)

    var configs *Config
    err = json.Unmarshal(buf, configs)
    check(err)

    configFile.Close()

    return configs
}

/*
    Manually sets the configs for the configuration file, overwrites the
    configurations that exist now, if there are any
*/
func SetConfigs(newConfigs *Config) {
    // should re-create file and write into it if going to change the configs
    // add default values, and return config object
    configFile, err := os.OpenFile(CONFIG_FILE, os.O_RDWR | os.O_CREATE, 0755)
    check(err)

    configs, err := json.Marshal(newConfigs)
    check(err)

    jsonString := string(obj)
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

    fileutils.SaveFile(filename, username, diskLocations, configs.Sys)

    // add file to database (diskLocations = location that the file was stored at)
    database.AddFileSpecsToDatabase(filename, username, diskLocations, configs.Sys)
}

func GetFile() {

    // first fetch where it is stored in database

    // get the actual file from those locations
}