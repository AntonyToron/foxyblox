/*******************************************************************************
* Author: Antony Toron
* File name: bash.go
* Date created: 2/16/18
*
* Description: command-line version of server, takes in commands to store files
* and also retreive files.
*******************************************************************************/

package bash

import (
    "fmt"
    "strings"
    "bufio"
    "os"
    "log"
    "foxyblox/system"
    "foxyblox/types"
    "foxyblox/cron"
    "foxyblox/server"
    "foxyblox/client"
    "strconv"


    "time"
    "os/exec"
    "math/rand"
    "math"
    "bytes"
    // "runtime"
)

// storageType
const LOCALHOST int = 0;
const EBS int = 1;

func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

func nextToken(line string) (string, int) {
    i := 0
    j := 0
    k := 0
    for i < len(line) && line[i] == ' ' {i++}
    j = i
    for (i < len(line) && line[i] != ' ' && line[i] != '\n') {
        k++
        i++
    }
    for (i < len(line) && line[i] == ' ') {i++}
    token := line[j:j + k]
    return token, i
}

/*
    Allows you to enter commands one by one, only quit when explicitly end
    command line program

    TODO: needs updating, functions are outdated
*/
func RunCmdLine() {
    //running := true
    scanner := bufio.NewScanner(os.Stdin)
    for scanner.Scan() {
        fmt.Println("Enter prompt: ")
        line := scanner.Text()

        // get command
        command, position := nextToken(line)
        line = line[position:]
        //fmt.Println(command); // note: semicolons are fine in go

        // save a file to storage
        if (strings.Compare(command, "save") == 0) {
            path, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(path, "") == 0) {
                fmt.Println("usage: save [path] [amountOfLocations] [locations list]");
                break;
            }

            fmt.Println("Saved file.")
        } else if (strings.Compare(command, "download") == 0) {
            filename, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(filename, "") == 0) {
                fmt.Println("usage: download [filename]");
                break;
            }

            fmt.Println("Retreived file.")
        } else if (strings.Compare(command, "remove") == 0) {
            filename, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(filename, "") == 0) {
                fmt.Println("usage: remove [filename]");
                break;
            }

            fmt.Println("Removed file.")
        } else { // quit
            break;
        }
    }
    
}

/*
    Run with command line parameters, and then exit
*/
func Run(args []string) {
    // echo command
    for i := 0; i < len(args); i++ {
        fmt.Printf("%s ", args[i])
    }
    fmt.Printf("\n")

    // check if plausible command
    if len(args) < 2 {
        fmt.Printf("Usage: ./foxyblox [command] [optional arguments]\n")
        fmt.Printf("Example commands: save, get, delete, checkDbParity, initLocal\n")
        fmt.Printf("createConfigFile\n")
        return
    }

    // get os (to know what the executable is called)
    // os := runtime.GOOS

    // switch based on the command given
    switch args[1] {
        case "save":
            targetFilename := args[2]
            username := args[3]
            locationsAmount, err := strconv.Atoi(args[4])
            check(err)

            locations := make([]string, locationsAmount)
            for i := 0; i < locationsAmount; i++ {
                locations[i] = args[5 + i]
            }

            system.AddFile(targetFilename, username, locations)

            fmt.Printf("Added file %s\n", targetFilename)

        case "get":
            targetFilename := args[2]
            username := args[3]

            getLocation := system.GetFile(targetFilename, username)

            fmt.Printf("Retreived file at %s\n", getLocation)

        case "delete":
            targetFilename := args[2]
            username := args[3]

            entry := system.DeleteFile(targetFilename, username)

            fmt.Printf("Deleted file %s\n", entry.Filename)

        case "checkDbParity":
            errorFound := cron.CheckDbParity(types.CONFIG_FILE)

            fmt.Printf("Checked parity, error found: %t", errorFound)

        case "initLocal":
            system.InitLocal()

            fmt.Printf("Created local structure\n")

        case "createConfigFile":
            system.GetConfigs()

            fmt.Printf("Created default config file, can change it now.\n")

        case "test":
            runTest1()
            runTest2()

            fmt.Printf("Ran tests\n")

        case "server":
            server.Run()

            fmt.Printf("Finished running server\n")

        case "client":
            client.Run()

            fmt.Printf("Finished running client\n")

        default:
            fmt.Printf("Error: unsupported command\n")
    }

}


func createRandomFile(filename string, fileSize int64) {
    data := make([]byte, types.MAX_BUFFER_SIZE)

    file, err := os.OpenFile(filename, os.O_RDWR | os.O_CREATE, 0755)
    check(err)

    var currentLocation int64 = 0
    for currentLocation != fileSize {
        // check if need to resize the buffers
        if (fileSize - currentLocation) < int64(types.MAX_BUFFER_SIZE) {
            newSize := fileSize - currentLocation

            data = make([]byte, newSize)
        } else {
            data = make([]byte, types.MAX_BUFFER_SIZE)
        }

        rand.Read(data)

        _, err = file.WriteAt(data, currentLocation) 
        check(err)

        currentLocation += int64(len(data))
    }

    file.Sync()
    file.Close()
}

func randStringRunes(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letterRunes[rand.Intn(len(letterRunes))]
    }
    return string(b)
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

        // fmt.Printf("Diff stdout: %q\n", out.String())
    }
}

// return true if either file or directory exists with given path
func pathExists(path string) (bool) {
    _, err := os.Stat(path)
    return !os.IsNotExist(err)
}

const SMALL_FILE_SIZE int = 1024
const BUFFER_SIZE int = 1024
const VERY_SMALL_FILE_SIZE = 6 // currently 1, 3 aren't working perfectly
const REGULAR_FILE_SIZE int = 8192
const TESTING_DISK_COUNT int = 3
const ROUNDS = 10
const FILE_SIZE_CAP = 32 // 32
const FILE_SIZE_MIN = 3
const NAME_SIZE = 24
const DATABASE_SIZE = 50
const DATABASE_SIZE_CAP = 512
const TEST_AMOUNT = 100
var database_size_cap int = 2048
// 86 to 112
// var letterRunes = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$^&()_+[]{}")
var letterRunes = []rune("VWXYZ[]^_abcdefghijklmnop") // all on drive 1

func runTest1() {
    // test adding to database at different sizes
    // 2^30 = 1 GB, 2^32 = 4 GB, 2^34 = 16 GB
    diskLocations := make([]string, TESTING_DISK_COUNT + 1)
    for i := 0; i < len(diskLocations); i++ {
        diskLocations[i] = fmt.Sprintf("./storage/drive%d", i)
    }

    // run the tests, DATABASE_SIZE_CAP
    for n := 1; n <= database_size_cap; n *= 2 {
        initializeDatabaseStructureLocal()

        testingFilename := randStringRunes(NAME_SIZE)
        username := "atoron" // all on same user for testing

        fileSize := int64(SMALL_FILE_SIZE)

        // create the file, with random data
        createRandomFile(testingFilename, fileSize)

        // prepopulate the database at the specific size
        databaseFiles := make([]string, n)

        for i := 0; i < n; i++ {
            databaseFiles[i] = randStringRunes(NAME_SIZE)
            username := "atoron" // all on same user for testing

            // fileSize := int64(SMALL_FILE_SIZE)

            // // create the file, with random data
            // createRandomFile(databaseFiles[i], fileSize)

            // just rename it
            err := os.Rename(testingFilename, databaseFiles[i])
            check(err)
            testingFilename = databaseFiles[i]

            system.AddFile(databaseFiles[i], username, diskLocations)
        }

        // just rename it
        newName := randStringRunes(NAME_SIZE)
        err := os.Rename(testingFilename, newName)
        check(err)
        testingFilename = newName

        
        testResults := make([]time.Duration, TEST_AMOUNT)

        // test saving it
        for j := 0; j < TEST_AMOUNT; j++ {
            start := time.Now()

            // preferably switch up the name of the file every time (but
            // this would increase the database every time...), technically
            // should be reconstructing the tree every time for this test
            // to really be accurate, can just re-enter the same filename
            // and everything multiple times, it is idempotent anyway, but
            // actually it's faster to just add when you already have
            // entry in the database, so this won't be an accurate reading

            system.AddFile(testingFilename, username, diskLocations)

            // just going to delete the file after, so that the runtimes
            // make sense, at least it'll just be a multiple of 2 basically
            // for all of the, so get an idea of this for different file
            // sizes (note that the file size doesn't matter for the
            // database itself)

            r := system.DeleteFile(testingFilename, username)
            if r == nil {
                return
            }

            t := time.Now()
            elapsed := t.Sub(start)
            testResults[j] = elapsed

            // since deleted, it won't be "cached" anymore, and save time
            // will be the same across runs

            // maybe can rename the file here, so that on the next round it's
            // a little different
            newName := randStringRunes(NAME_SIZE)
            err := os.Rename(testingFilename, newName)
            check(err)
            testingFilename = newName
        }

        // compute the average over the results
        sum := time.Duration(0)
        for i := 0; i < len(testResults); i++ {
            sum += testResults[i]
        }
        average := sum / time.Duration(float64(len(testResults)))

        // print the result
        fmt.Printf("Database size %d, took about ", n)
        fmt.Print(average)
        fmt.Println("")

        err = os.Remove(testingFilename)
        check(err)

        removeDatabaseStructureLocal()
    }

    removeDatabaseStructureLocal()
}

func runTest2() {
    initializeDatabaseStructureLocal()

    // 2^30 = 1 GB, 2^32 = 4 GB, 2^34 = 16 GB
    diskLocations := make([]string, TESTING_DISK_COUNT + 1)
    for i := 0; i < len(diskLocations); i++ {
        diskLocations[i] = fmt.Sprintf("./storage/drive%d", i)
    }

    /*
        Prepopulate the database with about 50-100 values, just for some average
        number, can be small files
    */
    databaseFiles := make([]string, DATABASE_SIZE)

    for i := 0; i < DATABASE_SIZE; i++ {
        testingFilename := randStringRunes(NAME_SIZE)
        username := "atoron" // all on same user for testing

        fileSize := int64(SMALL_FILE_SIZE)

        databaseFiles[i] = testingFilename

        // create the file, with random data
        createRandomFile(testingFilename, fileSize)

        system.AddFile(testingFilename, username, diskLocations)
    }
    
    // run the tests
    for i := FILE_SIZE_MIN; i <= FILE_SIZE_CAP; i++ {
        testingFilename := randStringRunes(NAME_SIZE)
        username := "atoron" // all on same user for testing

        fileSize := int64(math.Pow(2, float64(i)))

        // create the file, with random data
        createRandomFile(testingFilename, fileSize)

        testResults := make([]time.Duration, TEST_AMOUNT)

        // test saving it
        for j := 0; j < TEST_AMOUNT; j++ {
            start := time.Now()
            // preferably switch up the name of the file every time (but
            // this would increase the database every time...), technically
            // should be reconstructing the tree every time for this test
            // to really be accurate, can just re-enter the same filename
            // and everything multiple times, it is idempotent anyway, but
            // actually it's faster to just add when you already have
            // entry in the database, so this won't be an accurate reading

            system.AddFile(testingFilename, username, diskLocations)

            // just going to delete the file after, so that the runtimes
            // make sense, at least it'll just be a multiple of 2 basically
            // for all of the, so get an idea of this for different file
            // sizes (note that the file size doesn't matter for the
            // database itself)

            r := system.DeleteFile(testingFilename, username)
            if r == nil {
                return
            }

            t := time.Now()
            elapsed := t.Sub(start)
            testResults[j] = elapsed

            // since deleted, it won't be "cached" anymore, and save time
            // will be the same across runs

            // maybe can rename the file here, so that on the next round it's
            // a little different
            newName := randStringRunes(NAME_SIZE)
            err := os.Rename(testingFilename, newName)
            check(err)
            testingFilename = newName
        }

        // compute the average over the results
        sum := time.Duration(0)
        for i := 0; i < len(testResults); i++ {
            sum += testResults[i]
        }
        average := sum / time.Duration(float64(len(testResults)))

        // print the result
        fmt.Printf("File size 2^%d, took about ", i)
        fmt.Print(average)
        fmt.Println("")

        err := os.Remove(testingFilename)
        check(err)
    }

    // clean up
    for i := 0; i < DATABASE_SIZE; i++ {
        err := os.Remove(databaseFiles[i])
        check(err)
    }

    removeDatabaseStructureLocal()
}