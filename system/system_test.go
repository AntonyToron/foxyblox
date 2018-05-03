/*******************************************************************************
* Author: Antony Toron
* File name: system_test.go
* Date created: 2/16/18
*
* Description: tests system
*******************************************************************************/

package system

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
const ROUNDS = 10
const FILE_SIZE_CAP = 4 // 32
const FILE_SIZE_MIN = 3
const NAME_SIZE = 24
const DATABASE_SIZE = 100

// 24
var LARGE_FILE_SIZE int64 = int64(math.Pow(2, float64(18))) //int64(math.Pow(2, float64(30))) // 1 GB
var configs *types.Config
var diskLocations []string
var letterRunes = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$^&()_+[]{}")

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
        diskLocations[i] = fmt.Sprintf(types.LOCALHOST_DATADISK, i)
    }

    configs = &types.Config{Sys: types.LOCALHOST, Dbdisks: dbDisks,
                       Datadisks: diskLocations,
                       DataDiskCount: TESTING_DISK_COUNT, 
                       ParityDiskCount: 1}

    // clean up before tests
    generalCleanup()

    initializeDatabaseStructureLocal()

    retCode := m.Run()

    fmt.Println("Finished tests")

    // clean up
    generalCleanup()

    os.Exit(retCode)
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

func generalCleanup() {
    cmd := exec.Command("rm", "-rf", types.CONFIG_FILE, "./storage", "./downloaded*", "./testing*")

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

func TestBasicCorrectness(t *testing.T) {
    initializeDatabaseStructureLocal()

    // don't need to set configs here b/c default = local

    testingFilename := "testingFile.txt"
    username := "atoron"

    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    fileData := make([]byte, SMALL_FILE_SIZE)
    rand.Read(fileData)

    _, err = testingFile.WriteAt(fileData, 0)
    check(err)
    testingFile.Close()

    AddFile(testingFilename, username, configs.Datadisks)

    downloadedTo := GetFile(testingFilename, username)

    // test differences
    cmd := exec.Command("diff", testingFilename, downloadedTo)

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

    // delete the file, and make sure all parts are deleted
    entry := DeleteFile(testingFilename, username)
    for i := 0; i < len(entry.Disks); i++ {
        if entry.Disks[i] != configs.Datadisks[i] {
            t.Errorf("Error, tree entry did not have correct disk locations")
        }
    }

    removeDatabaseStructureLocal()
}

func TestOverallAddingGettingDeleting(t *testing.T) {
    initializeDatabaseStructureLocal()

    username := "atoron"

    amountOfFiles := 50
    filenames := make([]string, amountOfFiles)
    for i := 0; i < len(filenames); i++ {
        filenames[i] = fmt.Sprintf("testing_%d", i)
    }

    inDatabase := make([]bool, amountOfFiles)
    for i := 0; i < len(inDatabase); i++ {
        inDatabase[i] = false
    }

    // initialize the database somewhat
    added := 0
    for added != (amountOfFiles / 2) {
        num := rand.Intn(amountOfFiles - 1) + 1
        if !inDatabase[num] {
            // generate random data for the file
            // create sample file with random binary data
            testingFile, err := os.Create(filenames[num]) // overwrite existing file if there
            if err != nil {
                t.Errorf("Could not create %d\n", filenames[num])
            }

            fileData := make([]byte, SMALL_FILE_SIZE)
            rand.Read(fileData)

            _, err = testingFile.WriteAt(fileData, 0)
            check(err)
            testingFile.Close()

            AddFile(filenames[num], username, configs.Datadisks)
            inDatabase[num] = true
            added++
        }
    }

    r := 0
    for r != ROUNDS {
        // add one
        num := rand.Intn(amountOfFiles - 1) + 1
        for inDatabase[num] {
            num = rand.Intn(amountOfFiles - 1) + 1
        }
        // generate random data for the file
        // create sample file with random binary data
        testingFile, err := os.Create(filenames[num]) // overwrite existing file if there
        if err != nil {
            t.Errorf("Could not create %d\n", filenames[num])
        }

        fileData := make([]byte, SMALL_FILE_SIZE) // running with a non-small file size will make this test run for a while
        rand.Read(fileData)

        _, err = testingFile.WriteAt(fileData, 0)
        check(err)
        testingFile.Close()

        AddFile(filenames[num], username, configs.Datadisks)
        inDatabase[num] = true

        // get one
        num = rand.Intn(amountOfFiles - 1) + 1
        for !inDatabase[num] {
            num = rand.Intn(amountOfFiles - 1) + 1
        }
        downloadedTo := GetFile(filenames[num], username)
        if downloadedTo == "" {
            t.Errorf("Did not get entry %d at all", num)
            break
        }

        // diff the files
        cmd := exec.Command("diff", filenames[num], downloadedTo)

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

        // remove one
        num = rand.Intn(amountOfFiles - 1) + 1
        for !inDatabase[num] {
            num = rand.Intn(amountOfFiles - 1) + 1
        }
        entry := DeleteFile(filenames[num], username)
        if entry == nil {
            t.Errorf("There was an error in deletion")
            break
        }
        inDatabase[num] = false

        r++
    }
    
    removeDatabaseStructureLocal()
}

func TestAddingToLessThanFourDrives(t *testing.T) {
    initializeDatabaseStructureLocal()

    // don't need to set configs here b/c default = local

    testingFilename := "testingFile.txt"
    username := "atoron"

    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    fileData := make([]byte, SMALL_FILE_SIZE)
    rand.Read(fileData)

    _, err = testingFile.WriteAt(fileData, 0)
    check(err)
    testingFile.Close()

    AddFile(testingFilename, username, configs.Datadisks[0:2])

    downloadedTo := GetFile(testingFilename, username)

    // test differences
    cmd := exec.Command("diff", testingFilename, downloadedTo)

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

    // delete the file, and make sure all parts are deleted
    entry := DeleteFile(testingFilename, username)
    for i := 0; i < len(entry.Disks); i++ {
        if entry.Disks[i] != configs.Datadisks[i] {
            t.Errorf("Error, tree entry did not have correct disk locations")
        }
    }

    removeDatabaseStructureLocal()
}

func TestCorruptingADataDisk(t *testing.T) {
    initializeDatabaseStructureLocal()

    // don't need to set configs here b/c default = local

    testingFilename := "testingFile.txt"
    username := "atoron"

    // create sample file with random binary data
    testingFile, err := os.Create(testingFilename) // overwrite existing file if there
    if err != nil {
        t.Errorf("Could not create %d\n", testingFilename)
    }

    fileData := make([]byte, SMALL_FILE_SIZE)
    rand.Read(fileData)

    _, err = testingFile.WriteAt(fileData, 0)
    check(err)
    testingFile.Close()

    AddFile(testingFilename, username, configs.Datadisks)

    // corrupt one of the files
    fileToCorrupt := fmt.Sprintf("storage/drive0/%s/%s_0", username, testingFilename)
    file, err := os.OpenFile(fileToCorrupt, os.O_RDWR, 0755)
    check(err)

    amountOfErrors := 50
    locationOfErrors := 5

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

    downloadedTo := GetFile(testingFilename, username)

    // test differences
    cmd := exec.Command("diff", testingFilename, downloadedTo)

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

    // delete the file, and make sure all parts are deleted
    entry := DeleteFile(testingFilename, username)
    for i := 0; i < len(entry.Disks); i++ {
        if entry.Disks[i] != configs.Datadisks[i] {
            t.Errorf("Error, tree entry did not have correct disk locations")
        }
    }

    removeDatabaseStructureLocal()
}

// TODO: add hashes on the database file..., this is pretty bad though actually
// because that would require a linear progression through the file... and I
// only modify a few bits every time... technically can store MD5 hash of the
// entry in the entry itself (appended to it), this would be grossly expensive
// but the entries are large right now anyway -> seems like the best option**
// need to check hash on each entry as you go down the tree, though, to ensure
// catch all cases
// need hash of the header as well
// issue might be with computing hash of the root node
func TestCorruptingADatabaseDisk(t *testing.T) {

}

func TestCorruptingParityDiskThenCallCronTask(t *testing.T) {

}

/*
    Benchmarking tests
*/

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

// should the database be prepopulated with values, and get some statistics on
// how slow it is when things are in the database, also is saving a file idempotent
// can make a separate test for saving same file size over and over, and see how
// that goes up the more entries you have in the database
func BenchmarkSavingDifferentFileSizesOnFourDisks(b *testing.B) {
    initializeDatabaseStructureLocal()

    // 2^30 = 1 GB, 2^32 = 4 GB, 2^34 = 16 GB
    diskLocations = make([]string, TESTING_DISK_COUNT + 1)
    for i := 0; i < len(diskLocations); i++ {
        diskLocations[i] = fmt.Sprintf("./storage/drive%d", i)
    }

    /*
        Prepopulate the database with about 50-100 values, just for some average
        number, can be small files
    */
    fmt.Println("in test")
    databaseFiles := make([]string, DATABASE_SIZE)

    for i := 0; i < DATABASE_SIZE; i++ {
        testingFilename := randStringRunes(NAME_SIZE)
        username := "atoron" // all on same user for testing

        fileSize := int64(SMALL_FILE_SIZE)

        databaseFiles[i] = testingFilename

        // create the file, with random data
        createRandomFile(testingFilename, fileSize)

        fmt.Println("finished create file")

        AddFile(testingFilename, username, diskLocations)

        fmt.Println("adding files")
    }
    
    // run the tests
    for i := FILE_SIZE_MIN; i < FILE_SIZE_CAP; i++ {
        testingFilename := randStringRunes(NAME_SIZE)
        username := "atoron" // all on same user for testing

        fileSize := int64(math.Pow(2, float64(i)))

        // create the file, with random data
        createRandomFile(testingFilename, fileSize)

        fmt.Printf("About to run file size %d\n", i)

        b.Run(fmt.Sprintf("SaveOnFour-FileSize=2^%d", i), func(b *testing.B) {
            // test saving it
            for j := 0; j < b.N; j++ {
                // preferably switch up the name of the file every time (but
                // this would increase the database every time...), technically
                // should be reconstructing the tree every time for this test
                // to really be accurate, can just re-enter the same filename
                // and everything multiple times, it is idempotent anyway, but
                // actually it's faster to just add when you already have
                // entry in the database, so this won't be an accurate reading

                AddFile(testingFilename, username, diskLocations)

                // just going to delete the file after, so that the runtimes
                // make sense, at least it'll just be a multiple of 2 basically
                // for all of the, so get an idea of this for different file
                // sizes (note that the file size doesn't matter for the
                // database itself)

                time.Sleep(100 * time.Millisecond)


                DeleteFile(testingFilename, username)

                // since deleted, it won't be "cached" anymore, and save time
                // will be the same across runs
            }
        })
    }

    // clean up
    for i := 0; i < DATABASE_SIZE; i++ {
        os.Remove(databaseFiles[i])
    }

    removeDatabaseStructureLocal()
}

// func BenchmarkSaveSameFileSizeManyTimesOnDb(b *testing.B) {

// }

func TestPlay(t *testing.T) {
    str := randStringRunes(NAME_SIZE)
    fmt.Printf("Runes: %s\n", str)
}