/*******************************************************************************
* Author: Antony Toron
* File name: transaction.go
* Date created: 4/5/18
*
* Description: Defines an interface for starting a transaction, and committing.
* The approach taken is WAL (Write-ahead log), where a transaction is started,
* and adding actions to the transaction writes the final data to the journal,
* so if a crash occurs, the journal is just replayed. This file also handles
* locking the database file in the case that this is necessary (possible for
* parallelization later). Note: an alternative implementation would be to let
* the WAL grow in length, and just actually flush those changes to database when
* it gets too large. This makes committing usually fast, and slow only in some
* instances (when it gets too large). You would have to search the WAL for the
* entry you are searching for first, and then go to the database for it.
*******************************************************************************/

package transaction

import (
    "fmt"
    "os"
    "log"
    "bytes"
    "encoding/binary"
    "path"
    "foxyblox/types"
    // "time"
)

type Action struct {
    Location int64
    OldData []byte
    NewData []byte
}

type Transaction struct {
    DbFilenames []string
    DbParityFilename string
    Actions []*Action
    ActionAmount int
    WAL *os.File
    Configs *types.Config
}

type LogEntry struct {
    Location int64
    Size int64
    NewData []byte // size = SIZE_OF_ENTRY
}

// DbFilenames size = MAX_DISK_COUNT + NUM_PARITY_DISKS
type WALHeader struct {
    Status byte
    EntryCount byte
    DbDiskCount byte
    SizeOfEntry int16
    // NextEntry int64 <- just append, enter to end of the file
    DbFilenames []string // first one should be the disk this corresponds to, and last = parity disk
}

// check error, exit if non-nil
func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

// also needs the parity disk somehow, so can change it in commit
func New(dbFilenames []string, dbParityFilename string, configs *types.Config) *Transaction {
    // estimate that about 5 actions will happen per transaction, can expand
    // the array when it is full
    actions := make([]*Action, types.INIT_ACTION_SIZE)
    t := Transaction{dbFilenames, dbParityFilename, actions, 0, nil, configs}
    return &t
}

func getWALHeader(logFile *os.File) WALHeader {
    var SIZE_OF_WAL_HEADER int16 = types.RAW_WAL_HEADER //+  types.MAX_FILE_NAME_SIZE * int16(len(configs.Dbdisks))
    
    buf := make([]byte, SIZE_OF_WAL_HEADER)
    _, err := logFile.ReadAt(buf, 0)
    check(err)
    diskAmount := buf[2]
    dbdiskBuf := make([]byte, types.MAX_FILE_NAME_SIZE * int16(diskAmount))
    _, err = logFile.ReadAt(dbdiskBuf, int64(SIZE_OF_WAL_HEADER))
    check(err)

    var filenames []string = make([]string, diskAmount)
    for i := 0; i < int(diskAmount); i++ {
        lowerBound := i * types.MAX_PATH_TO_DB // types.RAW_WAL_HEADER + 
        upperBound := lowerBound + types.MAX_PATH_TO_DB
        filenames[i] = string(bytes.Trim(dbdiskBuf[lowerBound: upperBound], "\x00"))
    }

    var sizeOfEntry int16
    b := bytes.NewReader(buf[3:5])
    err = binary.Read(b, binary.LittleEndian, &sizeOfEntry)
    check(err)

    header := WALHeader{buf[0], buf[1], buf[2], sizeOfEntry, filenames}
    return header
}

func headerToBuf(header WALHeader, configs *types.Config) []byte {
    var SIZE_OF_WAL_HEADER int16 = types.RAW_WAL_HEADER +  types.MAX_FILE_NAME_SIZE * int16(len(configs.Dbdisks))
    buf := make([]byte, SIZE_OF_WAL_HEADER)
    buf[0] = header.Status
    buf[1] = header.EntryCount
    buf[2] = header.DbDiskCount

    // now the size of the entry
    bb := new(bytes.Buffer)
    err := binary.Write(bb, binary.LittleEndian, &header.SizeOfEntry)
    check(err)

    sizeOfEntryBuf := bb.Bytes()
    buf[3] = sizeOfEntryBuf[0]
    buf[4] = sizeOfEntryBuf[1]

    for i := 0; i < len(header.DbFilenames); i++ {
        lowerBound := types.RAW_WAL_HEADER +  i * types.MAX_PATH_TO_DB
        if len(header.DbFilenames[i]) > types.MAX_PATH_TO_DB {
            log.Fatal("Length of path to database is too long in headerToBuf")
        }
        for j := 0; j < len(header.DbFilenames[i]); j++ {
            buf[lowerBound + j] = header.DbFilenames[i][j]
        }
    }

    return buf
}

func bufToEntry(buf []byte) LogEntry {
    // first location (8 bytes), then 8 byte size, then new data
    location := bufToPointer(buf[0:types.POINTER_SIZE])
    size := bufToPointer(buf[types.POINTER_SIZE:2*types.POINTER_SIZE])
    
    entry := LogEntry{location, size, buf[2*types.POINTER_SIZE:len(buf)]}
    return entry
}

func bufToPointer(buf []byte) int64 {
    var pointer int64
    b := bytes.NewReader(buf)
    err := binary.Read(b, binary.LittleEndian, &pointer)
    check(err)

    return pointer
}

func HandleActionError(errCode int) {
    if errCode != 0 {
        log.Fatal("Exiting: there was an error in adding action")
    }
}

// error code: 1 = error, 0 = success, likely error if new data is different length than old
// maybe should be locking the WAL file here
// assuming that all of the actions are going to be modifying entries, so just
// going to be adding SIZE_OF_ENTRY data + location of where it goes
// ^ not exactly true, because we are also modifying the header, but header is
// smaller than an entry as of now, and can do it in pieces if it grows to larger
// size than entry
// maybe actually can just do variable size changes, and since reading in sequentially
// in the log file, it's fine anyway (will be more intuitive instead of extending things unecessarily)
func AddAction(t *Transaction, oldData []byte, newData []byte, location int64) int {
    if len(newData) != len(oldData) { //|| len(newData) != SIZE_OF_ENTRY
        return 1
    }

    // add it to the in-memory transaction (since the overall amount of memory)
    // that will be modified by the transaction is not very much
    if (t.ActionAmount == len(t.Actions)) { // expand (increase by two times)
        t.Actions = append(t.Actions, make([]*Action, len(t.Actions))...)
    }

    t.Actions[t.ActionAmount] = &Action{location, oldData, newData}

    // write it to the transaction log
    // lazily create the transaction file here if it does not exist already
    if t.WAL == nil {
        // can configure where this will actually go later (can just be on the
        // same drive that the server is running from, since will likely be on
        // a separate one from the actual drives, and when you restart the
        // server, just check for *_WAL files, then determine what drive the
        // DB is stored on from the filename like <atoron_1_WAL> is on drive 1)
        logName := fmt.Sprintf("%s_WAL", path.Base(t.DbFilenames[0]))
        log, err := os.OpenFile(logName, os.O_CREATE | os.O_RDWR, 0755)
        check(err)
        t.WAL = log
        /* 
            create short header for WAL file:
                1 byte (all 1s when ready/committed) to indicate status of log
                1 byte = amount of actions
                2 bytes = size of an entry in this file
                16 - (previous) extra bytes of 0s just in case need to add something later
        */
        var SIZE_OF_ENTRY int16 = types.MAX_FILE_NAME_SIZE + 2*(types.POINTER_SIZE) + int16(t.Configs.DataDiskCount + 1) * int16(types.MAX_DISK_NAME_SIZE)
        header := WALHeader{0, 0, byte(len(t.Configs.Dbdisks)), SIZE_OF_ENTRY, append(t.DbFilenames, t.DbParityFilename)}
        headerBuf := headerToBuf(header, t.Configs)
    
        var SIZE_OF_WAL_HEADER int16 = types.RAW_WAL_HEADER +  types.MAX_FILE_NAME_SIZE * int16(len(t.Configs.Dbdisks))

        // append remaining zeroes
        headerBuf = append(headerBuf, make([]byte, SIZE_OF_WAL_HEADER - int16(len(headerBuf)))...)
        _, err = log.WriteAt(headerBuf, 0)
        check(err)
    }

    // position of write
    bb := new(bytes.Buffer)
    err := binary.Write(bb, binary.LittleEndian, &location)
    check(err)

    entry := bb.Bytes()

    // size of data
    sizeOfData := int64(len(newData))
    bb = new(bytes.Buffer)
    err = binary.Write(bb, binary.LittleEndian, &sizeOfData)
    check(err)
    sizeOfDataBuf := bb.Bytes()

    entry = append(entry, sizeOfDataBuf...)

    // data
    entry = append(entry, newData...)

    // compute insertion point in log
    header := getWALHeader(t.WAL)

    // append to the end of the file (exactly where we stopped last time)
    fileStat, err := t.WAL.Stat(); check(err)
    sizeOfLog := fileStat.Size()
    _, err = t.WAL.WriteAt(entry, sizeOfLog)
    check(err)

    t.ActionAmount += 1
    header.EntryCount += 1

    // update the header
    newHeader := headerToBuf(header, t.Configs)
    _, err = t.WAL.WriteAt(newHeader, 0)
    check(err)

    return 0
}

// should probably lock the database file now, if concurrency is added into the
// database
// prevent commit or don't do anything when no actions added
func Commit(t *Transaction) {
    // mark the header in COMMIT state
    previousHeader := getWALHeader(t.WAL)
    previousHeader.Status = types.COMMIT
    commitHeader := headerToBuf(previousHeader, t.Configs)
    _, err := t.WAL.WriteAt(commitHeader, 0)
    check(err)

    // flush the COMMIT
    err = t.WAL.Sync()
    check(err)
    // actually start performing the actions (can perform the writes to the
    // parity disk here, as well, because if a system crash happens, won't
    // be able to tell one case from another, so will just re-perform all of the
    // actions, and then compute the parity disk bytes from scratch in the
    // modified areas by XORing all of the drives, because don't know if got
    // through part of the parity disk already or not)
    // can lock the database here
    // TODO: can possibly perform all of these actions in parallel (in separate
    // threads)
    dbFile, err := os.OpenFile(t.DbFilenames[0], os.O_RDWR, 0755)
    check(err)
    dbParityFile, err := os.OpenFile(t.DbParityFilename, os.O_RDWR, 0755)
    check(err)
    for i := 0; i < t.ActionAmount; i++ {
        action := t.Actions[i]
    
        // write to the dbFile
        _, err = dbFile.WriteAt(action.NewData, action.Location)
        check(err)

        // also update the parityFile
        buf := make([]byte, len(action.NewData))
        _, err = dbParityFile.ReadAt(buf, action.Location)
        check(err)

        for j := 0; j < len(buf); j++ {
            buf[j] ^= action.OldData[j] ^ action.NewData[j] // old data ^ new data
        }

        _, err = dbParityFile.WriteAt(buf, action.Location)
        check(err)
    }

    // flush the changes to the database (including parity disk)
    err = dbFile.Sync()
    check(err)
    err = dbParityFile.Sync()
    check(err)

    // delete the log file when certain that changes flushed into db
    t.WAL.Close()
    logName := fmt.Sprintf("%s_WAL", path.Base(t.DbFilenames[0]))
    os.Remove(logName)

    // clean up
    dbFile.Close()
    dbParityFile.Close()
}

// replay all of the actions on the log (write all of the data into the
// original disk, and compute parity as XOR of all three drives from
// scratch)
func ReplayLog(logName string) {
    log, err := os.Open(logName)
    check(err) // maybe should just assume not well-formed and delete in this case

    // check if the header is well-formed, otherwise the log was not
    // committed yet
    header := getWALHeader(log)
    if header.Status != types.COMMIT {
        log.Close()
        os.Remove(logName)
        return
    }

    dbFile, err := os.OpenFile(header.DbFilenames[0], os.O_RDWR, 0755)
    check(err)
    dbParityFile, err := os.OpenFile(header.DbFilenames[len(header.DbFilenames) - 1], os.O_RDWR, 0755)
    check(err)

    var SIZE_OF_WAL_HEADER int16 = types.RAW_WAL_HEADER +  types.MAX_FILE_NAME_SIZE * int16(len(header.DbFilenames))
    var SIZE_OF_ENTRY int = int(header.SizeOfEntry)
    currentPosition := int64(SIZE_OF_WAL_HEADER)
    for i := 0; i < int(header.EntryCount); i++ {
        // read in the location and size of the next component
        buf := make([]byte, 2*types.POINTER_SIZE)
        _, err = log.ReadAt(buf, currentPosition)
        check(err)
        location := bufToPointer(buf[0:types.POINTER_SIZE])
        size := bufToPointer(buf[types.POINTER_SIZE:2*types.POINTER_SIZE])
        currentPosition += 2*types.POINTER_SIZE

        // read in the actual data
        buf = make([]byte, size)
        _, err = log.ReadAt(buf, currentPosition)
        check(err)

        entry := LogEntry{location, size, buf}
        currentPosition += size
        
        // write to database file
        _, err = dbFile.WriteAt(entry.NewData, entry.Location)
        check(err)

        // recompute parity disk at this location
        parityBuf := entry.NewData
        for j := 1; j < len(header.DbFilenames); j++ {
            // filename = path in this case
            otherDb, err := os.Open(header.DbFilenames[j])
            check(err)

            otherDbBuf := make([]byte, SIZE_OF_ENTRY)
            fileStat, err := otherDb.Stat(); check(err)
            sizeOfDb := fileStat.Size()
            if entry.Location + int64(SIZE_OF_ENTRY) < sizeOfDb {
                // if read was out of bounds of the file, then must have been in the
                // middle of resizing the databases, so should just assume it to be 0

                // this case is fine though
                _, err = otherDb.ReadAt(otherDbBuf, entry.Location)
                check(err)
            }

            for k := 0; k < SIZE_OF_ENTRY; k++ {
                parityBuf[k] ^= otherDbBuf[k] 
            }
        }

        // write it to the parity disk
        _, err = dbParityFile.WriteAt(parityBuf, entry.Location)
        check(err)
    }

    // flush the re-done changes to the database (including parity disk)
    err = dbFile.Sync()
    check(err)
    err = dbParityFile.Sync()
    check(err)

    // invalidate the log first (write 0 to the commit status bit)
    // ^ don't think that needs to be done, b/c deleting a file is relatively atomic

    // delete the log file when certain that changes flushed into db
    log.Close()
    os.Remove(logName)

    // clean up
    dbFile.Close()
    dbParityFile.Close()
    for i := 1; i < len(header.DbFilenames); i++ {
        // filename = path in this case
        otherDb, err := os.Open(header.DbFilenames[i])
        check(err)

        otherDb.Close()
    }
}