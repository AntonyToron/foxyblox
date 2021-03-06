/*******************************************************************************
* Author: Antony Toron
* File name: types.go
* Date created: 4/10/18
*
* Description: Standardizes any types that are used across packages.
*******************************************************************************/

package types

import (
    // "fmt"
    "os"
    "crypto/md5"
    // "time"
)

// storageType
const LOCALHOST int = 0;
const EBS int = 1;

const MAX_BUFFER_SIZE int = 8192*8; //1024, empirically 8192*8 seems pretty good, more testing needed for this
const STRIP_COUNT int64 = 3;
const MD5_SIZE = md5.Size;

const DISK_COUNT int = 3;
const MAX_DISK_COUNT uint8 = 3;
const REGULAR_FILE_MODE os.FileMode = 0755; // owner can rwx, but everyone else rx but not w
const RAW_HEADER_SIZE int64 = 2 + 1 + 1 + 8 + 8 + 8
const HEADER_SIZE int64 = 64;
const MAX_FILE_NAME_SIZE int16 = 256 // (in bytes), will only accept ASCII characters for now
const MAX_DISK_NAME_SIZE uint8 = 128
const NUM_PARITY_DISKS  = 1
const POINTER_SIZE = 8

// have to add 1 because parity disk also needs to be stored!
const SIZE_OF_ENTRY = MAX_FILE_NAME_SIZE + 2*(POINTER_SIZE) + int16(MAX_DISK_COUNT + 1) * int16(MAX_DISK_NAME_SIZE) + MD5_SIZE
const ASCII = 255

// entries in header
const HEADER_FILE_SIZE int = 2
const HEADER_DISK_SIZE int = 2
const HEADER_DISK_AMT int = 3 

const BUFFER_SIZE = 8192

// config-related constants
const CONFIG_FILE = "config.txt"
const LOCALHOST_DBDISK = "storage/dbdrive%d"
const LOCALHOST_DATADISK = "storage/drive%d"
const DBDISK_COUNT = 3 // not including parity db disk
const DBDISK_PARITY_COUNT = 1
const RETRY_COUNT = 3

// transaction-related constants
const INIT_ACTION_SIZE = 5
const MAX_PATH_TO_DB = 256
const RAW_WAL_HEADER = 1 + 1 + 1 + 2
// const SIZE_OF_WAL_HEADER = 2 + MAX_FILE_NAME_SIZE * (MAX_DISK_COUNT + NUM_PARITY_DISKS)
const READY = 0x00
const COMMIT = 0xff
const MAX_ENTRIES_TO_BUFFER = 10
const SIZE_OF_WAL_ENTRY = SIZE_OF_ENTRY + POINTER_SIZE + POINTER_SIZE

type TreeEntry struct {
    Filename string
    Left int64
    Right int64
    Disks []string
    Hash []byte // md5 hash of the contents before this in the entry
}

type Config struct {
    Sys int
    Dbdisks []string
    Datadisks []string // slice containing all of the data disks available locally, including those for parity
    DataDiskCount int // default = 3, size of datadisks[] = datadiskcount + paritydiskcount
    ParityDiskCount int // default = 1 (RAID 4)
} 
// note: DataDiskCount defines the maximum amount of data drives you can distribute across (not including parity), can store on less
// should be careful to add + 1 in a lot of places to include that parity disk name in the entries in database, etc.

// ALL TODOs:
/*
    New TODO: make this compatible with adding in a username - add this to
    the file structure of the user, and also allow passing in locations to
    save in

    Here, storageType is not necessary - should do a parse on each one of the
    disk locations and save that component separately (use the regular
    writers and readers for localhost/EBS, but need slightly special 
    handling for the file otherwise [first create the file locally, and then
    have to send it to the other systems if not local])

    TODO: create folder under user's username if doesn't exist!!, in all of
    the disks that exist locally, just so that the local writers don't have
    any issues

    Don't label a drive as "p" now, any drive can be a parity drive, just put
    it at the end of the list by convention in the database, to signal it as
    the parity drive (and the drive right before that one is the one with
    padding) - right now, database only stores DISK_COUNT stuff, should make it
    store the amount in the config file, first of all, and + 1 for the amount
    of parity disks (also from the config file)

*/

/* NOTES

    Check if any of the drives is down - recover appropriately if this is
    the case.

    For now, can just append the 256 byte hash of the file to the end of the
    file and compare it each time I read the component (instead of having
    to compute the XOR every single time, which might not be necessary
    if not reading the whole file -> this might never happen for this
    application though) -> XOR would just tell me something is wrong, still
    don't know where something went wrong, so need to do the hash approach

    the file will be corrupted if the sector went bad, so the hash will solve
    this, just need to make sure to run fsck or something on the offending
    drive

    hash might be really slow though, maybe should hash in parts

    since go hashing is not optimized, can just hash my buffer, keep a temporary
    hashing variable = to that, then move onto the next buffer, hash it, and then
    save the hash of those two together as the new value of the temporary variable,
    and continuously do this until you finish the whole file (do this same
    calculation whenever reading a segment of a file, error out if there is
    some issue in the final comparison)

*/

/*
    Big TODO:
    Even though you can pass in different strings for disk locations on
    different systems, that path will not be available on local machine,
    so using diskLocations[i] as the parameter to the writers isn't exactly
    correct


    Todo:
    transaction file still uses MAX_DISK_COUNT, maybe pass in the configs.DataDiskCount somehow
        - this is an issue, b/c even though the user might decide to only spread across
        2 or 3 places, we still need to store that many entries, because using RAID 4
        - kind of an issue with that too, because if one service goes down entirely,
        then if doing just RAID 4, won't be able to get back the stuff
            -> need to basically downgrade the level of distribution, and almost
            do just mirroring (if 2 places -> 1 of them has the data, the other = mirror)
            if 3 places -> 2 have data, other = parity still, but only on 2 disks

            *** this might work actually, just have to standardize this across the system
            (can do this later, but BIG TODO)**

    also need to fix that max_disk_count thing in database (it uses that sometimes 
    for creating entries)

    fix that and add tests for it in fileutils and also database (base it off how many 
    places the file is stored at, not MAX_FILE_COUNT)

    maybe the thing in configs is only going to be used for defining a maximum, but
    most of the code should be defined solely based on the distribution given
    by user (max is necessary to keep the database well defined***)

    a property = more expensive to do less distributed, but more reliable (less
    probability of one going down), but more distributed = cheaper but less
    reliable the more you distribute across, which is why distribution across
    4 places should be max, and AWS only can do all of the above, but it is
    better to just do distribute across 4, good savings, and decent reliability
*/


/*
    Big TODO: if there is degredation in data on the parity disk, then need to catch this
    too, but preferably shouldn't hash the whole file -> maybe should just infer that there
    is something wrong with the parity disk by just having a daemon go through and check
    that the parity disk is an accurate reflection of the XOR of all of the files -> this is
    probably the most feasible, because the likelihood of the parity disk going down that
    often is very low that we wouldn't be able to catch it, the major points are the database
    disks

*/