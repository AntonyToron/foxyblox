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
    "strconv"
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

        default:
            fmt.Printf("Error: unsupported command\n")
    }

}