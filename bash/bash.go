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
    "foxyblox/fileutils"
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

func Run() {
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
            storageType, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(path, "") == 0 || strings.Compare(storageType, "") == 0) {
                fmt.Println("usage: save [path] [storage type]");
                break;
            }

            var t int = 0;
            switch storageType {
            case "localhost":
                t = LOCALHOST;
            case "ebs":
                t = EBS;
            default:
                fmt.Println("not implemented yet");
                break;
            }


            fileutils.SaveFile(path, t);

            fmt.Println("Saved file.")
        } else if (strings.Compare(command, "download") == 0) {
            filename, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(filename, "") == 0) {
                fmt.Println("usage: download [filename]");
                break;
            }

            // download back to current location
            fileutils.GetFile(filename, LOCALHOST)

            fmt.Println("Retreived file.")
        } else if (strings.Compare(command, "remove") == 0) {
            filename, position := nextToken(line)
            line = line[position:]

            if (strings.Compare(filename, "") == 0) {
                fmt.Println("usage: remove [filename]");
                break;
            }

            fileutils.RemoveFile(filename, LOCALHOST)

            fmt.Println("Removed file.")
        } else { // quit
            break;
        }
    }
    
}