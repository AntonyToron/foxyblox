/*******************************************************************************
* Author: Antony Toron
* File name: server.go
* Date created: 5/4/18
*
* Description: runs a server for uploading files
*******************************************************************************/

package server

import (
    "html/template" // part of Go standard library, keep HTML in separate file
    "fmt"
    "net/http"
    "log"
    "time"
    "crypto/md5"
    "io"
    "strconv"
    "os"
)

func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

// upload logic
func upload(w http.ResponseWriter, r *http.Request) {
    fmt.Println("method:", r.Method)
    if r.Method == "GET" {
        fmt.Println("Got into the GET method")
        crutime := time.Now().Unix()
        h := md5.New()
        io.WriteString(h, strconv.FormatInt(crutime, 10))
        token := fmt.Sprintf("%x", h.Sum(nil))

        t, err := template.ParseFiles("./server/upload.gtpl")
        check(err)
        t.Execute(w, token)
    } else {
        fmt.Println("Got into the POST method")

        // argument = max memory, parses the form and can get the components
        r.ParseMultipartForm(32 << 20)

        // get file "handle from" so that the file can be saved, has Filename + MIME header
        file, handler, err := r.FormFile("uploadfile")
        if err != nil {
            fmt.Println(err)
            return
        }
        defer file.Close()
        fmt.Fprintf(w, "%v", handler.Header)
        f, err := os.OpenFile("./server/uploaded/" + handler.Filename, os.O_WRONLY | os.O_CREATE, 0755)
        if err != nil {
            fmt.Println(err)
            return
        }
        defer f.Close()

        io.Copy(f, file) // saves the file to the local file f
    }
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hello\n")
}

func Run() {
    http.HandleFunc("/", rootHandler)
    http.HandleFunc("/upload/", upload) // note: if you put / at the end here (/upload/), then
    // the form should be submitted to /upload/ too, not /upload

    // listen on port 8080, on any interface (nil is not important yet)
    // block until program is terminated
    http.ListenAndServe(":8080", nil)
}


