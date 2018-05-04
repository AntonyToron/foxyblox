/*******************************************************************************
* Author: Antony Toron
* File name: client.go
* Date created: 5/4/18
*
* Description: simulates a client uploading a file to the server
*******************************************************************************/

package client

import (
    "bytes"
    "fmt"
    "io"
    "io/ioutil"
    "mime/multipart"
    "net/http"
    "os"
    "time"
    "foxyblox/types"
    "math/rand"
    "math"
    "log"
)

const FILE_SIZE_CAP = 30 // 32
const FILE_SIZE_MIN = 3

func check(err error) {
    if err != nil {
        log.Fatal("Exiting: ", err);
    }
}

func postFile(filename string, targetUrl string) error {
    bodyBuf := &bytes.Buffer{}
    bodyWriter := multipart.NewWriter(bodyBuf)

    // this step is very important
    fileWriter, err := bodyWriter.CreateFormFile("uploadfile", filename)
    if err != nil {
        fmt.Println("error writing to buffer")
        return err
    }

    // open file handle
    fh, err := os.Open(filename)
    if err != nil {
        fmt.Println("error opening file")
        return err
    }
    defer fh.Close()

    //iocopy
    _, err = io.Copy(fileWriter, fh)
    if err != nil {
        return err
    }

    contentType := bodyWriter.FormDataContentType()
    bodyWriter.Close()

    resp, err := http.Post(targetUrl, contentType, bodyBuf)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    _, err = ioutil.ReadAll(resp.Body) //resp_body
    if err != nil {
        return err
    }
    // fmt.Println(resp.Status)
    // fmt.Println(string(resp_body))
    return nil
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

func SendFile(fileSize int64, url string) {
    filename := fmt.Sprintf("random%d.txt", fileSize)

    createRandomFile(filename, fileSize)

    start := time.Now()

    postFile(filename, url)

    t := time.Now()
    duration := t.Sub(start)

    fmt.Printf("Sending file of size %d took ", fileSize)
    fmt.Print(duration)
    fmt.Println("")

    os.Remove(filename)
}

// sample usage
func Run() {
    targetUrl := "http://localhost:8080/upload/"

    for i := FILE_SIZE_MIN; i < FILE_SIZE_CAP; i++ {
        fileSize := int64(math.Pow(2, float64(i)))
        SendFile(fileSize, targetUrl)
    }
}