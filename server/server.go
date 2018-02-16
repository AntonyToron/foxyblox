/*******************************************************************************
* Author: Antony Toron
* File name: server.go
* Date created: 2/16/18
*
* Description: sample server code, creates a web-app for a wiki (edit and save
* pages in files)
* (source: https://golang.org/doc/articles/wiki/)
*******************************************************************************/

package server

import (
    "html/template" // part of Go standard library, keep HTML in separate file
    "fmt"
    "io/ioutil"
    "net/http"
    "regexp"
    "errors" // for creating a new error
)

type Page struct {
    Title string
    Body  []byte // byte slice (expected by io libraries) - similar to array
}

/*
    Precompute templates on server startup, and cache them so that ParseFiles
    is not called on each page render (this is a global variable)
    
    template.Must = convenience wrapper, panic when passed non-nill error value,
    which makes sense here because we want to panic if the templates can't be
    loaded -> server should exit

    template files are named after base file name

    Note: templates = *Template
*/
var templates = template.Must(template.ParseFiles("server/edit.html", 
    "server/view.html"))

/*
    Will be used for validating paths, otherwise user can specify arbitrary path
    and that isn't caught
*/
var validPath = regexp.MustCompile("^/(edit|save|view)/([a-zA-Z0-9]+)$")
/*
    Page saving and creation (sample run):

    p1 := &Page{Title: "TestPage", Body: []byte("This is a sample Page.")}
    p1.save()
    p2, _ := loadPage("TestPage")
    fmt.Println(string(p2.Body))
*/

/*
    Arguments/Return value:
        *Page = pointer to page
        error = return type, nil (zero-value for pointers, etc.) if no errors

    Description:
        save Page's Body field to a text file, title = filename
*/
func (p *Page) save() error {
    // := is a short assignment statement, implied type
    filename := p.Title + ".txt"
    // 0600 = permissions, read-write for current user only (octal)
    return ioutil.WriteFile(filename, p.Body, 0600)
}

/*
    Arguments/Return value:
        title = title of page
        (*Page, error) = returns pointer to the page + error (if exists)
            Note: error interface = has an Error() function that return string

    Description:
        return page constructed with proper title and body from saved file
*/
func loadPage(title string) (*Page, error) {
    filename := title + ".txt"
    // _ can be used to throw away a return value (ex: body, _)
    body, err := ioutil.ReadFile(filename) 
    if err != nil {
        return nil, err
    }
    return &Page{Title: title, Body: body}, nil
}

/*
    Arguments/Return value:
        response writer, http request
        return string and optional error

    Description:
        validate path and extract page title
*/
func getTitle(w http.ResponseWriter, r *http.Request) (string, error) {
    m := validPath.FindStringSubmatch(r.URL.Path)
    if m == nil {
        // write a 404 not found error to HTTP connection, and return error
        http.NotFound(w, r)
        return "", errors.New("Invalid Page Title")
    }
    return m[2], nil // Title = second subexpression
}

/*
    Server functions
*/

/*
    Arguments/Return value:
        w = response writer - assembles server's HTTP server
            writing to w sends the HTTP to the client
        r = http request
            HTTP request (ex: r.URL.Path contains path of request url
            and trailing [1:] means to create a sub-slice of Path from
            the 1st character to the end, to drop the leading "/")

    Description:
        Function type = http.HandlerFunc  
*/
func rootHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}


/*
    Arguments/Return value:
        w = response writer
        tmpl = name of template
        p = pointer to page to render

    Description:
        Helper function for rendering pages
*/
func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
    // read [tmpl].html, return *template.Template
    // note: path to file is relative where the executable is run
    // note: this is inefficient (we call ParseFiles every time page is
    // rendered, better would be to do this on program initialization and
    // cache the templates)
    // t, err := template.ParseFiles("server/" + tmpl + ".html")
    /*
    if err != nil {
        // send specified HTTP response code via http.Error + error message
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    */

    // execute template (write generated html to response writer), pass in p
    /*
    err = t.Execute(w, p)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
    */

    // template name = template file name
    err := templates.ExecuteTemplate(w, tmpl + ".html", p)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
    }
}


/*
    Arguments/Return value:
        w = response writer - assembles server's HTTP server
            writing to w sends the HTTP to the client
        r = http request

    Description:
        Function type = http.HandlerFunc
        All requests to /view will go here
*/
func viewHandler(w http.ResponseWriter, r *http.Request, title string) {
    // drop the leading "/view/" in the path
    //title := r.URL.Path[len("/view/"):]
    p, err := loadPage(title)
    // redirect to editing page if the page does not exist already
    if err != nil {
        // add status found (302) and location header to the HTTP response
        http.Redirect(w, r, "/edit/" + title, http.StatusFound)
        return
    }

    // basic formatting of an HTML page
    // fmt.Fprintf(w, "<h1>%s</h1><div>%s</div>", p.Title, p.Body)

    renderTemplate(w, "view", p)
}

/*
    Arguments/Return value:
        w = response writer - assembles server's HTTP server
            writing to w sends the HTTP to the client
        r = http request

    Description:
        Function type = http.HandlerFunc
        All requests to /edit will go here
        load the page (or creates an empty Page struct if it doesn't exist)
*/
func editHandler(w http.ResponseWriter, r *http.Request, title string) {
    p, err := loadPage(title) // error ignored here
    if err != nil {
        p = &Page{Title: title}
    }
    // Can do the following, but formatting like this is really ugly
    /*
    fmt.Fprintf(w, "<h1>Editing %s</h1>"+
                   "<form action\"/save/%s\" method=\"POST\">"+
                   "<textarea name=\"body\">%s</textarea><br>"+
                   "<input type=\"submit\" value=\"Save\">"+
                   "</form>",
                   p.Title, p.Title, p.Body)
    */

    renderTemplate(w, "edit", p)
}
/*
    Arguments/Return value:
        w = response writer - assembles server's HTTP server
            writing to w sends the HTTP to the client
        r = http request

    Description:
        Function type = http.HandlerFunc
        All requests to /save will go here
*/
func saveHandler(w http.ResponseWriter, r *http.Request, title string) {
    // Note: title is already validated before this and passed in
    // get form's only field sent (Body), return value = string
    body := r.FormValue("body")
    // store in a new page, note: []byte(body) converts the string to []byte
    p := &Page{Title: title, Body: []byte(body)}
    err := p.save()
    if err != nil { // let user know if error saving
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    http.Redirect(w, r, "/view/" + title, http.StatusFound)
}

/*
    Arguments/Return value:
        fn = function
        return = http.HandlerFunc, called a closure, b/c encloses values
        defined outside of it (i.e. fn enclosed by the closure)
            the http.HandlerFunc takes in a response writer and request

    Description:
        Wrapper for returning handlers, makes error handling easier
*/
func makeHandler(fn func (http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // this part is the closure
        // extract page title from request and call the provided handler 'fn'
        // note: func used as a type here
        m := validPath.FindStringSubmatch(r.URL.Path)
        if m == nil {
            // write a 404 not found error to HTTP connection
            http.NotFound(w, r)
            return
        }
        fn(w, r, m[2]) // Title = second subexpression
    }
}

/*
    To test: run and go to localhost:8080/view/[name of new page] which will
    then allow editing and saving
*/
func Run() {
    // handle all requests to web root with rootHandler function
    // note: :8080/monkeys will still go to rootHandler
    // http.HandleFunc("/", rootHandler)

    http.HandleFunc("/view/", makeHandler(viewHandler))
    http.HandleFunc("/edit/", makeHandler(editHandler))
    http.HandleFunc("/save/", makeHandler(saveHandler))

    // listen on port 8080, on any interface (nil is not important yet)
    // block until program is terminated
    http.ListenAndServe(":8080", nil)
}
