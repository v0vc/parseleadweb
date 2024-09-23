package main

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

var tmpl *template.Template

func init() {
	tmpl, _ = template.ParseGlob("templates/*.html")
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/", homeHandler).Methods("GET")
	router.HandleFunc("/upload", UploadHandler).Methods("POST")
	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	err := tmpl.ExecuteTemplate(w, "upload", nil)
	if err != nil {
		log.Println(err)
	}
}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	// Initialize error messages slice
	var serverMessages []string

	// Parse the multipart form, 10 MB max upload size
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		log.Println(err)
	}

	// Retrieve the file from form data
	file, handler, err := r.FormFile("avatar")
	if err != nil {
		if errors.Is(err, http.ErrMissingFile) {
			serverMessages = append(serverMessages, "No file submitted")
		} else {
			serverMessages = append(serverMessages, "Error retrieving the file")
		}

		if len(serverMessages) > 0 {
			er := tmpl.ExecuteTemplate(w, "messages", serverMessages)
			if er != nil {
				log.Println(er)
			}
			return
		}

	}
	defer func(file multipart.File) {
		er := file.Close()
		if er != nil {
			log.Println(er)
		}
	}(file)

	// Generate a unique filename to prevent overwriting and conflicts
	uuid, err := uuid.NewRandom()
	if err != nil {
		serverMessages = append(serverMessages, "Error generating unique identifier")
		er := tmpl.ExecuteTemplate(w, "messages", serverMessages)
		if er != nil {
			log.Println(er)
		}
		return
	}
	filename := uuid.String() + filepath.Ext(handler.Filename) // Append the file extension

	// Create the full path for saving the file
	filePath := filepath.Join("uploads", filename)

	// Save the file to the server
	dst, err := os.Create(filePath)
	if err != nil {
		serverMessages = append(serverMessages, "Error saving the file")
		er := tmpl.ExecuteTemplate(w, "messages", serverMessages)
		if er != nil {
			log.Println(er)
		}
		return
	}
	defer func(dst *os.File) {
		er := dst.Close()
		if er != nil {
			log.Println(er)
		}
	}(dst)
	if _, err = io.Copy(dst, file); err != nil {
		serverMessages = append(serverMessages, "Error saving the file")
		er := tmpl.ExecuteTemplate(w, "messages", serverMessages)
		if er != nil {
			log.Println(er)
		}
		return
	}

	serverMessages = append(serverMessages, fmt.Sprintf("%v:%v", "File uploaded:", filename))
	er := tmpl.ExecuteTemplate(w, "messages", serverMessages)
	if er != nil {
		log.Println(er)
	}
}
