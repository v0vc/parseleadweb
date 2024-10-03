package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xuri/excelize/v2"
	"golang.org/x/net/html"
)

const (
	dbFile  = "./pdb.sqlite"
	sqlite3 = "sqlite3"
	sheet1  = "Mapping"
	sheet2  = "Duplicate_excel"
	sheet3  = "Duplicate_admin"
	del1    = "delete from input1;"
	del2    = "delete from input2;"
	seq1    = "update sqlite_sequence set seq=0 where name='input1';"
	seq2    = "update sqlite_sequence set seq=0 where name='input2';"
	count1  = "select count(id) from input1;"
	count2  = "select count(id) from input2;"
	host    = "localhost:8080"
)

var tmpl *template.Template

func init() {
	tmpl, _ = template.ParseGlob("templates/*.html")
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/", homeHandler).Methods("GET")
	router.HandleFunc("/upload", UploadHandler).Methods("POST")
	router.HandleFunc("/download", DownloadHandler).Methods("GET")
	log.Println("Server starting...")
	log.Fatal(http.ListenAndServe(":8080", router))
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	err := tmpl.ExecuteTemplate(w, "upload", nil)
	if err != nil {
		log.Println(err)
	}
	// очищаем таблицы
	db, err := sql.Open(sqlite3, fmt.Sprintf("file:%v?_foreign_keys=false&cache=shared&mode=rw", dbFile))
	if err != nil {
		log.Println(err)
	}
	defer func(db *sql.DB) {
		er := db.Close()
		if er != nil {
			log.Println(er)
		}
	}(db)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.Println(err)
	}
	execCommand(ctx, tx, del1)
	fmt.Println("first table cleared OK")
	execCommand(ctx, tx, seq1)
	fmt.Println("first sequences cleared OK")
	execCommand(ctx, tx, del2)
	fmt.Println("second table cleared OK")
	execCommand(ctx, tx, seq2)
	fmt.Println("second sequences cleared OK")

	err = tx.Commit()
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println("database is cleared OK")
	}
}

func DownloadHandler(w http.ResponseWriter, r *http.Request) {
	// get the file name to download from url
	name := r.URL.Query().Get("name")

	// join to get the full file path
	directory := filepath.Join("uploads", name)
	// open file (check if exists)
	_, err := os.Open(directory)
	if err != nil {
		er := tmpl.ExecuteTemplate(w, "message", []string{"File not found on server!"})
		if er != nil {
			log.Println(er)
		}
		return
	}

	w.Header().Set("Content-Type", "application/vnd.ms-excel")
	// force a download with the content- disposition field
	w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(directory))

	// serve file out.
	http.ServeFile(w, r, directory)
}

func UploadHandler(w http.ResponseWriter, r *http.Request) {
	// Initialize error messages slice
	var serverMessages []string

	// Parse the multipart form, 10 MB max upload size
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		drawMessage(serverMessages, w, "10 MB max upload size", "message")
		return
	}

	// Retrieve the file from form data
	file, handler, err := r.FormFile("avatar")
	if err != nil {
		if errors.Is(err, http.ErrMissingFile) {
			drawMessage(serverMessages, w, "No file submitted", "message")
			return
		} else {
			drawMessage(serverMessages, w, "Error retrieving the file", "message")
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
	rnd, err := uuid.NewRandom()
	if err != nil {
		drawMessage(serverMessages, w, "Error generating unique identifier", "message")
		return
	}
	filename := rnd.String() + filepath.Ext(handler.Filename) // Append the file extension

	// Create the full path for saving the file
	filePath := filepath.Join("uploads", filename)

	// Save the file to the server
	dst, err := os.Create(filePath)
	if err != nil {
		drawMessage(serverMessages, w, "Error saving the file", "message")
		return
	}
	defer func(dst *os.File) {
		er := dst.Close()
		if er != nil {
			log.Println(er)
		}
	}(dst)

	if _, err = io.Copy(dst, file); err != nil {
		drawMessage(serverMessages, w, "Error saving the file", "message")
		return
	}
	resLink := fmt.Sprintf("http://%v/download?name=%v", host, filename)
	drawMessage(serverMessages, w, resLink, "messages")

	db, err := sql.Open(sqlite3, fmt.Sprintf("file:%v?_foreign_keys=false&cache=shared&mode=rw", dbFile))
	if err != nil {
		log.Println(err)
	}
	defer func(db *sql.DB) {
		er := db.Close()
		if er != nil {
			log.Println(er)
		}
	}(db)

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.Println(err)
	}

	f, err := excelize.OpenFile(filePath)
	if err != nil {
		// грузим админку
		text, e := os.ReadFile(filePath)
		if e != nil {
			drawMessage(serverMessages, w, e.Error(), "message")
			return
		}
		var table [][]string
		var rowAdm []string

		z := html.NewTokenizer(strings.NewReader(string(text)))
		for z.Token().Data != "html" {
			tt := z.Next()
			if tt == html.StartTagToken {
				t := z.Token()
				if t.Data == "tr" {
					if len(rowAdm) > 0 {
						table = append(table, rowAdm)
						rowAdm = []string{}
					}
				}
				if t.Data == "td" {
					inner := z.Next()
					if inner == html.TextToken {
						rowAdm = append(rowAdm, strings.TrimSpace((string)(z.Text())))
					}
				}
			}
		}
		if len(rowAdm) > 0 {
			table = append(table, rowAdm)
		}

		inp2, er := tx.PrepareContext(ctx, "insert into main.input2(name, active, sorting, moddate, leadid, fio, email, tel, page, utm_source, utm_medium, utm_campaign, utm_content, utm_term) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) on conflict do update set fiotel = fiotel+1;")
		if er != nil {
			log.Println(er)
		}
		defer func(inp2 *sql.Stmt) {
			er = inp2.Close()
			if er != nil {
				log.Println(er)
			}
		}(inp2)

		fmt.Printf("'%d' total rows in admin panel.\n", len(table))

		execCommand(ctx, tx, del2)

		for i, ri := range table {
			if i == 0 {
				continue
			}
			if len(ri) < 12 {
				fmt.Printf("invalid row: '%v'.\n", r)
			}
			tel := spaceStringsBuilder(ri[7])
			fio := strings.TrimSpace(ri[5])

			_, er := inp2.ExecContext(ctx, strings.TrimSpace(ri[0]), strings.TrimSpace(ri[1]), strings.TrimSpace(ri[2]), strings.TrimSpace(ri[3]), strings.TrimSpace(ri[4]), strings.ToLower(fio), strings.TrimSpace(ri[6]), strings.TrimPrefix(tel, "+"), strings.TrimSpace(ri[8]), strings.TrimSpace(ri[9]), strings.TrimSpace(ri[10]), strings.TrimSpace(ri[11]), strings.TrimSpace(ri[12]), strings.TrimSpace(ri[13]))

			if er != nil {
				log.Println(er)
			}
		}
		if er == nil {
			er = tx.Commit()
		} else {
			er = tx.Rollback()
		}

		if er != nil {
			log.Println(er)
		}
	} else {
		// грузим ексель
		defer func() {
			// Close the spreadsheet.
			if er := f.Close(); er != nil {
				fmt.Println(er)
			}
		}()
		// Get value from cell by given worksheet name and cell reference.
		firstSheet := f.WorkBook.Sheets.Sheet[0].Name
		fmt.Printf("'%s' is first sheet of %d sheets.\n", firstSheet, f.SheetCount)
		// Get all the rows in the Sheet1.
		rows, er := f.GetRows(firstSheet)
		if er != nil {
			fmt.Println(er)
			return
		}

		inp1, er := tx.PrepareContext(ctx, "insert into main.input1(nomer, firstdate, fio, email, tel, status, result, comment, isopen, opendate) values (?,?,?,?,?,?,?,?,?,?) on conflict do update set fiotel = fiotel+1;")
		if er != nil {
			log.Println(er)
		}
		defer func(inp1 *sql.Stmt) {
			er = inp1.Close()
			if er != nil {
				log.Println(er)
			}
		}(inp1)

		fmt.Printf("'%d' total rows in excel file.\n", len(rows))

		execCommand(ctx, tx, del1)
		for i, row := range rows {
			if i == 0 {
				continue
			}
			//fmt.Printf("processing '%d' row\n", i)

			celLen := len(row)
			if celLen < 2 {
				fmt.Printf("'%d' invalid row, skipped\n", i+1)
				continue
			}
			fio := strings.TrimSpace(row[2])
			switch {
			case celLen == 10:
				_, err = inp1.ExecContext(ctx, strings.TrimSpace(row[0]), strings.TrimSpace(row[1]), strings.ToLower(fio), strings.TrimSpace(row[3]), strings.TrimSpace(row[4]), strings.TrimSpace(row[5]), strings.TrimSpace(row[6]), strings.TrimSpace(row[7]), strings.TrimSpace(row[8]), strings.TrimSpace(row[9]))
			case celLen == 9:
				_, err = inp1.ExecContext(ctx, strings.TrimSpace(row[0]), strings.TrimSpace(row[1]), strings.ToLower(fio), strings.TrimSpace(row[3]), strings.TrimSpace(row[4]), strings.TrimSpace(row[5]), strings.TrimSpace(row[6]), strings.TrimSpace(row[7]), strings.TrimSpace(row[8]), "")
			case celLen == 8:
				_, err = inp1.ExecContext(ctx, strings.TrimSpace(row[0]), strings.TrimSpace(row[1]), strings.ToLower(fio), strings.TrimSpace(row[3]), strings.TrimSpace(row[4]), strings.TrimSpace(row[5]), strings.TrimSpace(row[6]), strings.TrimSpace(row[7]), "", "")
			case celLen == 7:
				_, err = inp1.ExecContext(ctx, strings.TrimSpace(row[0]), strings.TrimSpace(row[1]), strings.ToLower(fio), strings.TrimSpace(row[3]), strings.TrimSpace(row[4]), strings.TrimSpace(row[5]), strings.TrimSpace(row[6]), "", "", "")
			case celLen == 6:
				_, err = inp1.ExecContext(ctx, strings.TrimSpace(row[0]), strings.TrimSpace(row[1]), strings.ToLower(fio), strings.TrimSpace(row[3]), strings.TrimSpace(row[4]), strings.TrimSpace(row[5]), "", "", "", "")
			case celLen == 5:
				_, err = inp1.ExecContext(ctx, strings.TrimSpace(row[0]), strings.TrimSpace(row[1]), strings.ToLower(fio), strings.TrimSpace(row[3]), strings.TrimSpace(row[4]), "", "", "", "", "")
			default:
				fmt.Printf("check row '%d', skipped\n", i+1)
			}
			if err != nil {
				log.Println(err)
			}
		}
		if err == nil {
			er = tx.Commit()
		} else {
			er = tx.Rollback()
		}

		if er != nil {
			log.Println(er)
		}
	}

	txr, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.Println(err)
	}

	// чекаем заполненность таблиц
	inpCount1 := execCommandRes(ctx, txr, count1)
	inpCount2 := execCommandRes(ctx, txr, count2)

	if inpCount1 > 0 && inpCount2 > 0 {
		drawMessage(serverMessages, w, "Generate report.....", "message")
		resultFile := excelize.NewFile()
		_, err = resultFile.NewSheet(sheet1)
		if err != nil {
			log.Println(err)
		}
		_, err = resultFile.NewSheet(sheet2)
		if err != nil {
			log.Println(err)
		}

		_, err = resultFile.NewSheet(sheet3)
		if err != nil {
			log.Println(err)
		}

		headers1 := []string{"i1.fio", "i1.tel", "i1.email", "i1.firstdate", "i1.status", "i1.result", "i1.comment", "i1.isopen", "i1.opendate", "i1.fiotel", "i2.fio", "i2.tel", "i2.email", "i2.name", "i2.moddate", "i2.page", "i2.utm_source", "i2.utm_medium", "i2.utm_campaign", "i2.utm_content", "i2.utm_term", "i2.fiotel"}
		for i, header := range headers1 {
			err = resultFile.SetCellValue(sheet1, fmt.Sprintf("%s%d", string(rune(65+i)), 1), header)
			if err != nil {
				log.Println(err)
			}
		}

		headers2 := []string{"fio", "tel", "duplicateCount"}
		for i, header := range headers2 {
			err = resultFile.SetCellValue(sheet2, fmt.Sprintf("%s%d", string(rune(65+i)), 1), header)
			if err != nil {
				log.Println(err)
			}
			err = resultFile.SetCellValue(sheet3, fmt.Sprintf("%s%d", string(rune(65+i)), 1), header)
			if err != nil {
				log.Println(err)
			}
		}

		sel1, er := txr.PrepareContext(ctx, "select i1.fio, i1.tel, i1.email, i1.firstdate, i1.status, i1.result, i1.comment, i1.isopen, i1.opendate, i1.fiotel, i2.fio, i2.tel, i2.email, i2.name, i2.moddate, i2.page, i2.utm_source, i2.utm_medium, i2.utm_campaign, i2.utm_content, i2.utm_term, i2.fiotel from input1 i1 inner join input2 i2 on i1.tel = i2.tel;")
		if er != nil {
			log.Println(er)
		}
		defer func(sel1 *sql.Stmt) {
			er = sel1.Close()
			if er != nil {
				log.Println(er)
			}
		}(sel1)

		sel2, er := txr.PrepareContext(ctx, "select fio, tel, MAX(fiotel) as duplicateCount from input1 group by fiotel having duplicateCount <> 0 order by duplicateCount desc;")
		if er != nil {
			log.Println(er)
		}
		defer func(sel2 *sql.Stmt) {
			er = sel2.Close()
			if er != nil {
				log.Println(er)
			}
		}(sel2)

		sel3, er := txr.PrepareContext(ctx, "select fio, tel, MAX(fiotel) as duplicateCount from input2 group by fiotel having duplicateCount <> 0 order by duplicateCount desc;")
		if er != nil {
			log.Println(er)
		}
		defer func(sel3 *sql.Stmt) {
			er = sel3.Close()
			if er != nil {
				log.Println(er)
			}
		}(sel3)

		rows1, er := sel1.QueryContext(ctx)
		if er != nil {
			log.Println(er)
		}
		defer func(rows1 *sql.Rows) {
			er = rows1.Close()
			if er != nil {
				log.Println(er)
			}
		}(rows1)

		var res1 []*Res
		for rows1.Next() {
			var res Res
			if er := rows1.Scan(&res.i1fio, &res.i1tel, &res.i1email, &res.i1firstdate, &res.i1status, &res.i1result, &res.i1comment, &res.i1isopen, &res.i1opendate, &res.i1fiotel, &res.i2fio, &res.i2tel, &res.i2email, &res.i2name, &res.i2moddate, &res.i2page, &res.i2utm_source, &res.i2utm_medium, &res.i2utm_campaign, &res.i2utm_content, &res.i2utm_term, &res.i2fiotel); err != nil {
				log.Println(er)
			}
			res1 = append(res1, &res)
		}

		for i, res := range res1 {
			for j := 0; j < 22; j++ {
				celAdr := fmt.Sprintf("%s%d", string(rune(65+j)), i+2)
				var er error
				switch {
				case j == 0:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1fio)
				case j == 1:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1tel)
				case j == 2:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1email)
				case j == 3:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1firstdate)
				case j == 4:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1status)
				case j == 5:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1result)
				case j == 6:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1comment)
				case j == 7:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1isopen)
				case j == 8:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1opendate)
				case j == 9:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i1fiotel)
				case j == 10:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2fio)
				case j == 11:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2tel)
				case j == 12:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2email)
				case j == 13:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2name)
				case j == 14:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2moddate)
				case j == 15:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2page)
				case j == 16:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2utm_source)
				case j == 17:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2utm_medium)
				case j == 18:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2utm_campaign)
				case j == 19:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2utm_content)
				case j == 20:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2utm_term)
				case j == 21:
					er = resultFile.SetCellValue(sheet1, celAdr, res.i2fiotel)
				}

				if er != nil {
					log.Println(er)
				}
			}
		}

		rows2, er := sel2.QueryContext(ctx)
		if er != nil {
			log.Println(er)
		}
		defer func(rows2 *sql.Rows) {
			er = rows2.Close()
			if er != nil {
				log.Println(er)
			}
		}(rows2)

		var res2 []*Res1
		for rows2.Next() {
			var res Res1
			if er := rows2.Scan(&res.fio, &res.tel, &res.count); er != nil {
				log.Println(er)
			}
			res2 = append(res2, &res)
		}

		for i, res := range res2 {
			for j := 0; j < 3; j++ {
				celAdr := fmt.Sprintf("%s%d", string(rune(65+j)), i+2)
				var er error
				switch {
				case j == 0:
					er = resultFile.SetCellValue(sheet2, celAdr, res.fio)
				case j == 1:
					er = resultFile.SetCellValue(sheet2, celAdr, res.tel)
				case j == 2:
					er = resultFile.SetCellValue(sheet2, celAdr, res.count)
				}

				if er != nil {
					log.Println(er)
				}
			}
		}

		rows3, er := sel3.QueryContext(ctx)
		if er != nil {
			log.Println(er)
		}
		defer func(rows3 *sql.Rows) {
			er = rows3.Close()
			if er != nil {
				log.Println(er)
			}
		}(rows3)

		var res3 []*Res1
		for rows3.Next() {
			var res Res1
			if er := rows3.Scan(&res.fio, &res.tel, &res.count); er != nil {
				log.Println(er)
			}
			res3 = append(res3, &res)
		}

		for i, res := range res3 {
			for j := 0; j < 3; j++ {
				celAdr := fmt.Sprintf("%s%d", string(rune(65+j)), i+2)
				var er error
				switch {
				case j == 0:
					er = resultFile.SetCellValue(sheet3, celAdr, res.fio)
				case j == 1:
					er = resultFile.SetCellValue(sheet3, celAdr, res.tel)
				case j == 2:
					er = resultFile.SetCellValue(sheet3, celAdr, res.count)
				}

				if er != nil {
					log.Println(er)
				}
			}
		}

		er = resultFile.DeleteSheet("Sheet1")
		if er != nil {
			log.Println(er)
		}

		fn := fmt.Sprintf("result-%v.xlsx", time.Now().UTC().Format("20060102150405"))
		fnPath := filepath.Join("uploads", fn)
		if er = resultFile.SaveAs(fnPath); er != nil {
			log.Println(er)
		}
		execCommand(ctx, txr, del1)
		execCommand(ctx, txr, del2)
		er = txr.Commit()
		if er != nil {
			log.Println(er)
		}

		resLink = fmt.Sprintf("http://%v/download?name=%v", host, fn)
		serverMessages = nil
		drawMessage(serverMessages, w, resLink, "messages")

		er = removeGlob("uploads", []string{fnPath, filePath})
		if er != nil {
			log.Println("Error removing files")
		}
	} else {
		err = txr.Rollback()
		if err != nil {
			log.Println(err)
		}
	}
}

func removeGlob(path string, exclude []string) (err error) {
	contents, err := filepath.Glob(filepath.Join(path, "*"))
	if err != nil {
		return
	}
	for _, item := range contents {
		if slices.Contains(exclude, item) {
			continue
		}
		err = os.RemoveAll(item)
		if err != nil {
			return
		}
	}
	return
}

func execCommandRes(ctx context.Context, tx *sql.Tx, command string) int {
	stRows, err := tx.PrepareContext(ctx, command)
	if err != nil {
		log.Println(err)
	}
	defer func(stRows *sql.Stmt) {
		er := stRows.Close()
		if er != nil {
			log.Println(er)
		}
	}(stRows)

	var res int
	err = stRows.QueryRowContext(ctx).Scan(&res)
	if err != nil {
		log.Println(err)
	}
	return res
}

func execCommand(ctx context.Context, tx *sql.Tx, command string) {
	stRows, err := tx.PrepareContext(ctx, command)
	if err != nil {
		log.Println(err)
	}
	defer func(stRows *sql.Stmt) {
		er := stRows.Close()
		if er != nil {
			log.Println(er)
		}
	}(stRows)

	_, err = stRows.ExecContext(ctx)
	if err != nil {
		log.Println(err)
	}
}

func drawMessage(serverMessages []string, w http.ResponseWriter, message string, mesType string) {
	serverMessages = append(serverMessages, message)
	er := tmpl.ExecuteTemplate(w, mesType, serverMessages)
	if er != nil {
		log.Println(er)
	}
}

func spaceStringsBuilder(str string) string {
	var b strings.Builder
	b.Grow(len(str))
	for _, ch := range str {
		if !unicode.IsSpace(ch) {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

type Res struct {
	i1fio          string
	i1tel          string
	i1email        string
	i1firstdate    string
	i1status       string
	i1result       string
	i1comment      string
	i1isopen       string
	i1opendate     string
	i1fiotel       int
	i2fio          string
	i2tel          string
	i2email        string
	i2name         string
	i2moddate      string
	i2page         string
	i2utm_source   string
	i2utm_medium   string
	i2utm_campaign string
	i2utm_content  string
	i2utm_term     string
	i2fiotel       int
}

type Res1 struct {
	fio   string
	tel   string
	count int
}
