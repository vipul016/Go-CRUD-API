package main

import (
	"net/http"
	"encoding/json"
	"database/sql"
	_"github.com/lib/pq"
	"strings"
	"strconv"
	"os"
	"github.com/joho/godotenv"
	"log"

)

var db *sql.DB
var err error
type Detail struct{
	Id int `json:"ID"`
	Name string `json:"name"`
	Age int `json:"age"`
}
var jobChannel chan Detail

func get(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	rows,e := db.Query(`Select id,name,age from users`)

	if e != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
        return
	}
	defer rows.Close()

	var det []Detail

	for rows.Next() {
		var d Detail
		err = rows.Scan(&d.Id,&d.Name,&d.Age)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		det = append(det,d)
	}

	err = rows.Err()
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(det)

}
func getOne(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(r.URL.Path,"/")

	if len(parts) != 3 {
		http.NotFound(w,r)
		return
	}
	id,er := strconv.Atoi(parts[2])
	if er != nil {
        http.Error(w, "Invalid event ID", http.StatusBadRequest)
        return
    }

	q := db.QueryRow(`select id,name,age from users where id = $1`,id)

	var det Detail

	e := q.Scan(&det.Id,&det.Name,&det.Age)
	if e != nil {
		if e == sql.ErrNoRows{
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}
		http.Error(w,http.StatusText(http.StatusInternalServerError),http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type","application/json")
	json.NewEncoder(w).Encode(det)

}
func add(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var det Detail
	if er := json.NewDecoder(r.Body).Decode(&det); er!=nil {
		http.Error(w,http.StatusText(http.StatusBadRequest),http.StatusBadRequest)
		return
	}
	jobChannel <- det
	// e := db.QueryRow(`Insert into users (name,age) values ($1,$2) RETURNING id`,det.Name,det.Age).Scan(&idDet)
	// if e != nil {
	// 	http.Error(w,http.StatusText(http.StatusInternalServerError),http.StatusInternalServerError)
	// 	return
	// }
	// det.Id = idDet

	w.Header().Set("Content-Type","application/json")
	w.WriteHeader(http.StatusCreated)
	// json.NewEncoder(w).Encode(det)
	json.NewEncoder(w).Encode(map[string]string{"status":"request accepted"})

}
func update(w http.ResponseWriter, r *http.Request){
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var det Detail
	path := strings.Split(r.URL.Path,"/")

	if len(path) != 3 {
		http.NotFound(w,r)
		return
	}
	id,er := strconv.Atoi(path[2])
	if er != nil {
        http.Error(w, "Invalid ID", http.StatusBadRequest)
        return
    }

	if er := json.NewDecoder(r.Body).Decode(&det); er!= nil {
		http.Error(w,http.StatusText(http.StatusBadRequest),http.StatusBadRequest)
		return
	}

	res, e := db.Exec(`Update users set name = $1, age = $2 where id = $3`,det.Name,det.Age,id)
	if e != nil {
		if e == sql.ErrNoRows{
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}
		http.Error(w,http.StatusText(http.StatusInternalServerError),http.StatusInternalServerError)
		return
	}

	ro,_ := res.RowsAffected()
	if ro == 0 {
		http.Error(w,"Id not found",http.StatusBadRequest)
		return
	}
	det.Id = id

	w.Header().Set("Content-Type","application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(det)
}
func delete(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := strings.Split(r.URL.Path,"/")

	id,er := strconv.Atoi(path[2])
	if er != nil {
		http.Error(w,"Invaild ID",http.StatusBadRequest)
		return
	}

	res,e := db.Exec(`Delete from users where id = $1`,id)
	if e != nil {
		if e == sql.ErrNoRows{
			http.Error(w, "User not found", http.StatusNotFound)
			return
		}
		http.Error(w,http.StatusText(http.StatusInternalServerError),http.StatusInternalServerError)
		return
	}
	ro,_ := res.RowsAffected()
	if ro == 0 {
		if ro == 0 {
		http.Error(w,"Id not found",http.StatusBadRequest)
		return
	}
	}
	w.Header().Set("Content-Type","application/json")
	w.WriteHeader(http.StatusNoContent)

}
func worker(id int){
	for job := range jobChannel {
		_,err = db.Exec(`Insert into users (name,age) values ($1,$2)`,job.Name,job.Age)

		if err != nil {
		log.Printf("Worker %d : Error :%v\n",id,err)
		}
	}
	
}
func main(){

	err := godotenv.Load()
	if err != nil {
		panic("Error loading .env file")
	}
	sqlConn := os.Getenv("POSTGRES_CONN")
	db, err = sql.Open("postgres", sqlConn)
	defer db.Close()
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
	jobChannel = make(chan Detail, 100)
	const numWorkers = 5
	for w:= 1; w<= numWorkers;w++ {
		go worker(w)
	}

	http.HandleFunc("/get",get)
	http.HandleFunc("/get/",getOne)
	http.HandleFunc("/add",add)
	http.HandleFunc("/update/",update)
	http.HandleFunc("/delete/",delete)
	err = http.ListenAndServe(":8080",nil)

	if err != nil {
		panic(err)
	}

}