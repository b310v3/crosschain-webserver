package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"math/rand"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)


type Serviceinfo struct {
	TargetChain     string `json:"targetchain"`
	SourceChain string `json:"sourcechain"`
	SourceAddress      string `json:"sourceadd"`
	SourceEnode string `json:"sourceenode"`
}

type Rigisterinfo struct {
	Peerchain string `json:"peerchain"`
	Peerip    string `json:"peerip"`
	Peeraddress string `json:"peeraddress"`
	Peerenode string `json:"peerenode"`
}

type Respone struct {
	Status string
	Info   string
	Id     int64
}

// struct for genesis balance
type Balance struct {
	Balance string `json:"balance"`
}

type Config struct {
	HomesteadBlock int `json:"homesteadBlock"`
	ByzantiumBlock int `json:"byzantiumBlock"`
	ConstantinopleBlock int `json:"constantinopleBlock"`
	ChainId int `json:"chainId"`
	Eip150Block int `json:"eip150Block"`
	Eip155Block int `json:"eip155Block"`
	Eip150Hash string `json:"eip150Hash"`
	Eip158Block int `json:"eip158Block"`
	MaxCodeSize int `json:"maxCodeSize"`
	MaxCodeSizeChangeBlock int `json:"homestmaxCodeSizeChangeBlockeadBlock"`
	IsQuorum bool `json:"isQuorum"`
}

// struct for genesis.json
type Genesis struct {
	Alloc map[string]Balance `json:"alloc"`
	Coinbase string `json:"coinbase"`
	Config Config `json:"config"`
	Difficulty string `json:"difficulty"`
	ExtraData string `json:"extraData"`
	GasLimit string `json:"gasLimit"`
	Mixhash string `json:"mixhash"`
	Nonce string `json:"nonce"`
	ParentHash string `json:"parentHash"`
	Timestamp string `json:"timestamp"`
}

type FilesStruct struct {
	G Genesis `json:"g"`
	S []string `json:"s"`
}



// insert the info to the database
func Register(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var info Rigisterinfo
	err := decoder.Decode(&info)
	ErrorCheck(err)

	// connect to database and insert into the peer table
	db, err := sql.Open("mysql", "belove:oc886191@tcp(140.118.109.132:3306)/crosschain")
	defer db.Close()
	ErrorCheck(err)

	stmt, err := db.Prepare("INSERT INTO peers(peerchain, peerip, peeraddress, peerenode) VALUES(?, ?, ?, ?)")
	defer stmt.Close()
	ErrorCheck(err)

	res, err := stmt.Exec(info.Peerchain, info.Peerip, info.Peeraddress, info.Peerenode)
	ErrorCheck(err)
	lastId, err := res.LastInsertId()
	ErrorCheck(err)

	rs := Respone{"ok", "Rigistion complete!", lastId}
	js, jerr := json.Marshal(rs)
	ErrorCheck(jerr)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(js)
}


// return the ip info of the peer to the peer and host a chain
func Service(w http.ResponseWriter, r *http.Request) {

	var ip []string
	var id []int
	var tempip string
	var tempid int
	var tempadd string
	var tempenode string
	var peer []Register

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	decoder := json.NewDecoder(r.Body)
	var info Serviceinfo
	err := decoder.Decode(&info)
	ErrorCheck(err)

	// Connect to the database
	db, err := sql.Open("mysql", "belove:oc886191@tcp(140.118.109.132:3306)/crosschain")
	defer db.Close()
	ErrorCheck(err)

	rows, err := db.Query("Select peerid from peers where peesudo apt-get install xserver-xorg-input-libinputrchain = ?", info.TargetChain)
	defer rows.Close()
	ErrorCheck(err)
	for rows.Next() {
		err := rows.Scan(&tempid)
		ErrorCheck(err)
		append(id, tempid)
	}
	// Random pick a winner for doing the cross chain work and get the ip from the db
	rand.Seed(time.Now().UnixNano())
	err := db.QueryRow("Select peeradd, peerenode from peers where peerid = ?", id[rand.Intn(len(id))]).Scan(&tempadd, &tempenode)
	append(peer, Rigister{peeraddress: tempadd, peerenode: tempenode})

	id = nil

	rows, err := db.Query("Select peerid from peers where not peerchain = ? and not peerchain = ?", info.TargetChain, info.SourceChain)
	defer rows.Close()
	ErrorCheck(err)
	for rows.Next() {
		err := rows.Scan(&tempid)
		ErrorCheck(err)
		append(id, tempid)
	}
	err := db.QueryRow("Select peeradd, peerenode from peers where peerid = ?", id[rand.Intn(len(id))]).Scan(&tempadd, &tempenode)
	ErrorCheck(err)
	append(peer, Rigister{peeraddress: tempadd, peerenode: tempenode})
	for {
		err := db.QueryRow("Select peeradd, peerenode from peers where peerid = ?", id[rand.Intn(len(id))]).Scan(&tempadd, &tempenode)
		ErrorCheck(err)
		if Contain(ip, tempip) != true {
			append(peer, Rigister{peeraddress: tempadd, peerenode: tempenode})
			break
		}
	}
	append(peer, Register{peeraddress: info.SourceiAddress, peerenode: info.SourceEnode}) // Now ip should have 4 different peer info to form a chain

	// Down here need to create a config file for the temporary chain (genesis.json & static-nodes.json)
	genesisfile := CreateGenesis(peer)
	staticfile := CreateStatic(peer)
	files := FilesStruct{genesisfile, staticfile}
	// Here can start build chain (send other peer's ip info) with targetchain peer, sourcechain peer and other two chain peer
	SendMQTT(ip, files) // Two files will combine into one json message. Client need to tranfer them into files.

	// Down here need to replied to the service requester using http respone (postpone for intergration)
	
}

/*
// return the peer info and the transaction
func Validate(w http.ResponseWriter, r *http.Request) {
	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)
	request := buf.String()
}
*/

func SendMQTT(ip []string, files FilesStruct) { // wip, sending two file with in the json format
	// Start a new mqtt connection
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	ErrorCheck(err)
	defer conn.Close()
	ch, err := conn.Channel()
	ErrorCheck(err)
	defer ch.Close()

	err := ch.ExchangeDeclare(
		"getupandwork", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	ErrorCheck(err)
	
	jfiles, err := json.Marshal(files)
	ErrorCheck(err)

	for i := 0; i < 3; i++ {
		err := ch.Publish(
			"getupandwork",         // exchange
			ip[i], // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
					ContentType: "application/json",
					Body:        []byte(jfiles),
			})
		)
	}
}

func Contain(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func ErrorCheck(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Insert the address of the peers
func CreateGenisis(peer []Register) Genesis {
	var peermap  = map[string]Balance
	for i := 0; i < len(peer); i++ {
		peermap[peer[i].Peeraddress]: Balance{"1000000000000000000000000000"}
	}
	data := Genesis{
		Alloc: peermap
		Coinbase: "0x0000000000000000000000000000000000000000",
		Config: Config{
			HomesteadBlock: 0,
			ByzantiumBlock: 0,
			ConstantinopleBlock: 0,
			ChainId: 10,
			Eip150Block: 0,
			Eip155Block: 0,
			Eip150Hash: "0x0000000000000000000000000000000000000000000000000000000000000000",
			Eip158Block: 0,
			MaxCodeSize: 35,
			MaxCodeSizeChangeBlock: 0,
			IsQuorum: true
		}
		Difficulty: "0x0",
		ExtraData: "0x0000000000000000000000000000000000000000000000000000000000000000",
		GasLimit: "0xE0000000",
		Mixhash: "0x00000000000000000000000000000000000000647572616c65787365646c6578",
		Nonce: "0x0",
		ParentHash: "0x0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp: "0x00"
	}
	return data
}

func CreateStatic(peer []Register) []string {
	peerarray := make([]string)
	for i := 0; i < len(peer); i++ {
		append(peerarray, peer[i].Peerenode)
	}
	return peerarray
}

func main() {
	http.Handle("/register", http.HandlerFunc(Register))
	http.Handle("/service", http.HandlerFunc(Service))
	//http.Handle("/validate", http.HandlerFunc(Validate))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
