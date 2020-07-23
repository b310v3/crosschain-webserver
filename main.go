package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

// Registerinfo struct to store register info
type Registerinfo struct {
	Peerchain   string `json:"peerchain"`
	Peerip      string `json:"peerip"`
	Peeraddress string `json:"peeraddress"`
	Peerenode   string `json:"peerenode"`
}

// Serviceinfo struct to store diecovery info
type Serviceinfo struct {
	TargetChain   string `json:"targetchain"`
	SourceChain   string `json:"sourcechain"`
	SourceAddress string `json:"sourceadd"`
	SourceEnode   string `json:"sourceenode"`
}

type Servicerespone struct {
	Peer1 Registerinfo `json:"peer1"`
	Peer2 Registerinfo `json:"peer2"`
	Peer3 Registerinfo `json:"peer3"`
	Peer4 Registerinfo `json:"peer4"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// Contain find string is inside slide or not
func Contain(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func main() {
	// Register handler
	// Service handlervar
	var ip []string
	var id []int
	var tempip string
	var tempid int
	var tempadd string
	var tempenode string
	var tempchain string
	var peer Servicerespone
	var info Registerinfo
	var ccinfo Serviceinfo
	db, err := sql.Open("mysql", "belove:oc886191@tcp(140.118.109.132:3306)/crosschain")
	defer db.Close()
	failOnError(err, "Failed to connect database")

	conn, err := amqp.Dial("amqp://belove:oc886191@140.118.109.132:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"getupandwork",
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	// Queue for register service
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a register queue")

	// Queue for discovery service
	d, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a discovery queue")

	err = ch.QueueBind(
		q.Name,             // queue name
		"Register_Service", // routing key
		"getupandwork",     // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a register queue")

	err = ch.QueueBind(
		d.Name,              // queue name
		"Discovery_Service", // routing key
		"getupandwork",      // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a discovery queue")

	rmsgs, err := ch.Consume(
		q.Name, // queuepeer
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	dmsgs, err := ch.Consume(
		d.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	// do what the hell
	go func() {
		// Handler for the refister service, may need to insert unique value later
		for rmsg := range rmsgs {
			log.Println("receive register message?")
			err = json.Unmarshal(rmsg.Body, &info)
			failOnError(err, "Failed to decode body to json")

			stmt, err := db.Prepare("INSERT INTO peers(peerchain, peerip, peeraddress, peerenode) VALUES(?, ?, ?, ?)")
			defer stmt.Close()
			failOnError(err, "Failed to create database statement")

			res, err := stmt.Exec(info.Peerchain, info.Peerip, info.Peeraddress, info.Peerenode)
			failOnError(err, "Failed to exessecute statement")
			lastID, err := res.LastInsertId()
			failOnError(err, "Failed to get the last database ID")

			log.Printf("Received a message: %v", lastID)
			// send respone back
			err = ch.Publish(
				"",
				rmsg.ReplyTo,
				false,
				false,
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: rmsg.CorrelationId,
					Body:          []byte("200" + strconv.Itoa(int(lastID))),
				})
			failOnError(err, "Failed to respone")
		}
	}()

	go func() {
		// Handler for the discovery service
		for dmsg := range dmsgs {
			log.Println("receive discovery message?")
			err = json.Unmarshal(dmsg.Body, &ccinfo)
			fmt.Println(ccinfo)
			failOnError(err, "Failed to decode body to json")

			rows, err := db.Query("Select id from peers where peerchain = ?", ccinfo.TargetChain)
			defer rows.Close()
			failOnError(err, "Failed to query the database")
			for rows.Next() {
				err := rows.Scan(&tempid)
				failOnError(err, "Failed to read row")
				id = append(id, tempid)
			}

			rand.Seed(time.Now().UnixNano())
			err = db.QueryRow("Select peerchain, peerip, peeraddress, peerenode from peers where id = ?", id[rand.Intn(len(id))]).Scan(&tempchain, &tempip, &tempadd, &tempenode)
			peer.Peer4 = Registerinfo{Peerchain: tempchain, Peerip: tempip, Peeraddress: tempadd, Peerenode: tempenode}

			id = nil

			rows, err = db.Query("Select id from peers where not peerchain = ? and not peerchain = ?", ccinfo.TargetChain, ccinfo.SourceChain)
			defer rows.Close()
			failOnError(err, "")
			for rows.Next() {
				err := rows.Scan(&tempid)
				failOnError(err, "")
				id = append(id, tempid)
			}
			err = db.QueryRow("Select peerchain, peerip, peeraddress, peerenode from peers where id = ?", id[rand.Intn(len(id))]).Scan(&tempchain, &tempip, &tempadd, &tempenode)
			failOnError(err, "")
			peer.Peer2 = Registerinfo{Peerchain: tempchain, Peerip: tempip, Peeraddress: tempadd, Peerenode: tempenode}
			for {
				err = db.QueryRow("Select peerchain, peerip, peeraddress, peerenode from peers where id = ?", id[rand.Intn(len(id))]).Scan(&tempchain, &tempip, &tempadd, &tempenode)
				failOnError(err, "")
				if Contain(ip, tempip) == false {
					peer.Peer3 = Registerinfo{Peerchain: tempchain, Peerip: tempip, Peeraddress: tempadd, Peerenode: tempenode}
					break
				}
			}
			peer.Peer1 = Registerinfo{Peeraddress: ccinfo.SourceAddress, Peerenode: ccinfo.SourceEnode}
			peerlist, err := json.Marshal(peer)
			failOnError(err, "Failed to convert to json")

			// send back winner peer list to requester via mqtt
			err = ch.Publish(
				"",           // exchange
				dmsg.ReplyTo, // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: dmsg.CorrelationId,
					Body:          []byte(peerlist),
				})
			failOnError(err, "Failed to publish mqtt to peers")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
