package main

import (
	"github.com/drone/routes"
	"log"
	"net/http"
	"encoding/json"
	"io/ioutil"
	"io"
	"github.com/mkilling/goejdb"
    "labix.org/v2/mgo/bson"
    "github.com/pelletier/go-toml"
    "os"
    "strconv"
    "net"
	"net/rpc"
)
type Food         struct {
		DrinkAlcohol string `json:"drink_alcohol"`
		Types         string `json:"type"`
	}
type Movie	     struct {
		Movies  []string `json:"movies"`
		TvShows []string `json:"tv_shows"`
	}
type Music struct {
		SpotifyUserId string `json:"spotify_user_id"`
	}
type Flight	struct {
			Seat string `json:"seat"`
		}
type Travel		struct {
		Fl Flight  `json:"flight"`
	}
type Profile struct {
	Country       string `json:"country"`
	Email         string `json:"email"`
	FavoriteColor string `json:"favorite_color"`
	FavoriteSport string `json:"favorite_sport"`
	Fo Food `json:"food"`
	IsSmoking string `json:"is_smoking"`
	Mov Movie`json:"movie"`
	Mus Music `json:"music"`
	Profession string `json:"profession"`
	T   Travel   `json:"travel"`
	Zip string `json:"zip"`
}

type Profiles [5]Profile
//var prof
var P *Profile
var v = []Profile{}
var databaseName string
var portNum int64
var rpc_port_number int64
var replica []interface {}

type Listener int

func (l *Listener) GetLine(line []byte, ack *bool) error {
	log.Println("**** From RPC **** "+string(line))
	jb, err3 := goejdb.Open(databaseName, goejdb.JBOWRITER | goejdb.JBOCREAT)
    if err3 != nil {
        panic(err3)
    }
    coll, _ := jb.CreateColl("contacts", nil)
    coll.SaveBson(line)
    jb.Close()
    log.Printf("\nSaved new item in RPC")
	return nil
}
func (l *Listener) DeleteRPC(line []byte, ack *bool) error {
	log.Println("**** From RPC Delete **** "+string(line))
	jb, err3 := goejdb.Open(databaseName, goejdb.JBOWRITER | goejdb.JBOCREAT)
    if err3 != nil {
        panic(err3)
    }
    coll, _ := jb.CreateColl("contacts", nil)
    res, eer := coll.Update(`{"email" :"`+string(line)+`", "$dropall": true}`)
    if eer != nil {
        panic(eer)
    }
    log.Println(res)
    jb.Close()
    log.Printf("\n Deleted in RPC")
	return nil

}
func rpcTest(){
	addy, err := net.ResolveTCPAddr("tcp", "localhost:"+strconv.FormatInt(rpc_port_number,10))
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		log.Fatal(err)
	}

	listener := new(Listener)
	rpc.Register(listener)
	rpc.Accept(inbound)

}

func main() {
	tomlFile := os.Args[1]
	config, err := toml.LoadFile(tomlFile)
	
	if err != nil {
    	log.Println("Error ", err.Error())
	}else{
		databaseName = config.Get("database.file_name").(string)
		portNum = config.Get("database.port_num").(int64)
		rpc_port_number = config.Get("replication.rpc_server_port_num").(int64)
		replica = config.Get("replication.replica").([]interface {})
		log.Println("Database Name-->"+databaseName)
		log.Println(portNum)
		log.Println(rpc_port_number)
		log.Println("replica-->",replica[0])
		
	}
	
	go rpcTest()
	
	mux := routes.New()
	mux.Get("/profile/:email", GetProfile)
	mux.Post("/profile",CreateProfile)
	mux.Put("/profile/:email",UpdateProfile)
	mux.Del("/profile/:email",DeleteProfile)
	
	http.Handle("/", mux)
	log.Println("Listening...")
	//log.Println("printing P-->"+P.Email)
	http.ListenAndServe(":"+strconv.FormatInt(portNum,10), nil)
	
	
    
}
func DeleteProfile(w http.ResponseWriter, r *http.Request){
	log.Println("in Delete")
	res2B, _ := json.Marshal(v)
	log.Println("before delete json->"+string(res2B))
	params := r.URL.Query()
	email := params.Get(":email")
	
	i:=0
	for i:=0;i<len(v);i++{
		if(v[i].Email==email){
			log.Println("found")
			break
		}
	}
	
	v = append(v[:i], v[i+1:]...)
	res2B, _ = json.Marshal(v)
	log.Println("after delete json->"+string(res2B))
	
	jb, err3 := goejdb.Open(databaseName, goejdb.JBOWRITER | goejdb.JBOCREAT)
    if err3 != nil {
        panic(err3)
    }
    coll, _ := jb.CreateColl("contacts", nil)
    log.Println(`{"email" : "`+email+`" , "$dropall": true}`);
    res, eer := coll.Update(`{"email" :"`+email+`", "$dropall": true}`)
    if eer != nil {
        panic(eer)
    }
    log.Println(res)
    jb.Close()
	log.Println("**** Begin RPC to Delete****")
    client, err := rpc.Dial("tcp",replica[0].(string)[7:len(replica[0].(string))])
	if err != nil {
		log.Fatal(err)
	}

	
		var reply bool
		err = client.Call("Listener.DeleteRPC", []uint8(email), &reply)
		if err != nil {
			log.Fatal(err)
		}
	
	
	w.WriteHeader(http.StatusNoContent)
	

}
func UpdateProfile(w http.ResponseWriter, r *http.Request){
	log.Println("in Update")
	res2B, _ := json.Marshal(v)
	log.Println(string(res2B));
	params := r.URL.Query()
	email := params.Get(":email")
	var prof Profile
    body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
    if err != nil {
        panic(err)
    }
    if err := r.Body.Close(); err != nil {
        panic(err)
    }
    if err := json.Unmarshal(body, &prof); err != nil {
        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(422) // unprocessable entity
        if err := json.NewEncoder(w).Encode(err); err != nil {
            panic(err)
        }
    }
    log.Println("printing Travel-->"+prof.T.Fl.Seat)
    jb, err3 := goejdb.Open(databaseName, goejdb.JBOWRITER | goejdb.JBOCREAT)
    if err3 != nil {
        panic(err3)
    }
    coll, _ := jb.CreateColl("contacts", nil)
    res, _ := coll.Find(`{"email" :` +email+`}`) // Name starts with 'Bru' string
    log.Printf("\n\nRecords found: %d\n", len(res))
    var m map[string]interface{}
    for _, bs := range res {
        
        bson.Unmarshal(bs, &m)
        log.Println(m["travel"].(map[string]interface {})["flight"].(map[string]interface {})["seat"])
    }
    
    for i:=0;i<len(v);i++{
		if(v[i].Email==email){
			log.Println("found")
			log.Println("email in put-->"+v[i].Email)
			if(prof.T.Fl.Seat!=""){
				log.Println("seat before put"+v[i].T.Fl.Seat)
				v[i].T=prof.T
				log.Println("seat after put"+v[i].T.Fl.Seat)
				
				
				
  				  log.Println("wtf-->",prof.T)
  				  m["travel"].(map[string]interface {})["flight"].(map[string]interface {})["seat"]=prof.T.Fl.Seat
  				  log.Println(m["travel"])
  				  
				//res, _ := coll.Update(`{"email" : `+email+`,"$set" :{'travel.flight.seat':"fuck"}}`)
				//log.Printf("\n\nRecords updated: %d\n", res)
				
			}
			if(prof.Email!=""){
				v[i].Email=prof.Email
				m["email"]=prof.Email
			}
			if(prof.Zip!=""){
				v[i].Zip=prof.Zip
				m["zip"]=prof.Zip
			}
			if(prof.Country!=""){
				v[i].Country=prof.Country
				m["country"]=prof.Country
			}
			if(prof.FavoriteColor!=""){
				v[i].FavoriteColor=prof.FavoriteColor
				m["favorite_color"]=prof.FavoriteColor
			}
			if(prof.IsSmoking!=""){
				v[i].IsSmoking=prof.IsSmoking
				m["is_smoking"]=prof.IsSmoking
			}
			if(prof.FavoriteSport!=""){
				v[i].FavoriteSport=prof.FavoriteSport
				
				m["favorite_sport"]=prof.FavoriteSport
			}
			if(prof.Fo.DrinkAlcohol!="" || prof.Fo.Types!=""){
				v[i].Fo=prof.Fo
				m["food"].(map[string]interface {})["drink_alcohol"]=prof.Fo.DrinkAlcohol
				m["food"].(map[string]interface {})["type"]=prof.Fo.Types
				
			}
			if(prof.Mus.SpotifyUserId!=""){
				v[i].Mus=prof.Mus
				m["music"].(map[string]interface {})["spotify_user_id"]=prof.Mus.SpotifyUserId
			}
			if(len(prof.Mov.Movies)>0 || len(prof.Mov.TvShows)>0){
				v[i].Mov=prof.Mov
				m["movie"].(map[string]interface {})["movies"]=prof.Mov.Movies
				m["movie"].(map[string]interface {})["tv_shows"]=prof.Mov.TvShows
			}
			break
		}
	}
	
	bsrec, _ := bson.Marshal(m)
    				coll.SaveBson(bsrec)
    				log.Printf("\nSaved new item")
    				
    log.Println("**** Begin RPC to Update****")
    client, err := rpc.Dial("tcp",replica[0].(string)[7:len(replica[0].(string))])
	if err != nil {
		log.Fatal(err)
	}

	
		var reply bool
		err = client.Call("Listener.GetLine", bsrec, &reply)
		if err != nil {
			log.Fatal(err)
		}
    //newJson:=string(body[:])
    //log.Println("new Json-->"+newJson)
    //coll, _ := jb.CreateColl("contacts", nil)
    //log.Println("update query Json-->"+`{"email" : `+email+`,"$set" : `+newJson+`}`)
    //res, _ := coll.Update(`{"email" : `+email+`,"$set" : `+newJson+`}`)
    //log.Printf("\n\nRecords updated: %d\n", res)

    // Now print the result set records
    

    // Close database
    jb.Close()
	
	//json.NewEncoder(w).Encode(v)
	log.Println("seat after put"+v[0].T.Fl.Seat)
	res2B, _ = json.Marshal(v[0])
	log.Println(string(res2B));
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
    w.WriteHeader(http.StatusNoContent)
	
}
func CreateProfile(w http.ResponseWriter, r *http.Request){
	var prof Profile
	res2B, _ := json.Marshal(v)
	log.Println("p in json->"+string(res2B))
    body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
    if err != nil {
        panic(err)
    }
    if err := r.Body.Close(); err != nil {
        panic(err)
    }
    log.Println("incoming json-->"+string(body[:]))
    log.Println("database name in create-->"+databaseName)
    jb, err3 := goejdb.Open(databaseName, goejdb.JBOWRITER | goejdb.JBOCREAT | goejdb.JBOTRUNC )
    if err3 != nil {
        panic(err3)
    }
    //newJson:=string(body[:])
    coll, _ := jb.CreateColl("contacts", nil)
     
    if err := json.Unmarshal(body, &prof); err != nil {
        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(422) // unprocessable entity
        if err := json.NewEncoder(w).Encode(err); err != nil {
            panic(err)
        }
    }
    test, _ :=json.Marshal(prof)
    //rec := map[string]interface{} {"email" : prof.Email}
    //var rec map[string]Profile
	var dat map[string]interface{}
	if erro := json.Unmarshal([]byte(test), &dat); erro != nil {
        panic(err)
    }
    log.Println(dat)
    //rec[prof.Email]=prof
    bsrec, _ := bson.Marshal(dat)
    coll.SaveBson(bsrec)
    log.Printf("\nSaved item")
    jb.Close()
    
    log.Println("**** Begin RPC to Create****")
    client, err := rpc.Dial("tcp", replica[0].(string)[7:len(replica[0].(string))])
	if err != nil {
		log.Fatal(err)
	}

	
		var reply bool
		err = client.Call("Listener.GetLine", bsrec, &reply)
		if err != nil {
			log.Fatal(err)
		}
    
    v=append(v,prof)
	log.Println("printing P-->"+prof.Email)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
    w.WriteHeader(http.StatusCreated)
    
}
func GetProfile(w http.ResponseWriter, r *http.Request) {
	log.Println("In get")
	res2B, _ := json.Marshal(v)
	log.Println("p in json->"+string(res2B))
	params := r.URL.Query()
	email := params.Get(":email")
	log.Println("printing request email-->"+email)
	
	jb, err3 := goejdb.Open(databaseName, goejdb.JBOWRITER | goejdb.JBOCREAT)
    if err3 != nil {
        panic(err3)
    }
    coll, _ := jb.CreateColl("contacts", nil)
	res, _ := coll.Find(`{"email" :`+email+`}`) // Name starts with 'Bru' string
    log.Printf("\n\nRecords found: %d\n", len(res))
    if(len(res)==0){
    log.Println("not found")
		//w.Write([]byte("No Such Profile Exists"))
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(404)
    
    }else{
    
	    var m map[string]interface{}
	    for _, bs := range res {
	        
	        bson.Unmarshal(bs, &m)
	        log.Println(m)
	    }
	    returnJson, _ := json.Marshal(m)
	    log.Println("json frm db->"+string(returnJson))
	    
	    w.Write([]byte(returnJson))
	    
    }
    jb.Close()
	/*f:=0
	for i:=0;i<len(v);i++{
		log.Println("item-->"+v[i].Email)
		if(v[i].Email==email){
		log.Println("found")
		//json.NewEncoder(w).Encode(v[i])
		res2B, _ := json.Marshal(v[i])
		log.Println("p in json->"+string(res2B))
		w.Write([]byte(res2B))
		f=1
		break
		}
	}
	if(f==0){
	log.Println("not found")
		//w.Write([]byte("No Such Profile Exists"))
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(404)
	}*/
}