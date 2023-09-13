package main

import (
	"fmt"
	"log"
	"math/big"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"url-shortener/generate"
)

var u UrlRepository
var keyMap sync.Map
var bloomfilter BloomFilter

func init() {
	u = NewUrlRepository()
	bloomfilter = *NewBloomFilter(10000000, 100)
	// TODO: DBにあるURLを全部bloomfilterに入れる
}

func base62(str string) string {
	var i big.Int
	bb := []byte(str)
	i.SetBytes(bb)
	encoded := i.Text(62)
	return encoded
}

func generateDummyUniqueId() string {
	now := time.Now().UnixMilli()
	value, ok := keyMap.Load(now)
	var i int
	if ok {
		i = value.(int)
	}
	keyMap.Store(now, i+1)
	// TODO: set worker id
	return fmt.Sprint(now) + "01" + strconv.Itoa(i)
}

func generateToken(originalUrl string) (*Url, error) {
	url := &Url{
		OriginalUrl: originalUrl,
		Token:       base62(generateDummyUniqueId()),
	}
	// save as tmp to get primary key
	lastId, err := u.SaveUrl(url)
	if err != nil {
		return nil, err
	}
	// save actual token based primary key
	url.Token = base62(strconv.Itoa(int(lastId)))
	url.Id = lastId

	_, err = u.UpdateUrl(url)
	if err != nil {
		return nil, err
	}
	return url, nil
}

func urlSaveHandler(w http.ResponseWriter, r *http.Request) {
	originalUrl := r.FormValue("url")
	if len(originalUrl) < 1 {
		http.Error(w, "url not contained", http.StatusBadRequest)
		return
	}

	var err error
	url := &Url{}

	if false {
		if bloomfilter.Contains(originalUrl) {
			url, err = u.GetTokenFromUrl(originalUrl)
		} else {
			url, err = generateToken(originalUrl)
			bloomfilter.Add(originalUrl)
		}
	} else {
		url, err = u.GetTokenFromUrl(originalUrl)
		if err != nil {
			fmt.Println(err)
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		if len(url.Token) < 1 {
			url, err = generateToken(originalUrl)
		}
	}

	if err != nil {
		fmt.Println(err)
		http.Error(w, "server error a", http.StatusInternalServerError)
		return
	}

	_, err = w.Write([]byte(url.Token))
	if err != nil {
		fmt.Println(err)
		http.Error(w, "server error b", http.StatusInternalServerError)
		return
	}
}

func urlGetHandler(w http.ResponseWriter, r *http.Request) {
	_, token := filepath.Split(r.URL.Path)
	if len(token) < 1 {
		http.Error(w, "token not specified", http.StatusBadRequest)
		return
	}
	url, err := u.GetUrlFromToken(token)
	if err != nil {
		fmt.Println(err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if len(url.Token) < 1 {
		http.Error(w, "url not found", http.StatusNotFound)
		return
	}
	hello := []byte("token: " + token + " , original_url: " + url.OriginalUrl)
	_, err = w.Write(hello)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	http.HandleFunc("/save", urlSaveHandler)
	http.HandleFunc("/", urlGetHandler)

	go func() {
		time.Sleep(1 * time.Second)
		generate.GenerateUrls()
	}()

	fmt.Println("server start")
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
}
