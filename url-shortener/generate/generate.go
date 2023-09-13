package generate

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	petname "github.com/dustinkirkland/golang-petname"
)

func writeUrlWorker(result <-chan string) {
	f, err := os.Create("test.dat")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	bf := bufio.NewWriter(f)
	for r := range result {
		if !strings.Contains(r, "server error") {
			bf.WriteString("http://localhost:8080/" + r)
		} else {
			fmt.Println("err")
		}
	}
	bf.Flush()
}

func requestWorker(cli *http.Client, tasks <-chan string, result chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	for t := range tasks {
		body := strings.NewReader("url=" + t)
		response, err := cli.Post("http://localhost:8080/save", "application/x-www-form-urlencoded", body)
		if err != nil {
			panic(err)
		}
		bo, err := io.ReadAll(response.Body)
		if err != nil {
			panic(err)
		}
		token := string(bo) + "\n"
		result <- token
		response.Body.Close()
	}
}

func GenerateUrls() {
	fmt.Println("aaa")
	const workerNum = 50
	tasks := make(chan string, 100000)
	result := make(chan string, 100000)

	wg := sync.WaitGroup{}
	wg.Add(workerNum)

	cli := &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: 20,
		},
	}
	for i := 0; i < workerNum; i++ {
		go requestWorker(cli, tasks, result, &wg)
	}
	go writeUrlWorker(result)

	for i := 0; i < 30000; i++ {
		word := petname.Generate(3, "-")
		tasks <- word
	}
	close(tasks)

	go func() {
		wg.Wait()
		fmt.Println("wait end")
		close(result)

		fmt.Println("end")
	}()
}

/*
func GenerateUrlForPost() {
	f, err := os.Create("post.dat")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	bf := bufio.NewWriter(f)
	for i := 0; i < 50000; i++ {
		word := petname.Generate(3, "-")
		bf.WriteString("http://localhost:8080/save POST url=" + word + "\n")
	}
	bf.Flush()
}
*/
