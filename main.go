package main

import (
	"fmt"
	"net/http"
	"sync"
)

// POST /publish
func publisher(w http.ResponseWriter, r *http.Request, m sync.Map) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Invalid http method")
		return
	}
	m.Store("name", r.Body)
	m.Range(func(key, value interface{}) bool {
		fmt.Fprintf(w, "%v: %v", key, value)
		return true
	})
	fmt.Fprintf(w, "OK!!")
}

func subscriber(w http.ResponseWriter, r *http.Request, m sync.Map) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprint(w, "Invalid http method")
		return
	}
	m.Range(func(key, value interface{}) bool {
		fmt.Printf("%v: %v", key, value)
		return true
	})
	fmt.Fprintf(w, "OK!!!!!")
}

func main() {
	m := sync.Map{}

	http.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		publisher(w, r, m)
	})
	http.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		subscriber(w, r, m)
	})

	http.ListenAndServe(":8080", nil)
}

// キューイング機能の実装
	// 0. HTTPリクエストを処理できるようにする。echoサーバー入れるのが良いかな。
	// 1. キューを格納する構造体の配列を用意する。
	// 2. publishするエンドポイントと、subscribeするエンドポイントを用意する。
	// 	- HTTPリクエストでPOSTされていくるオブジェクトをJSONにシリアライズする。
	// 	- FIFOで実装する
	// 	- publishするときにtopic名を受け取るようにし、publish/subscribeに名前をつけて識別する。topic名は一意制約をかける。
	// 3. キューがいっぱいの時、空の時のpub/subのそれぞれの挙動を制御する。
	// 4. リングバッファを考慮する。
