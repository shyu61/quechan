package main

import (
	"fmt"
)

func main() {
	fmt.Println("Hello, world!")
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
