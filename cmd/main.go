package main

import "go-mqtt/pkg"


func main() {
	svr := pkg.NewService()
	svr.Start()
	<-svr.Done
}