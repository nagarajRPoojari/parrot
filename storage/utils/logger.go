package utils

import "log"

func init() {
	log.SetPrefix("[Parrot] ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
