package main

import (
	"bufio"
	"flag"
	"github.com/xdean/goex/xconfig"
	"log"
	"os"
	"strings"
)

func main() {
	fptr := flag.String("in", "in.txt", "file path to read from")
	fout := flag.String("out", "out.txt", "file path to write")
	flag.Parse()

	fread, err := os.Open(*fptr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = fread.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	fwrite, err := os.Create(*fout)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = fwrite.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	s := bufio.NewScanner(fread)
	w := bufio.NewWriter(fwrite)
	for s.Scan() {
		inline := s.Text()
		info := strings.Split(inline, " ")
		user := xconfig.Encrypt([]byte(string(info[0])), "sqlExporter@User")
		passwd := xconfig.Encrypt([]byte(string(info[1])), "sqlExporter@Password")
		outline := user + "\t" + passwd + "\n"
		defer w.Flush()
		w.WriteString(inline + "\n")
		w.WriteString(outline)
		w.WriteString("\n")
	}

	err = s.Err()
	if err != nil {
		log.Fatal(err)
	}
}
