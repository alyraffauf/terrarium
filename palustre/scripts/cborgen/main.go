package main

import (
	"github.com/alyraffauf/terrarium/palustre/listlex"
	cbg "github.com/whyrusleeping/cbor-gen"
	"log"
)

func main() {
	config := cbg.Gen{MaxStringLength: 1_000_000, MaxArrayLength: 8192, MaxByteLength: 2_000_000}
	err := config.WriteMapEncodersToFile("palustre/listlex/cbor_gen.go", "listlex",
		listlex.GraphList{}, listlex.GraphListitem{}, listlex.RichtextFacet{},
		listlex.RichtextFacet_ByteSlice{}, listlex.RichtextFacet_Link{},
		listlex.RichtextFacet_Mention{}, listlex.RichtextFacet_Tag{},
	)
	if err != nil {
		log.Fatal(err)
	}
}
