// Package main defines the nectar command line tool.
package main

import (
	"os"

	"github.com/troubling/nectar"
)

func main() {
	nectar.CLI(os.Args, nil, nil, nil)
}
