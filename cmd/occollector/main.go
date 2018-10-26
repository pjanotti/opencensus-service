// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Program ocagent collects OpenCensus stats and traces
// to export to a configured backend.
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/census-instrumentation/opencensus-service/internal/version"
)

func main() {
	var signalsChannel = make(chan os.Signal)
	signal.Notify(signalsChannel, os.Interrupt, syscall.SIGTERM)

	log.Printf("OpenCensus Collector started.\n%v", version.Info())

	<-signalsChannel
	log.Println("SIGTERM received, starting shutdown...")

	log.Println("Shutdown complete")
}
