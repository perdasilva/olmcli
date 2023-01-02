/*
Copyright © 2022 Per G. da Silva <pegoncal@redhat.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"github.com/perdasilva/olmcli/cmd"
)

func main() {
	//f, err := os.Create("cpu.prof")
	//if err != nil {
	//	log.Fatal("could not create CPU profile: ", err)
	//}
	//defer f.Close() // error handling omitted for example
	//if err := pprof.StartCPUProfile(f); err != nil {
	//	log.Fatal("could not start CPU profile: ", err)
	//}
	//defer pprof.StopCPUProfile()
	//
	//// ... rest of the program ...
	//
	//fm, err := os.Create("mem.prof")
	//if err != nil {
	//	log.Fatal("could not create memory profile: ", err)
	//}
	//defer fm.Close() // error handling omitted for example
	//runtime.GC()     // get up-to-date statistics
	//if err := pprof.WriteHeapProfile(fm); err != nil {
	//	log.Fatal("could not write memory profile: ", err)
	//}
	cmd.Execute()
}
