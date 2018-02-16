/*******************************************************************************
* Author: Antony Toron
* File name: main.go
* Date created: 2/16/18
*
* Description: entry into code, command runs a server
*******************************************************************************/

package main

import (
    //"fmt"
    "foxyblox/server"
)

/*
*/
func main() {
    server.Run() // note: to use function in another package, need capital R
}