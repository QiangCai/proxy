#!/bin/bash

echo proxy $3:$4 to $1:$2 ...
nohup java -cp proxy.jar org.apache.carbondata.Proxy $1 $2 $3 $4 &
