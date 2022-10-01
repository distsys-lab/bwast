#!/usr/bin/env bash
rm ./config/currentView
java -Dlogback.configurationFile="./config/logback.xml" -cp bin/*:lib/* bftsmart.demo.counter.LatencyCounterClient 0 1 10000000
