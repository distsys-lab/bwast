#!/usr/bin/env bash
rm ./config/currentView
java -Dlogback.configurationFile="./config/logback.xml" -XX:MaxRAMPercentage=90 -cp bin/*:lib/* bftsmart.demo.counter.ModifiedCounterServer 0 500MB