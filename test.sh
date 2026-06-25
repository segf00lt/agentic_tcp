#!/bin/sh

pkill agentic_tcp
rm *.log *.csv

./agentic_tcp -listen :9001 -send :9002 & ./agentic_tcp -listen :9002 -send :9001


