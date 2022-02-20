#!/bin/sh
./dblisten -connect="dbname=qctldb user=postgres host=localhost" -tables=oprequests,opaborts
