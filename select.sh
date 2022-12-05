#!/bin/bash
while IFS="," read -r line
do
   echo "| $line| " >> "$GITHUB_STEP_SUMMARY"
done < <(grep . result.csv | sed 1d | tail -n 30 | tr ',' '|') #Reads the last 30 lines except the header