#!/bin/bash
while IFS="," read -r line
do
   echo "| $line| " >> "$GITHUB_STEP_SUMMARY"
done < <(grep . result.csv | sed 1d | tail -n 15 | tr ',' '|') #Reads the last 15 lines except the header