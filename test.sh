#!/bin/bash

FILE_PATH="manifest/test/manifest.json"  

while true; do
    if [[ -f "$FILE_PATH" ]]; then
        FILE_SIZE=$(stat -f%z "$FILE_PATH")
        echo "Size of $FILE_PATH: $FILE_SIZE bytes"
    else
        echo "File $FILE_PATH does not exist."
    fi
done