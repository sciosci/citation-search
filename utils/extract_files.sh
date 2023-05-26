#!/bin/bash

input_folder="/home/ubuntu/mypetalibrary/semantic-scholar/23-05-2023/compressed"
output_folder="home/ubuntu/mypetalibrary/semantic-scholar/23-05-2023/extracted"

# Create the output folder if it doesn't exist
mkdir -p "$output_folder"

# Loop through each file in the input folder
for file in "$input_folder"/*; do
  if [[ -f "$file" ]]; then
    # Extract the file
    base_name=$(basename "$file")
    output_file="$output_folder/${base_name%.*}"
    gunzip -c "$file" > "$output_file"
    echo "File extracted: $output_file"
  fi
done

echo "Extraction complete."