#!/usr/bin/env bash

# This is WORK IN PROGRESS - I intended to create an alternative implementation
# with efficient unix tools like sed and awk to compare performance.

# Base path to search for files recursively
basePath="/Users/md/code/spark-kit/memartscc-token-renewal/remote-test/log/application_1734940586637_0046-short-success"

# File extensions to exclude from output (strict match)
exclude_extensions=("*.zip" "*.tar" "*.gz" "*.rar" "*.7z" "*.tgz" "*.bz2" "*.tbz2" "*.xz" "*.txz")

# File extensions to include in the output (lenient match)
include_extensions=("*.log" "*.err" "*.error" "*.warn" "*.warning" "*.info" "*.txt" "*.out" "*.debug" "*.trace")

start_time="$(date -u +%s)"

# Build the 'find' command
find_command="find \"$basePath\" -type f"

# Add inclusion filters
include_filters=""
for ext in "${include_extensions[@]}"; do
    include_filters+=" -name \"$ext\" -o"
done
# Remove the trailing "-o" and wrap with parentheses
if [ -n "$include_filters" ]; then
    include_filters="\\( ${include_filters::-3} \\)"
fi

# Add exclusion filters
exclude_filters=""
for ext in "${exclude_extensions[@]}"; do
    exclude_filters+=" -name \"$ext\" -o"
done
# Remove the trailing "-o" and wrap with parentheses
if [ -n "$exclude_filters" ]; then
    exclude_filters="\\( ${exclude_filters::-3} \\)"
    exclude_filters="-not $exclude_filters"
fi

# Combine the filters into the final find command
if [ -n "$include_filters" ]; then
    find_command+=" $include_filters"
fi
if [ -n "$exclude_filters" ]; then
    find_command+=" $exclude_filters"
fi

# Print and run the command
echo "Executing: $find_command"
files=$(eval "$find_command")

# Feed all files to sort
mkdir -p out
sort -k1,1 $files > out/merged.log

end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
echo "Elapsed time: $elapsed seconds"

#
#time sort -m -k1,1 file1.log file2.log > merged.log
#
#awk 'BEGIN { FS=" "; OFS=" " }
#{
#    # Parse and reformat the timestamp
#    if ($1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}/) {
#        print $0
#    } else {
#        # Handle other formats
#    }
#}' file.log > normalized.log
#
#awk '...' file1.log file2.log file3.log | sort -k1,1 > merged.log
#
