#!/bin/bash

set -e

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <bootstrap_binary_path> <bootstrap_zip_path>"
    echo "Example: $0 /build/target/lambda/rotel-lambda-forwarder/bootstrap /build/target/lambda/rotel-lambda-forwarder/bootstrap.zip"
    exit 1
fi

BOOTSTRAP_BINARY="$1"
BOOTSTRAP_ZIP="$2"

# Verify bootstrap binary exists
if [ ! -f "$BOOTSTRAP_BINARY" ]; then
    echo "Error: Bootstrap binary not found at: $BOOTSTRAP_BINARY"
    exit 1
fi

# Expand BOOTSTRAP_ZIP to absolute path if it's relative
if [[ "$BOOTSTRAP_ZIP" != /* ]]; then
    BOOTSTRAP_ZIP="$(cd "$(dirname "$BOOTSTRAP_ZIP")" && pwd)/$(basename "$BOOTSTRAP_ZIP")"
fi

rm -f "$BOOTSTRAP_ZIP"

#echo "Analyzing dependencies for: $BOOTSTRAP_BINARY"

# Run ldd and extract library paths for libpython and libexpat
LIBS_TO_BUNDLE=$(ldd "$BOOTSTRAP_BINARY" | grep -E 'libpython|libexpat' | awk '{print $3}' | grep -v '^$')

if [ -z "$LIBS_TO_BUNDLE" ]; then
    echo "No libpython or libexpat libraries found in dependencies"
    exit 0
fi

# Create temporary directory for staging libraries
TEMP_DIR=$(mktemp -d)
LIB_DIR="$TEMP_DIR/lib"
mkdir -p "$LIB_DIR"

echo "Found libraries to bundle:"
for lib in $LIBS_TO_BUNDLE; do
    echo "  - $lib"
    # Copy library to staging directory
    cp "$lib" "$LIB_DIR/"

    # Also copy any symlinks or related versions
    lib_basename=$(basename "$lib")
    lib_dirname=$(dirname "$lib")

    # Find and copy related symlinks (e.g., libpython3.so -> libpython3.so.1.0)
    # find "$lib_dirname" -name "${lib_basename%.*}*" -type f -o -type l 2>/dev/null | while read -r related_lib; do
    #     if [ "$related_lib" != "$lib" ]; then
    #         echo "    + $(basename "$related_lib")"
    #         cp -P "$related_lib" "$LIB_DIR/" 2>/dev/null || true
    #     fi
    # done
done

#echo "Adding $BOOTSTRAP_BINARY to $BOOTSTRAP_ZIP"
cp $BOOTSTRAP_BINARY $TEMP_DIR/

# Add libraries to the zip file under lib/ path
#echo "Adding libraries to $BOOTSTRAP_ZIP under lib/ path"
cd "$TEMP_DIR"
zip -r "$BOOTSTRAP_ZIP" bootstrap lib/

# Cleanup
rm -rf "$TEMP_DIR"

#echo "Successfully bundled libraries into $BOOTSTRAP_ZIP"
