#!/bin/bash
# Build script for Maven Central Miner (Java version)

set -e

echo "=================================="
echo "Maven Central Miner - Build Script"
echo "=================================="
echo

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed."
    echo "Please install Maven first:"
    echo "  macOS: brew install maven"
    echo "  Linux: sudo apt-get install maven"
    echo "  Or download from: https://maven.apache.org/download.cgi"
    exit 1
fi

# Try to set JAVA_HOME from Homebrew OpenJDK if not already set
if [ -z "$JAVA_HOME" ]; then
    # Try to find Homebrew OpenJDK installations
    for version in 21 17 11; do
        BREW_JDK="/opt/homebrew/opt/openjdk@${version}"
        if [ -d "$BREW_JDK" ]; then
            export JAVA_HOME="$BREW_JDK"
            export PATH="$JAVA_HOME/bin:$PATH"
            echo "Using Homebrew OpenJDK ${version} from: $JAVA_HOME"
            break
        fi
    done
    
    # Try generic openjdk (latest)
    if [ -z "$JAVA_HOME" ] && [ -d "/opt/homebrew/opt/openjdk" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk"
        export PATH="$JAVA_HOME/bin:$PATH"
        echo "Using Homebrew OpenJDK from: $JAVA_HOME"
    fi
fi

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if [ -z "$JAVA_VERSION" ] || [ "$JAVA_VERSION" = "1" ]; then
    # Handle Java 8 version format (1.8.x)
    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f2)
    if [ "$JAVA_VERSION" = "8" ]; then
        echo "Error: Java 11 or higher is required."
        echo "Current Java version: $(java -version 2>&1 | head -n 1)"
        echo ""
        echo "Please install OpenJDK 17:"
        echo "  brew install openjdk@17"
        echo "  sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk"
        exit 1
    fi
fi

if [ "$JAVA_VERSION" -lt 11 ]; then
    echo "Error: Java 11 or higher is required."
    echo "Current Java version: $(java -version 2>&1 | head -n 1)"
    echo ""
    echo "Please install OpenJDK 17:"
    echo "  brew install openjdk@17"
    echo "  sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk"
    exit 1
fi

echo "Java version: $(java -version 2>&1 | head -n 1)"
echo "Maven version: $(mvn -version | head -n 1)"
echo

# Build the project
echo "Building Maven Central Miner..."
mvn clean package

if [ $? -eq 0 ]; then
    echo
    echo "=================================="
    echo "Build successful!"
    echo "=================================="
    echo
    echo "Executable JAR created at:"
    echo "  target/maven-miner-1.0.0-jar-with-dependencies.jar"
    echo
    echo "To run the miner:"
    echo "  ./run.sh"
    echo "  or"
    echo "  java -jar target/maven-miner-1.0.0-jar-with-dependencies.jar"
    echo
else
    echo
    echo "Build failed. Please check the errors above."
    exit 1
fi
