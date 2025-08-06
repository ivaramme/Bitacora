#!/bin/bash

# Dependency Update Script for Bitacora
# This script helps check for and update dependencies safely

echo "=== Bitacora Dependency Update Helper ==="
echo

# Check current Java version
echo "Current Java version:"
java -version
echo

# Check Maven version
echo "Current Maven version:"
mvn --version | head -1
echo

# Display current dependency versions
echo "Checking for dependency updates..."
mvn versions:display-dependency-updates

echo
echo "Checking for plugin updates..."
mvn versions:display-plugin-updates

echo
echo "=== Security Scan ==="
echo "Running OWASP dependency check (this may take a while)..."
mvn org.owasp:dependency-check-maven:check

echo
echo "=== Build Test ==="
echo "Testing current build..."
mvn clean compile

echo
echo "=== Instructions ==="
echo "1. Review the dependency and plugin updates above"
echo "2. Update versions in pom.xml carefully"
echo "3. Test build after each major update"
echo "4. Run 'mvn clean test' to ensure tests pass"
echo "5. Check security report in target/dependency-check-report.html"