#!/bin/bash

# Test script to verify that session_user now returns the actual logged-in username

echo "Testing that session_user returns the actual logged-in username..."

# Test with different usernames to verify it's using the actual username instead of hardcoded 'postgres'

echo -e "\n1. Testing session_user with username 'testuser1':"
psql -h localhost -p 5432 -U testuser1 -d wincc -c "SELECT session_user"

echo -e "\n2. Testing current_user with username 'testuser1':"
psql -h localhost -p 5432 -U testuser1 -d wincc -c "SELECT current_user"

echo -e "\n3. Testing user with username 'testuser1':"
psql -h localhost -p 5432 -U testuser1 -d wincc -c "SELECT user"

echo -e "\n4. Testing session_user with username 'admin':"
psql -h localhost -p 5432 -U admin -d wincc -c "SELECT session_user"

echo -e "\n5. Testing current_user with username 'admin':"
psql -h localhost -p 5432 -U admin -d wincc -c "SELECT current_user"

echo -e "\n6. Testing user with username 'admin':"
psql -h localhost -p 5432 -U admin -d wincc -c "SELECT user"

echo -e "\n7. Testing combined query with username 'operator':"
psql -h localhost -p 5432 -U operator -d wincc -c "SELECT session_user, current_user, user, current_database()"

echo -e "\n8. Testing with standard 'postgres' username for comparison:"
psql -h localhost -p 5432 -U postgres -d wincc -c "SELECT session_user, current_user, user"

echo -e "\nAll username tests completed!"
echo "The session_user, current_user, and user should now return the actual username provided via -U flag"