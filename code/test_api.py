#!/usr/bin/env python3
"""
Test script for the Vela API endpoint
"""
import requests
import json

def test_vela_create_endpoint():
    """Test the /api/vela/create endpoint"""
    url = "http://localhost:8000/api/vela/create"
    
    # Test data
    test_data = {
        "namespace": "test-namespace",
        "dbuser": "testuser",
        "dbname": "testdb",
        "dbpassword": "testpassword123"
    }
    
    try:
        response = requests.post(url, json=test_data)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code == 200:
            print("✅ API endpoint is working correctly!")
        else:
            print("❌ API endpoint returned an error")
            
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to the API. Make sure the server is running on localhost:8000")
    except Exception as e:
        print(f"❌ Error testing API: {str(e)}")

if __name__ == "__main__":
    print("Testing Vela API endpoint...")
    test_vela_create_endpoint()
