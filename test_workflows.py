import requests
import time
import sys

API_BASE_URL = "http://localhost:8000"
POLL_INTERVAL = 2
MAX_WAIT_TIME = 60

def check_api_health():
    """Check if API is running"""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False

def submit_workflow(workflow):
    """Submit workflow and return execution_id"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/workflows",
            json=workflow,
            timeout=10
        )
        if response.status_code == 201:
            return response.json()["execution_id"]
        else:
            print(f"Failed to submit: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def trigger_workflow(execution_id):
    """Trigger workflow execution"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/workflow/trigger/{execution_id}",
            timeout=10
        )
        if response.status_code == 200:
            print(f"Workflow triggered: {execution_id}")
            return True
        else:
            print(f"Failed to trigger: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Error triggering workflow: {e}")
        return False

def get_workflow_status(execution_id):
    """Get workflow status"""
    try:
        response = requests.get(f"{API_BASE_URL}/workflows/{execution_id}", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def wait_for_completion(execution_id, workflow_name):
    """Wait for workflow to complete"""
    print(f"Waiting for '{workflow_name}'...")
    start_time = time.time()
    
    while True:
        if time.time() - start_time > MAX_WAIT_TIME:
            print("Timeout")
            return False
        
        status_data = get_workflow_status(execution_id)
        if not status_data:
            time.sleep(POLL_INTERVAL)
            continue
        
        status = status_data["status"]
        progress = status_data.get("progress", {})
        print(f"  Status: {status} | Progress: {progress.get('completed', 0)}/{progress.get('total', 0)}")
        
        if status == "COMPLETED":
            elapsed = time.time() - start_time
            print(f"Completed in {elapsed:.1f}s\n")
            return True
        elif status == "FAILED":
            print(f"Failed: {status_data.get('error', 'Unknown error')}\n")
            return False
        
        time.sleep(POLL_INTERVAL)

def submit_and_trigger(workflow):
    """Submit workflow, trigger it, and return execution_id"""
    execution_id = submit_workflow(workflow)
    if not execution_id:
        return None
    
    if not trigger_workflow(execution_id):
        return None
    
    return execution_id

def test_linear_chain():
    """Test 1: Linear execution A → B → C"""
    print("\n=== Test 1: Linear Chain ===")
    
    workflow = {
        "name": "Linear Processing",
        "dag": {
            "nodes": [
                {
                    "id": "fetch_data",
                    "handler": "call_external_service",
                    "dependencies": [],
                    "config": {"url": "http://api.example.com/data"}
                },
                {
                    "id": "process_data",
                    "handler": "llm",
                    "dependencies": ["fetch_data"],
                    "config": {"prompt": "Analyze {{ fetch_data.data }}"}
                },
                {
                    "id": "store_result",
                    "handler": "call_external_service",
                    "dependencies": ["process_data"],
                    "config": {"url": "http://api.example.com/store"}
                }
            ]
        }
    }
    
    execution_id = submit_and_trigger(workflow)
    if not execution_id:
        return False
    
    return wait_for_completion(execution_id, workflow["name"])

def test_parallel_execution():
    """Test 2: Parallel execution with fan-in"""
    print("\n=== Test 2: Parallel Execution ===")
    
    workflow = {
        "name": "Parallel API Fetcher",
        "dag": {
            "nodes": [
                {
                    "id": "input",
                    "handler": "input",
                    "dependencies": []
                },
                {
                    "id": "P1",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "http://api.example.com/users"}
                },
                {
                    "id": "P2",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "http://api.example.com/posts"}
                },
                {
                    "id": "P3",
                    "handler": "call_external_service",
                    "dependencies": ["input"],
                    "config": {"url": "http://api.example.com/comments"}
                },
                {
                    "id": "output",
                    "handler": "output",
                    "dependencies": ["P1", "P2", "P3"]
                }
            ]
        }
    }
    
    execution_id = submit_and_trigger(workflow)
    if not execution_id:
        return False
    
    return wait_for_completion(execution_id, workflow["name"])

def test_diamond_pattern():
    """Test 3: Diamond pattern (A → B,C → D)"""
    print("\n=== Test 3: Diamond Pattern ===")
    
    workflow = {
        "name": "Diamond Pattern",
        "dag": {
            "nodes": [
                {
                    "id": "A",
                    "handler": "input",
                    "dependencies": []
                },
                {
                    "id": "B",
                    "handler": "call_external_service",
                    "dependencies": ["A"],
                    "config": {"url": "http://api.example.com/b"}
                },
                {
                    "id": "C",
                    "handler": "call_external_service",
                    "dependencies": ["A"],
                    "config": {"url": "http://api.example.com/c"}
                },
                {
                    "id": "D",
                    "handler": "output",
                    "dependencies": ["B", "C"]
                }
            ]
        }
    }
    
    execution_id = submit_and_trigger(workflow)
    if not execution_id:
        return False
    
    return wait_for_completion(execution_id, workflow["name"])

def test_cycle_detection():
    """Test 4: Cycle detection (should fail)"""
    print("\n=== Test 4: Cycle Detection ===")
    
    workflow = {
        "name": "Invalid Circular",
        "dag": {
            "nodes": [
                {
                    "id": "A",
                    "handler": "input",
                    "dependencies": ["C"]
                },
                {
                    "id": "B",
                    "handler": "call_external_service",
                    "dependencies": ["A"],
                    "config": {}
                },
                {
                    "id": "C",
                    "handler": "output",
                    "dependencies": ["B"]
                }
            ]
        }
    }
    
    try:
        response = requests.post(f"{API_BASE_URL}/workflows", json=workflow, timeout=10)
        if response.status_code == 400 and "cycle" in response.json().get("detail", "").lower():
            print("Cycle correctly detected\n")
            return True
        else:
            print(f"Expected 400 with cycle error, got {response.status_code}\n")
            return False
    except Exception as e:
        print(f"Error: {e}\n")
        return False

def main():
    """Run all tests"""
    print("Workflow Orchestration Engine - Integration Tests")
    print("=" * 50)
    
    # Check API
    print("\nChecking API health...")
    if not check_api_health():
        print("API not available at", API_BASE_URL)
        print("Start services: docker-compose up")
        sys.exit(1)
    print("API is healthy")
    
    # Run tests
    tests = [
        ("Linear Chain", test_linear_chain),
        ("Parallel Execution", test_parallel_execution),
        ("Diamond Pattern", test_diamond_pattern),
        ("Cycle Detection", test_cycle_detection),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
            time.sleep(2)  # Pause between tests
        except KeyboardInterrupt:
            print("\nInterrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"Test crashed: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("Summary:")
    passed = sum(1 for _, s in results if s)
    print(f"Passed: {passed}/{len(results)}")
    
    for name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"  [{status}] {name}")
    
    sys.exit(0 if passed == len(results) else 1)

if __name__ == "__main__":
    main()