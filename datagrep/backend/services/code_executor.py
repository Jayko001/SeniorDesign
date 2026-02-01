"""
Code Executor Service
Executes Python code in a Docker sandbox with access to files and database
"""

import docker
import os
import tempfile
import json
import time
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

# Docker client
_docker_client = None


def get_docker_client():
    """Get or create Docker client"""
    global _docker_client
    if _docker_client is None:
        try:
            _docker_client = docker.from_env()
        except Exception as e:
            raise Exception(f"Failed to connect to Docker: {str(e)}")
    return _docker_client


async def execute_python_code(
    code: str,
    file_paths: List[str] = None,
    db_config: Dict[str, Any] = None,
    timeout: int = 60
) -> Dict[str, Any]:
    """
    Execute Python code in a Docker sandbox
    
    Args:
        code: Python code to execute
        file_paths: List of CSV file paths to mount (read-only)
        db_config: PostgreSQL connection configuration
        timeout: Execution timeout in seconds (default: 60)
        
    Returns:
        Dictionary with execution results:
        {
            "status": "success" | "error" | "timeout",
            "output": str,  # stdout content
            "error": str,   # stderr/exception content
            "execution_time": float,  # seconds
            "result_data": Any  # parsed results if JSON output
        }
    """
    start_time = time.time()
    client = get_docker_client()
    
    # Prepare file mounts (read-only)
    binds = {}
    if file_paths:
        for file_path in file_paths:
            if os.path.exists(file_path):
                # Mount files as read-only
                binds[file_path] = {
                    "bind": f"/data/{os.path.basename(file_path)}",
                    "mode": "ro"
                }
    
    # Prepare environment variables
    env_vars = {}
    if db_config:
        # Set PostgreSQL connection environment variables
        env_vars["POSTGRES_HOST"] = db_config.get("host", os.getenv("POSTGRES_HOST", "db"))
        env_vars["POSTGRES_PORT"] = str(db_config.get("port", os.getenv("POSTGRES_PORT", "5432")))
        env_vars["POSTGRES_DB"] = db_config.get("database", os.getenv("POSTGRES_DB", "datagrep"))
        env_vars["POSTGRES_USER"] = db_config.get("user", os.getenv("POSTGRES_USER", "datagrep"))
        env_vars["POSTGRES_PASSWORD"] = db_config.get("password", os.getenv("POSTGRES_PASSWORD", "datagrep_dev"))
    
    # Create temporary file for code - write it directly without wrapping
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as code_file:
        code_file.write(code)
        code_path = code_file.name
    
    try:
        
        # Use sandbox image (will be built via docker-compose)
        image_name = "datagrep-sandbox:latest"
        
        # Prepare volumes dict
        volumes_dict = {
            code_path: {"bind": "/code/script.py", "mode": "ro"}
        }
        volumes_dict.update(binds)
        
        # Only use network if db_config is provided (container needs to connect to database)
        # Otherwise use default bridge network (no network parameter needed)
        network_name = None
        if db_config:
            target_network = "datagrep-network"
            try:
                networks = client.networks.list()
                network_names = [net.name for net in networks]
                # Try exact match first, then docker-compose prefixed names
                possible_names = [
                    target_network,
                    f"datagrep_{target_network}",
                    f"{os.path.basename(os.getcwd())}_{target_network}"
                ]
                for possible_name in possible_names:
                    if possible_name in network_names:
                        network_name = possible_name
                        break
            except Exception as net_check_error:
                # If db_config is provided but network not found, this will likely fail when trying to connect to DB
                # But we'll let it fail naturally rather than complicating the logic
                network_name = None
        
        # Run container
        container = None
        try:
            # Prepare container run arguments
            run_kwargs = {
                "image": image_name,
                "command": ["python", "/code/script.py"],
                "volumes": volumes_dict,
                "environment": env_vars,
                "mem_limit": "512m",  # 512MB memory limit
                "cpu_period": 100000,
                "cpu_quota": 50000,  # 50% CPU limit
                "detach": True,  # Run in detached mode to support timeout
                "stdout": True,
                "stderr": True
            }
            # Only add network if it exists
            if network_name:
                run_kwargs["network"] = network_name
            
            # Run container in detached mode (timeout parameter removed from containers.run() in docker-py 6.x)
            container = client.containers.run(**run_kwargs)
            
            # Wait for container to complete with timeout
            try:
                container.wait(timeout=timeout)
            except Exception as wait_error:
                # If timeout occurred, stop the container
                container.stop()
                raise Exception(f"Container execution timed out after {timeout} seconds")
            
            # Get container logs
            container_output = container.logs(stdout=True, stderr=True)
            
            # Parse container output (returns bytes)
            if isinstance(container_output, bytes):
                output_text = container_output.decode('utf-8')
            else:
                output_text = str(container_output)
            
            execution_time = time.time() - start_time
            
            # Try to parse JSON if output looks like JSON
            result_data = None
            output_lines = output_text.strip().split('\n')
            if output_lines:
                # Try to parse the last line as JSON (common pattern)
                try:
                    result_data = json.loads(output_lines[-1])
                except:
                    # Try parsing entire output as JSON
                    try:
                        result_data = json.loads(output_text.strip())
                    except:
                        pass
            
            return {
                "status": "success",
                "output": output_text,
                "error": None,
                "execution_time": round(execution_time, 2),
                "result_data": result_data
            }
            
        except docker.errors.ContainerError as e:
            execution_time = time.time() - start_time
            # Try to extract error message from container
            error_msg = str(e)
            try:
                if container:
                    logs = container.logs(stdout=True, stderr=True)
                    if logs:
                        error_msg = logs.decode('utf-8') if isinstance(logs, bytes) else str(logs)
            except:
                pass
            return {
                "status": "error",
                "output": "",
                "error": error_msg,
                "execution_time": round(execution_time, 2),
                "result_data": None
            }
        except docker.errors.ImageNotFound:
            execution_time = time.time() - start_time
            return {
                "status": "error",
                "output": "",
                "error": "Sandbox image not found. Please build the sandbox image first.",
                "execution_time": round(execution_time, 2),
                "result_data": None
            }
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                "status": "error",
                "output": "",
                "error": f"Execution failed: {str(e)}",
                "execution_time": round(execution_time, 2),
                "result_data": None
            }
        finally:
            # Clean up container if it exists
            if container:
                try:
                    container.remove(force=True)
                except:
                    pass
    
    finally:
        # Clean up temp file
        if os.path.exists(code_path):
            try:
                os.remove(code_path)
            except:
                pass


def execute_python_code_sync(
    code: str,
    file_paths: List[str] = None,
    db_config: Dict[str, Any] = None,
    timeout: int = 60
) -> Dict[str, Any]:
    """
    Synchronous version of execute_python_code
    """
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is running, we need to run in executor
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, execute_python_code(code, file_paths, db_config, timeout))
                return future.result()
        else:
            return loop.run_until_complete(execute_python_code(code, file_paths, db_config, timeout))
    except RuntimeError:
        return asyncio.run(execute_python_code(code, file_paths, db_config, timeout))

