from flask import Flask, render_template, request, Response, stream_with_context, jsonify
import subprocess
import json
import uuid
import threading
import queue
import time
import shlex
import os

app = Flask(__name__)

# Global jobs storage
jobs = {}
# Active processes storage: job_id -> [Popen objects]
active_processes = {}

# Path to iobench binary
IOBENCH_PATH = "./iobench/iobench"

@app.route("/", methods=["GET"])
def index():
    devices = get_devices()
    return render_template("index.html", devices=devices)


@app.route("/job", methods=["POST"])
def create_job():
    """
    Accepts a job configuration.
    Structure:
    {
      "execution_mode": "sequential" | "parallel",
      "workers": [
        {
          "id": 1,
          "name": "Worker 1",
          "config": {
             "block_size": "4k",
             "duration": 10,
             "rw_mix": 100 (read %),
             "pattern": "random" | "sequential",
             "queue_depth": 32
          },
          "targets": ["/dev/loop0", ...] 
        }
      ]
    }
    """
    try:
        data = request.json
        job_id = str(uuid.uuid4())
        jobs[job_id] = data
        return jsonify({"job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/stop", methods=["POST"])
def stop_job():
    """
    Stops all running processes for a given job_id or all jobs if no ID provided.
    """
    job_id = request.json.get("job_id")
    
    keys_to_remove = []
    
    if job_id:
        target_jobs = [job_id] if job_id in active_processes else []
    else:
        target_jobs = list(active_processes.keys())
        
    count = 0
    for jid in target_jobs:
        procs = active_processes.get(jid, [])
        for p in procs:
            if p.poll() is None: # If running
                p.terminate() # Try terminate first
                try:
                    p.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    p.kill() # Force kill
                count += 1
        keys_to_remove.append(jid)
        
    for k in keys_to_remove:
        if k in active_processes:
            del active_processes[k]
            
    return jsonify({"status": "stopped", "killed_processes": count})

@app.route("/stream")
def stream():
    job_id = request.args.get("job_id")
    if not job_id or job_id not in jobs:
        def error_gen():
             yield "data: ERR: Invalid Job ID\n\n"
             yield "data: STATUS:Error\n\n"
        return Response(stream_with_context(error_gen()), content_type="text/event-stream")

    job = jobs[job_id]
    
    def generate():
        try:
            execution_mode = job.get("execution_mode", "parallel")
            workers = job.get("workers", [])

            # If only one worker, execution mode doesn't matter much, just run it.
            # But we respect the flag if multiple.
            
            if len(workers) == 0:
                yield "data: STATUS:Done\n\n"
                return

            if execution_mode == "sequential":
                for worker in workers:
                    # Pass job_id to worker for process tracking
                    worker["job_id"] = job_id 
                    yield from run_worker(worker)
                yield "data: STATUS:Done\n\n"
                
            else: # parallel
                out_queue = queue.Queue()
                threads = []
                active_workers = len(workers)
                
                def worker_thread_func(w):
                    # Pass job_id to worker for process tracking
                    w["job_id"] = job_id
                    for chunk in run_worker(w):
                        out_queue.put(chunk)
                    out_queue.put(None) # Signal this worker is done

                for worker in workers:
                    t = threading.Thread(target=worker_thread_func, args=(worker,))
                    t.start()
                    threads.append(t)

                completed_workers = 0
                while completed_workers < active_workers:
                    try:
                        item = out_queue.get(timeout=0.1)
                        if item is None:
                            completed_workers += 1
                        else:
                            yield item
                    except queue.Empty:
                        # Keep connection alive with comment
                        yield ": keepalive\n\n" 
                        continue
                
                for t in threads:
                    t.join()
                    
                yield "data: STATUS:Done\n\n"
                
        except Exception as e:
            app.logger.error(f"Stream error: {e}")
            # Send error to client log
            yield f"data: {json.dumps({'worker_id': 'SYSTEM', 'line': f'CRITICAL STREAM ERROR: {str(e)}'})}\n\n"
            yield f"data: STATUS:Error\n\n"

    return Response(stream_with_context(generate()), content_type="text/event-stream")


def run_worker(worker):
    """
    Generator that runs a single worker process and yields its output formatted as SSE.
    """
    wid = worker.get("id")
    job_id = worker.get("job_id") # We need to pass job_id to worker to track process
    config = worker.get("config", {})
    targets_config = worker.get("targets", []) # This is the list of target objects
    
    # Extract device paths for the command
    target_devices = [t.get("device") for t in targets_config if t.get("device")]
    
    # Debug logging
    print(f"DEBUG: Worker ID: {wid} | Job ID: {job_id}")
    print(f"DEBUG: Config: {config}")
    print(f"DEBUG: Targets Config: {targets_config}")

    if not target_devices:
        yield f"data: {json.dumps({'worker_id': wid, 'line': 'WARN: No targets defined for this worker'})}\n\n"
        return

    # Build command
    cmd = [IOBENCH_PATH]

    # Duration (convert to seconds based on unit)
    # If config doesn't have duration, check if we have targets and use the first one's duration
    if "duration" not in config and targets_config:
        first_target = targets_config[0]
        try:
            duration = int(first_target.get("duration", 10))
        except:
            duration = 10
        duration_unit = first_target.get("duration_unit", "seconds")
    else:
        try:
            duration = int(config.get("duration", 10))
        except:
            duration = 10
        duration_unit = config.get("duration_unit", "seconds")
    
    if duration_unit == "minutes":
        duration = duration * 60
    elif duration_unit == "hours":
        duration = duration * 3600
        
    print(f"DEBUG: Final Duration: {duration} seconds")
    
    cmd.extend(["-t", str(duration)])
    
    # Block Size
    cmd.extend(["-bs", str(config.get("block_size", "4k"))])
    
    # Pattern
    if config.get("pattern") == "sequential":
        cmd.append("-seq")
    
    # RW Mix
    # iobench -wp <write_percentage>
    # UI sends 'read_percent' (0-100)
    read_pct = config.get("read_percent", 100)
    try:
        read_pct = int(read_pct)
    except:
        read_pct = 100 # default to read only
        
    wp = 100 - read_pct
    if wp > 0:
         cmd.extend(["-wp", str(wp)])
    
    # Queue Depth -> iobench -qs
    qs = config.get("queue_depth")
    if qs:
        cmd.extend(["-qs", str(qs)])

    # Devices
    cmd.extend(target_devices)

    # Log command for debugging
    cmd_str = " ".join(shlex.quote(s) for s in cmd)
    yield f"data: {json.dumps({'worker_id': wid, 'line': f'CMD: {cmd_str}'})}\n\n"

    proc = None
    try:
        # Use stdbuf or unbuffered logic if possible, but python Popen with bufsize=1 (line buffered) usually works for text.
        # We ensure stderr goes to stdout so we capture errors.
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.getcwd() # Explicit execution dir
        )
        
        # Register process
        if job_id:
            if job_id not in active_processes:
                active_processes[job_id] = []
            active_processes[job_id].append(proc)
        
        for line in proc.stdout:
            yield f"data: {json.dumps({'worker_id': wid, 'line': line.strip()})}\n\n"
            
        proc.wait()
        
        if proc.returncode != 0:
             yield f"data: {json.dumps({'worker_id': wid, 'error': f'Process exited with code {proc.returncode}'})}\n\n"
             
    except FileNotFoundError:
        yield f"data: {json.dumps({'worker_id': wid, 'line': 'ERROR: iobench binary not found at ' + IOBENCH_PATH})}\n\n"
        return
    except Exception as e:
        yield f"data: {json.dumps({'worker_id': wid, 'line': f'ERROR: Failed to start iobench: {str(e)}'})}\n\n"
        return


def get_devices():
    """
    Parses lsblk output to get a list of available block devices.
    Includes only regular disk devices, excludes loop and CD/DVD devices.
    OS disk is visible but marked as disabled for safety.
    """
    devices = []
    try:
        result = subprocess.run(
            ["lsblk", "-d", "-o", "NAME,SIZE,MODEL,TYPE,MOUNTPOINT", "-J"],
            capture_output=True,
            text=True
        )
        data = json.loads(result.stdout)
        
        for dev in data.get("blockdevices", []):
            dev_type = dev.get("type", "")
            
            # Filter out CD/DVD and loop devices - keep only regular disks
            if dev_type != "disk":
                continue
            
            path = f"/dev/{dev['name']}"
            is_os = False
            disabled_reason = None
            
            # Check if this is the OS disk
            mountpoint = dev.get("mountpoint")
            if mountpoint in ["/", "/boot", "/boot/efi"]:
                is_os = True
                disabled_reason = "OS Disk - Locked for safety"
            
            devices.append({
                "path": path,
                "size": dev.get("size", "Unknown"),
                "model": dev.get("model") or "N/A",
                "type": dev_type,
                "os_disk": is_os,
                "disabled": is_os,
                "disabled_reason": disabled_reason
            })
            
    except Exception as e:
        # Fallback if lsblk fails (e.g. dev environment)
        app.logger.error(f"Failed to enumerate devices: {e}")
        pass
        
    return devices



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
