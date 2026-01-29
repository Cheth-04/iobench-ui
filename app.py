from flask import Flask, render_template, request, Response, stream_with_context, jsonify
import subprocess
import json
import uuid
import threading
import queue
import shlex
import os
import sys
import stat
import time
import signal

app = Flask(__name__)

# Global jobs storage
jobs = {}
# Active processes storage - Maps job_id to list of Popen objects
active_processes = {}

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Check if running as PyInstaller bundle
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS

# Smart Binary Search
POSSIBLE_PATHS = [
    os.path.join(BASE_DIR, "iobench", "iobench"),
    os.path.join(BASE_DIR, "iobench"),
]
IOBENCH_PATH = None
for p in POSSIBLE_PATHS:
    if os.path.exists(p):
        IOBENCH_PATH = p
        break
if not IOBENCH_PATH:
    # Fallback default
    IOBENCH_PATH = os.path.join(BASE_DIR, "iobench")

# --- AUTO-FIX PERMISSIONS ---
if os.path.exists(IOBENCH_PATH):
    try:
        st = os.stat(IOBENCH_PATH)
        os.chmod(IOBENCH_PATH, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    except:
        pass

@app.route("/", methods=["GET"])
def index():
    devices = get_devices()
    return render_template("index.html", devices=devices)

@app.route("/job", methods=["POST"])
def create_job():
    try:
        data = request.json
        job_id = data.get("id") or str(uuid.uuid4())
        print(f"--> CREATING JOB: {job_id}")
        jobs[job_id] = data
        return jsonify({"job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# --- NEW STOP ENDPOINT ---
@app.route("/stop_job", methods=["POST"])
def stop_job():
    try:
        data = request.json
        job_id = data.get("job_id")
        
        if not job_id:
            return jsonify({"error": "No Job ID provided"}), 400

        print(f"--> STOP REQUEST FOR JOB: {job_id}")

        if job_id in active_processes:
            procs = active_processes[job_id]
            killed_count = 0
            
            # 1. Terminate gracefully
            for p in procs:
                if p.poll() is None:
                    print(f"    Terminating PID: {p.pid}")
                    p.terminate()
                    killed_count += 1
            
            # 2. Wait briefly
            time.sleep(0.5)
            
            # 3. Kill forcefully if still alive
            for p in procs:
                if p.poll() is None:
                    print(f"    Killing PID: {p.pid}")
                    p.kill()
            
            # Clean up storage
            # (Optional: keep it until stream closes, but usually safe to clear)
            # del active_processes[job_id] 
            
            return jsonify({"status": "stopped", "killed_count": killed_count})
        else:
            return jsonify({"status": "ignored", "message": "Job not active"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/stream")
def stream():
    job_id = request.args.get("job_id")
    if not job_id or job_id not in jobs:
        def error_gen(): 
            yield f"data: {json.dumps({'worker_id': 'SYSTEM', 'line': f'ERROR: Job ID {job_id} not found'})}\n\n"
            yield "data: STATUS:Error\n\n"
        return Response(stream_with_context(error_gen()), content_type="text/event-stream")

    job = jobs[job_id]
    
    def generate():
        try:
            execution_mode = job.get("execution_mode", "parallel")
            workers = job.get("workers", [])

            if len(workers) == 0:
                yield "data: STATUS:Done\n\n"
                return

            if execution_mode == "sequential":
                for worker in workers:
                    # Check if job was stopped (removed from active list or flagged)
                    # We can check if job_id is still in jobs, or just let run_worker handle it
                    worker["job_id"] = job_id 
                    yield from run_worker(worker)
                yield "data: STATUS:Done\n\n"
            else: # parallel
                out_queue = queue.Queue()
                threads = []
                active_workers = len(workers)
                
                def worker_thread_func(w):
                    w["job_id"] = job_id
                    for chunk in run_worker(w):
                        out_queue.put(chunk)
                    out_queue.put(None)

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
                        yield ": keepalive\n\n" 
                        continue
                
                for t in threads:
                    t.join()
                yield "data: STATUS:Done\n\n"

        except Exception as e:
            err_msg = f"CRITICAL STREAM ERROR: {str(e)}"
            print(err_msg)
            yield f"data: {json.dumps({'worker_id': 'SYSTEM', 'line': err_msg})}\n\n"
            yield f"data: STATUS:Error\n\n"

    return Response(stream_with_context(generate()), content_type="text/event-stream")

def run_worker(worker):
    wid = worker.get("id")
    job_id = worker.get("job_id")
    
    # Extract config. Prioritize target-specific config if available.
    targets = worker.get("targets", [])
    config = worker.get("config", {})
    
    # Merge config: use first target's settings if available, else worker global
    if targets:
        target_conf = targets[0]
        # Copy relevant keys from target to config for the command builder
        for k in ['block_size', 'queue_depth', 'duration', 'duration_unit', 'pattern', 'read_percent']:
            if k in target_conf:
                config[k] = target_conf[k]
    
    if not os.path.exists(IOBENCH_PATH):
        msg = f"ERROR: Binary NOT FOUND at: {IOBENCH_PATH}"
        yield f"data: {json.dumps({'worker_id': wid, 'line': msg})}\n\n"
        return

    target_devices = [t.get("device") for t in targets if t.get("device")]
    if not target_devices:
        yield f"data: {json.dumps({'worker_id': wid, 'line': 'WARN: No targets defined'})}\n\n"
        return

    # --- BUILD COMMAND ---
    cmd = [IOBENCH_PATH]

    cmd.extend(["-bs", str(config.get("block_size", "4k"))])
    
    if config.get("queue_depth"):
        cmd.extend(["-qs", str(config.get("queue_depth"))])

    duration = int(config.get("duration", 10))
    duration_unit = config.get("duration_unit", "seconds")
    
    if duration_unit == "minutes": duration *= 60
    elif duration_unit == "hours": duration *= 3600
    cmd.extend(["-t", str(duration)])
    
    if config.get("pattern") == "sequential":
        cmd.append("-seq")
    
    read_pct = int(config.get("read_percent", 100))
    if read_pct < 100:
        write_pct = 100 - read_pct
        cmd.extend(["-wp", str(write_pct)])
    
    cmd.extend(target_devices)

    cmd_str = " ".join(shlex.quote(s) for s in cmd)
    print(f"\n[Worker {wid}] EXECUTING COMMAND:\n{cmd_str}\n")
    yield f"data: {json.dumps({'worker_id': wid, 'line': f'CMD: {cmd_str}'})}\n\n"

    try:
        # Compatibility arguments for older python (3.6)
        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "bufsize": 1,
            "cwd": BASE_DIR
        }
        
        # Python 3.7+ uses text=True, older uses universal_newlines=True
        if sys.version_info >= (3, 7):
            kwargs["text"] = True
        else:
            kwargs["universal_newlines"] = True

        proc = subprocess.Popen(cmd, **kwargs)
        
        # Register process for stopping
        if job_id:
            if job_id not in active_processes: active_processes[job_id] = []
            active_processes[job_id].append(proc)
        
        for line in iter(proc.stdout.readline, ''):
            if line:
                print(f"[Worker {wid} OUT]: {line.strip()}") 
                yield f"data: {json.dumps({'worker_id': wid, 'line': line.strip()})}\n\n"
            
        proc.wait()
        
        # Check return code. -15 or -9 means killed by us (User Stop)
        if proc.returncode == -15 or proc.returncode == -9:
             yield f"data: {json.dumps({'worker_id': wid, 'line': 'INFO: Job stopped by user'})}\n\n"
        elif proc.returncode != 0:
             err_msg = f"EXIT CODE: {proc.returncode}"
             yield f"data: {json.dumps({'worker_id': wid, 'line': err_msg})}\n\n"
             
    except Exception as e:
        err_msg = f"ERROR: Execution failed: {str(e)}"
        yield f"data: {json.dumps({'worker_id': wid, 'line': err_msg})}\n\n"

# --- FIXED DEVICE DETECTION ---
def get_devices():
    devices = []
    try:
        cmd = "lsblk"
        if os.path.exists("/usr/bin/lsblk"): cmd = "/usr/bin/lsblk"
        elif os.path.exists("/bin/lsblk"): cmd = "/bin/lsblk"

        # Compat args
        kwargs = {"capture_output": True, "text": True}
        if sys.version_info < (3, 7):
             kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "universal_newlines": True}

        result = subprocess.run(
            [cmd, "-o", "NAME,SIZE,MODEL,TYPE,MOUNTPOINT", "-J"],
            **kwargs
        )
        
        if result.stdout:
            data = json.loads(result.stdout)
            
            def has_os_mount(node):
                if node.get("mountpoint") in ["/", "/boot", "/boot/efi", "/etc", "/usr"]:
                    return True
                for child in node.get("children", []):
                    if has_os_mount(child):
                        return True
                return False

            for dev in data.get("blockdevices", []):
                if dev.get("type") != "disk": continue
                path = f"/dev/{dev['name']}"
                is_os = has_os_mount(dev)
                devices.append({
                    "path": path,
                    "size": dev.get("size", "Unknown"),
                    "model": dev.get("model") or "N/A",
                    "disabled": is_os,
                    "disabled_reason": "OS Disk - Locked for Safety" if is_os else None
                })
    except Exception as e:
        print(f"Error getting devices: {e}")
        pass
    return devices

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
