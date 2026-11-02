# Starts station in its own process group, waits for its ports, then sends
# CTRL_BREAK and waits for a clean exit.
import os, signal, socket, subprocess, sys, time

exe, log_path, *args = sys.argv[1:]
log = open(log_path, "wb")
p = subprocess.Popen([exe, *args], stdout=log, stderr=subprocess.STDOUT,
                     creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
for _ in range(120):
    try:
        socket.create_connection(("127.0.0.1", 8888), timeout=1).close()
        break
    except OSError:
        time.sleep(0.5)
time.sleep(float(os.environ.get("HOLD", "3")))
t = time.time()
p.send_signal(signal.CTRL_BREAK_EVENT)
try:
    rc = p.wait(timeout=60)
except subprocess.TimeoutExpired:
    p.kill()
    print("  did not exit within 60 s after ctrl-break")
    sys.exit(1)
print(f"  exit code {rc} after {time.time() - t:.1f} s")
sys.exit(rc)
