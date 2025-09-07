from flask import Flask, jsonify, render_template_string
from threading import Thread
import time
import os
import requests
import random
import threading
import hashlib
import gc
import psutil
import json

app = Flask(__name__)
start_time = time.time()

# HTML template with auto-refresh for continuous activity
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Bot Manga Downloader - Keep Alive</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="300">
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #1a1a1a; color: #fff; }
        .container { max-width: 600px; margin: 0 auto; text-align: center; }
        .status { background: #2d2d2d; padding: 20px; border-radius: 10px; margin: 20px 0; }
        .online { color: #4CAF50; }
        .time { color: #FFC107; }
        .refresh { color: #2196F3; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Bot Manga Downloader</h1>
        <div class="status">
            <h2 class="online">✅ Status: ONLINE</h2>
            <p class="time">⏰ Uptime: {{ uptime_hours }}h {{ uptime_minutes }}m</p>
            <p>📅 Started: {{ start_date }}</p>
            <p>🔄 Last refresh: {{ current_time }}</p>
        </div>
        <div class="status">
            <p class="refresh">🔄 Auto-refresh setiap 5 menit untuk keep-alive</p>
            <p>💡 Halaman ini membantu mencegah Replit sleep</p>
            <p>🚀 Bookmark halaman ini dan buka sesekali</p>
        </div>
        <div class="status">
            <p>📱 Bot Commands:</p>
            <p>/start - Mulai bot</p>
            <p>/manga - Download mode normal</p>
            <p>/komik - Download mode comic</p>
            <p>/clear - Hapus pesan</p>
        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    uptime_seconds = int(time.time() - start_time)
    uptime_hours = uptime_seconds // 3600
    uptime_minutes = (uptime_seconds % 3600) // 60
    
    return render_template_string(HTML_TEMPLATE,
        uptime_hours=uptime_hours,
        uptime_minutes=uptime_minutes,
        start_date=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
        current_time=time.strftime('%Y-%m-%d %H:%M:%S')
    )

@app.route('/health')
def health():
    uptime = int(time.time() - start_time)
    return jsonify({
        "status": "healthy",
        "uptime_seconds": uptime,
        "deployment": os.getenv("REPLIT_DEPLOYMENT") == "1",
        "timestamp": int(time.time()),
        "bot_status": "running",
        "keep_alive": True
    })

@app.route('/ping')
def ping():
    responses = ["pong! 🏓", "alive! ✅", "running! 🚀", "active! ⚡"]
    return random.choice(responses)

@app.route('/wake')
def wake():
    return jsonify({
        "message": "Bot awakened!",
        "timestamp": int(time.time()),
        "status": "active"
    })

# Ultra-aggressive keep-alive endpoints
@app.route('/heartbeat')
def heartbeat():
    # Simulate CPU activity to prevent idle detection
    dummy_hash = hashlib.md5(str(time.time()).encode()).hexdigest()
    return jsonify({
        "heartbeat": "alive",
        "hash": dummy_hash,
        "timestamp": int(time.time()),
        "uptime": int(time.time() - start_time)
    })

@app.route('/activity')
def activity():
    # Show memory and CPU activity
    try:
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent()
        # Force some computation to show activity
        waste_cycles = sum(range(1000))
        return jsonify({
            "active": True,
            "memory_percent": memory.percent,
            "cpu_percent": cpu,
            "computation": waste_cycles,
            "timestamp": int(time.time())
        })
    except:
        return jsonify({"active": True, "timestamp": int(time.time())})

@app.route('/force-alive')
def force_alive():
    # Most aggressive endpoint - forces activity
    # Do multiple things to show we're very active
    current_time = time.time()
    data = []
    for i in range(100):
        data.append(hashlib.md5(f"{current_time}{i}".encode()).hexdigest())
    
    return jsonify({
        "status": "FORCE_ALIVE",
        "computed_hashes": len(data),
        "sample_hash": data[0] if data else "none",
        "timestamp": int(current_time),
        "message": "Bot is ACTIVELY running and WILL NOT SLEEP!"
    })

@app.route('/status/full')
def full_status():
    # Comprehensive status that does lots of work
    try:
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        cpu = psutil.cpu_percent(interval=0.1)  # Small interval to show activity
        
        # Generate some work
        work_data = {
            "hash_" + str(i): hashlib.md5(f"work_{time.time()}_{i}".encode()).hexdigest() 
            for i in range(20)
        }
        
        return jsonify({
            "system": {
                "memory_used": memory.percent,
                "memory_available": memory.available,
                "disk_used": disk.percent,
                "cpu_percent": cpu,
                "uptime": int(time.time() - start_time)
            },
            "bot_status": "ULTRA_ACTIVE",
            "work_completed": work_data,
            "timestamp": int(time.time()),
            "keep_alive_level": "MAXIMUM"
        })
    except Exception as e:
        return jsonify({
            "status": "ACTIVE_FALLBACK",
            "error": str(e),
            "timestamp": int(time.time())
        })

def run():
    app.run(host='0.0.0.0', port=5000, debug=False)

def keep_alive():
    print("🌐 Starting enhanced keep-alive server on port 5000...")
    print("💡 Tip: Bookmark https://[your-repl-url] and open it occasionally to prevent sleep")
    t = Thread(target=run)
    t.daemon = True
    t.start()

# DYNAMIC MULTI-THREADED KEEP-ALIVE SYSTEM
def start_ultra_aggressive_keepalive():
    # Import the global mode variable from main
    import main
    
    # Thread 1: Dynamic basic pings
    def ultra_fast_ping():
        endpoints = ["/ping", "/health", "/heartbeat", "/wake"]
        while True:
            try:
                # Check current mode and adjust interval
                current_mode = getattr(main, 'keep_alive_mode', 'ultra_aggressive')
                if current_mode == "ultra_aggressive":
                    interval = 30  # 30 seconds for ultra aggressive
                    mode_text = "🚀 ULTRA-FAST"
                else:
                    interval = 180  # 3 minutes for normal
                    mode_text = "😌 NORMAL"
                
                time.sleep(interval)
                endpoint = random.choice(endpoints)
                try:
                    response = requests.get(f"http://localhost:5000{endpoint}", timeout=5)
                    if response.status_code == 200:
                        print(f"{mode_text} ping successful: {endpoint}")
                except Exception as ping_error:
                    print(f"⚠️ {mode_text} ping failed: {ping_error}")
            except Exception as e:
                pass

    # Thread 2: Dynamic activity pings
    def activity_ping():
        while True:
            try:
                # Check current mode and adjust interval
                current_mode = getattr(main, 'keep_alive_mode', 'ultra_aggressive')
                if current_mode == "ultra_aggressive":
                    interval = 45  # 45 seconds for ultra aggressive
                    mode_text = "💪 ACTIVITY"
                else:
                    interval = 300  # 5 minutes for normal
                    mode_text = "💤 LIGHT-ACTIVITY"
                
                time.sleep(interval)
                
                # Only do intensive activity monitoring in ultra-aggressive mode
                if current_mode == "ultra_aggressive":
                    endpoint = "/activity"
                else:
                    endpoint = "/health"  # Use lighter endpoint in normal mode
                
                try:
                    response = requests.get(f"http://localhost:5000{endpoint}", timeout=5)
                    if response.status_code == 200:
                        print(f"{mode_text} ping successful - showing system status")
                except Exception as ping_error:
                    print(f"⚠️ {mode_text} ping failed: {ping_error}")
            except Exception as e:
                pass

    # Thread 3: Dynamic force-alive pings
    def force_alive_ping():
        while True:
            try:
                # Check current mode and adjust behavior
                current_mode = getattr(main, 'keep_alive_mode', 'ultra_aggressive')
                if current_mode == "ultra_aggressive":
                    interval = 60  # 1 minute for ultra aggressive
                    endpoint = "/force-alive"
                    mode_text = "🔥 FORCE-ALIVE"
                else:
                    interval = 600  # 10 minutes for normal
                    endpoint = "/ping"  # Use lighter endpoint
                    mode_text = "😌 KEEP-ALIVE"
                
                time.sleep(interval)
                try:
                    response = requests.get(f"http://localhost:5000{endpoint}", timeout=5)
                    if response.status_code == 200:
                        print(f"{mode_text} ping successful - {'maximum aggression!' if current_mode == 'ultra_aggressive' else 'standard monitoring'}")
                except Exception as ping_error:
                    print(f"⚠️ {mode_text} ping failed: {ping_error}")
            except Exception as e:
                pass

    # Thread 4: Dynamic status monitoring
    def full_status_ping():
        while True:
            try:
                # Check current mode and adjust behavior
                current_mode = getattr(main, 'keep_alive_mode', 'ultra_aggressive')
                if current_mode == "ultra_aggressive":
                    interval = 90  # 90 seconds for ultra aggressive
                    endpoint = "/status/full"
                    mode_text = "📊 FULL-STATUS"
                else:
                    interval = 900  # 15 minutes for normal
                    endpoint = "/health"  # Use lighter endpoint
                    mode_text = "📋 STATUS-CHECK"
                
                time.sleep(interval)
                try:
                    timeout = 10 if current_mode == "ultra_aggressive" else 5
                    response = requests.get(f"http://localhost:5000{endpoint}", timeout=timeout)
                    if response.status_code == 200:
                        print(f"{mode_text} ping successful - {'comprehensive system check' if current_mode == 'ultra_aggressive' else 'basic health check'}")
                except Exception as ping_error:
                    print(f"⚠️ {mode_text} ping failed: {ping_error}")
            except Exception as e:
                pass

    # Thread 5: Dynamic resource cycling
    def resource_cycling():
        while True:
            try:
                # Check current mode and adjust behavior
                current_mode = getattr(main, 'keep_alive_mode', 'ultra_aggressive')
                if current_mode == "ultra_aggressive":
                    interval = 120  # 2 minutes for ultra aggressive
                    work_intensity = 10000
                    memory_activity = 1000
                    mode_text = "🔄 RESOURCE-CYCLING"
                else:
                    interval = 600  # 10 minutes for normal
                    work_intensity = 1000  # Lighter computation
                    memory_activity = 100
                    mode_text = "🔄 LIGHT-CYCLING"
                
                time.sleep(interval)
                
                # Force garbage collection
                gc.collect()
                
                # Adjustable computation based on mode
                dummy_work = 0
                for i in range(work_intensity):
                    dummy_work += i * 2
                
                # Adjustable memory activity
                temp_data = [str(time.time() + i) for i in range(memory_activity)]
                temp_data.clear()
                
                print(f"{mode_text} completed - dummy work: {dummy_work}")
                
            except Exception as e:
                pass

    # Start all aggressive threads
    threads = [
        ("UltraFastPing", ultra_fast_ping),
        ("ActivityPing", activity_ping), 
        ("ForceAlivePing", force_alive_ping),
        ("FullStatusPing", full_status_ping),
        ("ResourceCycling", resource_cycling)
    ]
    
    for name, func in threads:
        thread = threading.Thread(target=func, name=name)
        thread.daemon = True
        thread.start()
        print(f"🚀 {name} thread started - ULTRA AGGRESSIVE MODE")
    
    print("🔥 DYNAMIC KEEP-ALIVE SYSTEM ACTIVATED - MODE RESPONSIVE!")

# Compatibility function for existing code
def start_self_ping():
    start_ultra_aggressive_keepalive()