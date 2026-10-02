#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Quake 代理：全部数据源"""
import http.server, socketserver, json, time, threading, os
import websocket

PORT = 18080
BASE = os.path.dirname(os.path.abspath(__file__))
tf = os.path.join(BASE, "token.txt")
TOKEN = open(tf, encoding="utf-8").read().strip() if os.path.exists(tf) else ""

L = threading.Lock()
EEW = []
HIST = []
STATIONS = []
VA_LIST = []
WEATHER_LIST = []
CHEJI_LIST = []
NEA_LIST = []
TSUNAMI_LIST = []
TYPHOON_LIST = []
CONN = {}

def set_conn(name, ok):
    with L:
        CONN[name] = "已连接" if ok else "未连接"

def put(lst, item, key, n=500):
    with L:
        lst[:] = [x for x in lst if str(x.get(key)) != str(item.get(key))]
        lst.insert(0, item)
        del lst[n:]

def W(p):
    return "wss://api.2v8.cn/ws/" + p + "?token=" + TOKEN

# ===== /ws/all 聚合 =====
def all_on(ws_app, raw):
    try:
        items = json.loads(raw)
        if not isinstance(items, list): items = [items]
        for item in items:
            src = item.get("source", "")
            d = item.get("Data")
            if not isinstance(d, dict): continue
            eid = d.get("id")
            if not eid: continue
            try: m = float(d.get("magnitude") or 0)
            except: m = 0
            nm = d.get("placeName", "") or ""
            is_eew = src.endswith("_eew") or src in ("cea","cea-pr","cea_all","early_est")
            e = {"EventID": src.upper() + "_" + str(eid),
                 "ReportNum": d.get("updates", 1),
                 "Latitude": d.get("latitude", 0),
                 "Longitude": d.get("longitude", 0),
                 "Depth": d.get("depth", 0),
                 "Magnitude": m,
                 "HypoCenter": "[" + src.upper() + "]" + nm,
                 "OriginTime": d.get("shockTime", "") or "",
                 "MaxIntensity": d.get("maxIntensity","") or d.get("epiIntensity","") or "",
                 "isFinal": bool(d.get("final", False)),
                 "type": src,
                 "_kind": "eew" if is_eew else "hist"}
            if is_eew:
                put(EEW, e, "EventID", 2000)
            else:
                he = {"id": str(eid), "O_TIME": e["OriginTime"],
                      "EPI_LAT": str(e["Latitude"]), "EPI_LON": str(e["Longitude"]),
                      "EPI_DEPTH": e["Depth"], "M": str(m),
                      "LOCATION_C": e["HypoCenter"], "_kind": "hist"}
                put(HIST, he, "id", 500)
            print("[" + src.upper() + "] M" + str(m), flush=True)
    except Exception as e:
        print("[all err]", e, flush=True)

def all_conn():
    time.sleep(1)
    while True:
        try:
            def on_open(w):
                print("[ALL]已连接", flush=True)
                set_conn("ALL", True)
            def on_err(w, e): set_conn("ALL", False)
            def on_close(w, c, m): set_conn("ALL", False)
            ws = websocket.WebSocketApp(W("all"),
                on_open=on_open, on_message=all_on,
                on_error=on_err, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception: pass
        time.sleep(30)

# ===== 测站 =====
_SM = {}
def pga_to_intensity_gal(g):
    if g < 0.5: return 0
    if g < 1.4: return 2
    if g < 4.5: return 3
    if g < 10: return 4
    if g < 25: return 5
    if g < 44: return 6
    if g < 80: return 7
    if g < 140: return 8
    if g < 250: return 9
    if g < 440: return 10
    if g < 800: return 11
    return 12

def station_on(source):
    def handler(ws_app, raw):
        try:
            o = json.loads(raw)
            if not isinstance(o, dict): return
            if "stations" in o and isinstance(o["stations"], list):
                with L: _SM[source] = o["stations"]
                print("[" + source + "测站] 表 " + str(len(o["stations"])), flush=True)
                return
            if "Data" in o and isinstance(o["Data"], dict):
                d = o["Data"]
                is_pga = "pga" in d
                arr = d.get("pga") or d.get("shindo") or d.get("mmi")
                if not arr or not isinstance(arr, list): return
                with L:
                    st_map = _SM.get(source, [])
                    new_list = []
                    for idx, val in enumerate(arr):
                        if idx >= len(st_map): break
                        st = st_map[idx]
                        try: inten = float(val)
                        except: continue
                        if is_pga:
                            inten = pga_to_intensity_gal(inten)
                        # 不再过滤，负值也保留（前端按"无数据/无感"显示）
                        new_list.append({
                            "id": st.get("id", source + "_" + str(idx)),
                            "name": source + "-" + str(idx+1),
                            "lat": float(st.get("latitude", 0)),
                            "lng": float(st.get("longitude", 0)),
                            "intensity": inten,
                            "source": source,
                            "type": "ocean" if source == "snet" else "land"
                        })
                    STATIONS[:] = [s for s in STATIONS if s.get("source") != source] + new_list
                    print("[" + source + "测站] " + str(len(new_list)), flush=True)
        except Exception as e:
            print("[" + source + " err]", e, flush=True)
    return handler

def station_conn(url, name):
    time.sleep(1)
    while True:
        try:
            def on_open(w):
                print("[" + name + "测站]已连接", flush=True)
                set_conn(name + "测站", True)
            def on_err(w, e): set_conn(name + "测站", False)
            def on_close(w, c, m): set_conn(name + "测站", False)
            ws = websocket.WebSocketApp(url,
                on_open=on_open, on_message=station_on(name),
                on_error=on_err, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception: pass
        time.sleep(60)

# ===== 通用灾害 =====
def simple_on(lst, tag, key="id"):
    def handler(ws_app, raw):
        try:
            arr = json.loads(raw)
            if not isinstance(arr, list): arr = [arr]
            for it in arr:
                d = it.get("Data") if isinstance(it, dict) else None
                if not isinstance(d, dict): continue
                eid = d.get(key) or d.get("id") or d.get("code") or d.get("eventId")
                if not eid: continue
                e = dict(d); e["type"] = tag
                put(lst, e, key, 100)
                print("[" + tag + "] 收到", flush=True)
        except Exception: pass
    return handler

def simple_conn(url, name, lst, tag, key="id"):
    while True:
        try:
            def on_open(w):
                print("[" + tag + "]已连接", flush=True)
                set_conn(name, True)
            def on_err(w, e): set_conn(name, False)
            def on_close(w, c, m): set_conn(name, False)
            ws = websocket.WebSocketApp(url,
                on_open=on_open, on_message=simple_on(lst, tag, key),
                on_error=on_err, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception: pass
        time.sleep(60)

# ===== GQ / USGS =====
def aloys_on(tag):
    def f(ws_app, raw):
        try:
            o = json.loads(raw)
            if not isinstance(o, dict): return
            pl = o.get("Data") or o.get("payload") or {}
            eid = pl.get("id") or pl.get("eventId")
            if not eid: return
            try: m = round(float(pl.get("magnitude") or 0), 2)
            except: m = 0
            try: dep = round(float(pl.get("depth") or 0), 2)
            except: dep = 0
            ot = pl.get("shockTime") or pl.get("originTime") or pl.get("time") or ""
            e = {"EventID": tag.upper() + "_" + str(eid),
                 "ReportNum": pl.get("revision", pl.get("updates", 1)),
                 "Latitude": round(float(pl.get("latitude") or 0), 4),
                 "Longitude": round(float(pl.get("longitude") or 0), 4),
                 "Depth": dep,
                 "Magnitude": m,
                 "HypoCenter": "[" + tag.upper() + "]" + (pl.get("placeName") or pl.get("region") or ""),
                 "OriginTime": ot,
                 "MaxIntensity": "", "isFinal": False,
                 "type": tag, "_kind": "eew"}
            put(EEW, e, "EventID")
            print("[" + tag.upper() + "] M" + str(m), flush=True)
        except Exception: pass
    return f

def aloys_conn(tag, url):
    time.sleep(1)
    while True:
        try:
            def on_open(w):
                print("[" + tag.upper() + "]已连接", flush=True)
                set_conn(tag.upper(), True)
            def on_err(w, e): set_conn(tag.upper(), False)
            def on_close(w, c, m): set_conn(tag.upper(), False)
            ws = websocket.WebSocketApp(url,
                on_open=on_open, on_message=aloys_on(tag),
                on_error=on_err, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception: pass
        time.sleep(30)

# ===== HTTP =====
def send(h, obj):
    b = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    h.send_response(200)
    h.send_header("Content-Type", "application/json; charset=utf-8")
    h.send_header("Access-Control-Allow-Origin", "*")
    h.send_header("Content-Length", str(len(b)))
    h.end_headers()
    h.wfile.write(b)

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        p = self.path.split("?")[0]
        if p in ("/all_eew", "/cenc_eew.json"):
            with L: send(self, list(EEW))
        elif p in ("/history", "/cenc_eqlist.json"):
            with L: send(self, {"shuju": list(HIST)})
        elif p in ("/stations", "/station", "/station.json"):
            with L: send(self, {"data": list(STATIONS)})
        elif p == "/volcano":
            with L: send(self, list(VA_LIST))
        elif p == "/weather":
            with L: send(self, list(WEATHER_LIST))
        elif p == "/cheji":
            with L: send(self, list(CHEJI_LIST))
        elif p == "/nea":
            with L: send(self, list(NEA_LIST))
        elif p == "/tsunami":
            with L: send(self, list(TSUNAMI_LIST))
        elif p == "/typhoon":
            try:
                import subprocess
                raw = subprocess.check_output(
                    ["curl","-s","--max-time","10","-H","User-Agent: Mozilla/5.0",
                     "http://typhoon.nmc.cn/weatherservice/typhoon/jsons/list_default"],
                    stderr=subprocess.DEVNULL).decode("utf-8", errors="ignore")
                st = raw.find("{")
                en = raw.rfind("}") + 1
                data = json.loads(raw[st:en])
                tlist = data.get("typhoonList") or []
                out = []
                for t in tlist:
                    if not isinstance(t, list) or len(t) < 8: continue
                    out.append({
                        "id": str(t[0]),
                        "name": t[2] or "",
                        "enname": t[1] or "",
                        "tc_num": str(t[3]) if t[3] else "",
                        "isactive": "1" if str(t[7]) == "start" else "0",
                        "state": t[7] or ""
                    })
                out.sort(key=lambda x: (0 if x["isactive"]=="1" else 1, -int(x["tc_num"]) if x["tc_num"].isdigit() else 0))
                send(self, out)
            except Exception as e:
                print("[typhoon]", e, flush=True)
                send(self, [])
        elif p == "/typhoon_detail":
            try:
                import subprocess
                qs = self.path.split("?")[1] if "?" in self.path else ""
                tid = ""
                for kv in qs.split("&"):
                    if kv.startswith("id="): tid = kv[3:]; break
                raw = subprocess.check_output(
                    ["curl","-s","--max-time","10","-H","User-Agent: Mozilla/5.0",
                     "http://typhoon.nmc.cn/weatherservice/typhoon/jsons/view_" + tid],
                    stderr=subprocess.DEVNULL).decode("utf-8", errors="ignore")
                st = raw.find("{"); en = raw.rfind("}") + 1
                data = json.loads(raw[st:en]) if st >= 0 and en > st else {}
                out = {"id": tid, "name": "", "enname": "", "points": [], "forecasts": {}}
                ty = data.get("typhoon") or []
                if isinstance(ty, list) and len(ty) > 8:
                    out["name"] = ty[2] or ""; out["enname"] = ty[1] or ""
                    smap = {"TD":"热带低压","TS":"热带风暴","STS":"强热带风暴","TY":"台风","STY":"强台风","SuperTY":"超强台风"}
                    for pt in (ty[8] or []):
                        if isinstance(pt, list) and len(pt) >= 6:
                            out["points"].append({"time": pt[1], "lat": pt[5], "lng": pt[4], "strong": smap.get(pt[3], pt[3] or "")})
                            fdict = None
                            for idx in (11, 10, 12):
                                if len(pt) > idx and isinstance(pt[idx], dict) and any(isinstance(v, list) for v in pt[idx].values()):
                                    fdict = pt[idx]; break
                            if fdict:
                                for org, fcst in fdict.items():
                                    if not isinstance(fcst, list): continue
                                    if org not in out["forecasts"]: out["forecasts"][org] = []
                                    for f in fcst:
                                        if isinstance(f, list) and len(f) >= 4:
                                            sv = f[7] if len(f) > 7 else ""
                                            out["forecasts"][org].append({"time": f[1], "lat": f[3], "lng": f[2], "strong": smap.get(sv, sv)})
                send(self, out)
            except Exception as e:
                print("[typhoon_detail]", e, flush=True)
                send(self, {"id": "", "points": [], "forecasts": {}})
        elif p == "/source_list":
            with L: send(self, dict(CONN))
        elif p == "/quake":
            f = os.path.join(BASE, "quake.html")
            if os.path.exists(f):
                b = open(f, "rb").read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
            else:
                self.send_response(404); self.end_headers()
        elif p.startswith("/static/"):
            fpath = os.path.join(BASE, p.lstrip("/"))
            if os.path.exists(fpath):
                b = open(fpath, "rb").read()
                ctype = "text/css" if p.endswith(".css") else "application/javascript"
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
            else:
                self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *a): pass

class S(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

print("[代理] 启动，端口 " + str(PORT), flush=True)
print("[代理] Token " + TOKEN[:20] + "...", flush=True)

threading.Thread(target=all_conn, daemon=True).start()
threading.Thread(target=station_conn, args=(W("nied"), "nied"), daemon=True).start()
threading.Thread(target=station_conn, args=(W("snet"), "snet"), daemon=True).start()
def kma_conn():
    time.sleep(1)
    while True:
        try:
            def on_open(w):
                print("[kma]已连接", flush=True)
                set_conn("kma", True)
            def on_err(w, e): set_conn("kma", False)
            def on_close(w, c, m): set_conn("kma", False)
            ws = websocket.WebSocketApp(W("kma"),
                on_open=on_open, on_message=all_on,
                on_error=on_err, on_close=on_close)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception: pass
        time.sleep(30)

threading.Thread(target=kma_conn, daemon=True).start()
threading.Thread(target=station_conn, args=(W("palert"), "palert"), daemon=True).start()
threading.Thread(target=station_conn, args=(W("kma_station"), "kma_station"), daemon=True).start()
threading.Thread(target=station_conn, args=(W("trem"), "trem"), daemon=True).start()
threading.Thread(target=simple_conn, args=(W("va"), "火山", VA_LIST, "火山"), daemon=True).start()
threading.Thread(target=simple_conn, args=(W("weatheralarm"), "气象", WEATHER_LIST, "气象"), daemon=True).start()
threading.Thread(target=simple_conn, args=(W("cheji"), "灾害", CHEJI_LIST, "灾害"), daemon=True).start()
threading.Thread(target=simple_conn, args=(W("nea"), "小行星", NEA_LIST, "小行星"), daemon=True).start()
threading.Thread(target=simple_conn, args=(W("tsunami"), "海啸", TSUNAMI_LIST, "海啸"), daemon=True).start()
threading.Thread(target=aloys_conn, args=("gq", "wss://api.aloys23.link/api/v1/alert/ws/quake/gq"), daemon=True).start()
threading.Thread(target=aloys_conn, args=("usgs", "wss://api.aloys23.link/api/v1/alert/ws/quake/usgs"), daemon=True).start()

print("[代理] 已启动所有数据源", flush=True)
S(("127.0.0.1", PORT), H).serve_forever()
