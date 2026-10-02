# -*- coding: utf-8 -*-
import io, os

PATH = "/home/tiny/app/Quake/quake.html"
with io.open(PATH, encoding="utf-8", errors="replace") as f:
    lines = f.readlines()

KEYS = ["shake", "Shake", "SHAKE", "circle", "Circle", "intensity", "Intensity",
        "jma", "JMA", "摇晃", "震度", "polygon", "Polygon", "geojson", "GeoJSON",
        "feature", "Feature", "area", "Area", "radius", "Radius", "station",
        "Station", "summary", "18080"]

print("== 文件总行数: %d ==" % len(lines))
print("== 命中行 ==")
for i, ln in enumerate(lines, 1):
    if any(k in ln for k in KEYS):
        s = ln.rstrip("\n")
        if len(s) > 200:
            s = s[:200] + " ..."
        print("%5d| %s" % (i, s))
