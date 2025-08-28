/** @jsx React.createElement */
const {useState, useEffect, useRef, useMemo} = React;

/* =========================
   LASEK – Map Explorer UI
   ========================= */

const API_KEY = 'YOUR_API_KEY';

/* Dataset-specific example prompts */
const DATASET_PROMPTS = {
  "NE_countries.geojson": [
    "Visualize population estimates with a graduated color ramp on pop_est",
    "Categorize countries by continent using distinct colors on continent",
    "Label countries by name with subtle halo",
    "Highlight GDP estimates using graduated colors on gdp_md_est"
  ], "Countries.geojson": [
    "Color countries based on their income group",
    "Categorize countries by continent using distinct colors on continent",
    "Show countries population with graduated scheme",
    "Display country names",
      "Color all countries in Europe in gold and all other countries in red",
      "Show France in red and all other countries in blue"
  ], "Roads.geojson": [
    "Show toll roads with a different color",
    "Color roads based on length",
    "Distinguish different types of roads, e.g. highways vs regular roads "
  ], "Chicago_Crimes.geojson": [
    "Show top 10 crime types with distinct colors",
    "Highlight domestic crimes",
    "Display data from different districts with different colors"
  ], "Populated_places.geojson": [
    "Distinguish different timezones",
    "Show country names as labels",
    "Show province names as labels"
  ]
};
const DEFAULT_PROMPTS = [
  "Visualize a numeric attribute with graduated colors",
  "Categorize a string attribute with distinct colors",
  "Show uniform styling for all features",
  "Enable labels for a key attribute"
];
const displayName = (s) => s.replace(/\.geojson$/i, "");

/* ---------- Color helpers ---------- */
const clamp01 = (t) => Math.max(0, Math.min(1, t));
const hexToRgb = (hex) => {
  const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex || "");
  return m ? {r: parseInt(m[1], 16), g: parseInt(m[2], 16), b: parseInt(m[3], 16)} : {r: 0, g: 0, b: 0};
};
const rgbToHex = (r, g, b) => {
  const h = (x) => x.toString(16).padStart(2, '0');
  return `#${h(Math.round(r))}${h(Math.round(g))}${h(Math.round(b))}`;
};
const lerp = (a, b, t) => a + (b - a) * t;

// Fixed gradientColor (blue channel interpolates)
const gradientColor = (startHex = "#0000ff", endHex = "#ff0000", t = 0) => {
  const s = hexToRgb(startHex);
  const e = hexToRgb(endHex);
  t = clamp01(t);
  return rgbToHex(
    lerp(s.r, e.r, t),
    lerp(s.g, e.g, t),
    lerp(s.b, e.b, t)
  );
};

// ── Pronounced multi-stop ramp (blue → cyan → yellow → orange → red)
const PRONOUNCED_STOPS = [
  { pos: 0.00, color: "#08306B" }, // deep blue
  { pos: 0.35, color: "#0FA3FF" }, // cyan
  { pos: 0.60, color: "#FFFF80" }, // yellow
  { pos: 0.80, color: "#FDAE61" }, // orange
  { pos: 1.00, color: "#D7191C" }  // red
];
const lerpHex = (aHex, bHex, t) => {
  const a = hexToRgb(aHex), b = hexToRgb(bHex);
  return rgbToHex(lerp(a.r,b.r,t), lerp(a.g,b.g,t), lerp(a.b,b.b,t));
};
const multiStopColor = (t, stops = PRONOUNCED_STOPS) => {
  t = clamp01(t);
  for (let i = 0; i < stops.length - 1; i++) {
    const A = stops[i], B = stops[i+1];
    if (t >= A.pos && t <= B.pos) {
      const local = (t - A.pos) / Math.max(1e-9, (B.pos - A.pos));
      return lerpHex(A.color, B.color, local);
    }
  }
  return t <= 0 ? stops[0].color : stops[stops.length-1].color;
};

/* ---------- Date helpers ---------- */
const pad2 = (x) => String(x).padStart(2, '0');
const toYMD = (d) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
const toYMDHMS = (d) => `${toYMD(d)} ${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`;
const parseYMD = (s) => {
  if (!s || typeof s !== 'string') return null;
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const d = new Date(+m[1], +m[2] - 1, +m[3]);
  return isNaN(d) ? null : d;
};
const daysInMonth = (y, m) => new Date(y, m + 1, 0).getDate();
const sameDay = (a, b) => a && b && a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
const inRangeDateOnly = (d, min, max) => (!min || d >= min) && (!max || d <= max);

/* ---------- Parse/Join DateTime strings ---------- */
const splitDateTime = (val) => {
  if (!val) return {date: "", h: 0, m: 0, s: 0, hasTime: false};
  const [dPart, tPartRaw] = String(val).includes('T')
    ? String(val).split('T')
    : String(val).split(' ');
  let h = 0, m = 0, s = 0, hasTime = false;
  const tPart = tPartRaw;
  if (tPart) {
    const mm = tPart.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (mm) { h = +mm[1]; m = +mm[2]; s = +(mm[3] || 0); hasTime = true; }
  }
  return {date: dPart, h, m, s, hasTime};
};
const joinDateTime = (date, h = 0, m = 0, s = 0, includeTime = false) => {
  if (!date) return "";
  if (!includeTime) return date;
  return `${date}T${pad2(h)}:${pad2(m)}:${pad2(s)}`;
};
const parseFilterToDate = (val, role /* 'start' | 'end' */) => {
  if (!val) return null;
  const {date, h, m, s, hasTime} = splitDateTime(val);
  const d = parseYMD(date);
  if (!d) return null;
  if (hasTime) return new Date(d.getFullYear(), d.getMonth(), d.getDate(), h, m, s, 0);
  if (role === 'end') return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23, 59, 59, 999);
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0);
};

/* ---------- UI styles ---------- */
const colorBoxStyle = {
  width: 42, height: 32, border: '1px solid #bbb', borderRadius: 6, padding: 0, margin: 0, background: 'transparent', cursor: 'pointer'
};

/* ---------- Calendar + time dropdowns (always show time section) ---------- */
function DateTimePicker({
  label, value, onChange, min, max,
  granularity /* { hour, minute, second } or undefined */,
  defaultTimeRole = 'start'
}) {
  const g = granularity || {hour: true, minute: true, second: true};

  const parsed = splitDateTime(value);
  const minD = parseYMD(min);
  const maxD = parseYMD(max);
  const today = new Date();

  const base = parsed.date ? parseYMD(parsed.date) : (minD || today);
  const [viewYear, setViewYear] = useState(base.getFullYear());
  const [viewMonth, setViewMonth] = useState(base.getMonth());
  const [selDate, setSelDate] = useState(parsed.date || toYMD(base));

  const def = defaultTimeRole === 'end' ? {h: 23, m: 59, s: 59} : {h: 0, m: 0, s: 0};
  const [H, setH] = useState(parsed.hasTime ? parsed.h : def.h);
  const [M, setM] = useState(parsed.hasTime ? parsed.m : def.m);
  const [S, setS] = useState(parsed.hasTime ? parsed.s : def.s);

  useEffect(() => {
    const p = splitDateTime(value);
    if (p.date) {
      setSelDate(p.date);
      const d = parseYMD(p.date);
      if (d) { setViewYear(d.getFullYear()); setViewMonth(d.getMonth()); }
    } else {
      const b = minD || today;
      setSelDate(toYMD(b));
      setViewYear(b.getFullYear()); setViewMonth(b.getMonth());
    }
    if (p.hasTime) { setH(p.h); setM(p.m); setS(p.s); }
    else { setH(def.h); setM(def.m); setS(def.s); }
  }, [value]); // eslint-disable-line

  const prevMonth = () => {
    let y = viewYear, m = viewMonth - 1;
    if (m < 0) { m = 11; y -= 1; }
    setViewYear(y); setViewMonth(m);
  };
  const nextMonth = () => {
    let y = viewYear, m = viewMonth + 1;
    if (m > 11) { m = 0; y += 1; }
    setViewYear(y); setViewMonth(m);
  };

  const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  const monthOptions = monthNames.map((name, idx) => ({ name, idx }));
  const yearStart = (minD ? minD.getFullYear() : viewYear - 50);
  const yearEnd   = (maxD ? maxD.getFullYear() : viewYear + 50);
  const yearOptions = []; for (let y = yearStart; y <= yearEnd; y++) yearOptions.push(y);

  const firstDay = new Date(viewYear, viewMonth, 1).getDay(); // 0=Sun
  const dim = daysInMonth(viewYear, viewMonth);
  const dow = ['Su','Mo','Tu','We','Th','Fr','Sa'];

  const weeks = [];
  let day = 1 - firstDay;
  for (let w=0; w<6; w++) {
    const cells = [];
    for (let d=0; d<7; d++, day++) {
      const inThisMonth = day >=1 && day <= dim;
      const thisDate = new Date(viewYear, viewMonth, Math.max(1, Math.min(dim, day)));
      const disabled = !inThisMonth || !inRangeDateOnly(thisDate, minD, maxD);
      const isSel = selDate && inThisMonth && sameDay(thisDate, parseYMD(selDate));
      const isToday = sameDay(thisDate, today) && inThisMonth;
      const cellDate = inThisMonth ? toYMD(new Date(viewYear, viewMonth, day)) : null;

      cells.push(
        <div key={d}
             onClick={()=>{ if (!disabled && cellDate) {
               setSelDate(cellDate);
               onChange(joinDateTime(cellDate, H, M, S, true));
             }}}
             style={{
               height: 28, display:'flex', alignItems:'center', justifyContent:'center',
               fontSize:12, cursor: disabled ? 'not-allowed' : 'pointer',
               color: disabled ? '#bbb' : '#222',
               borderRadius: 4,
               border: isSel ? '2px solid #1976d2' : '1px solid #eee',
               background: isSel ? '#e3f2fd' : (isToday ? '#f6faff' : 'transparent'),
               opacity: inThisMonth ? 1 : 0.35,
               userSelect: 'none'
             }}
             title={inThisMonth ? cellDate : ""}
        >
          {inThisMonth ? day : ''}
        </div>
      );
    }
    weeks.push(<div key={w} style={{display:'grid', gridTemplateColumns:'repeat(7, 1fr)', gap:4}}>{cells}</div>);
  }

  const hours = Array.from({length:24}, (_,i)=>i);
  const mins  = Array.from({length:60}, (_,i)=>i);
  const secs  = Array.from({length:60}, (_,i)=>i);

  return (
    <div style={{border:'1px solid #ddd', borderRadius:8, padding:8}}>
      {/* Header with Month/Year dropdowns */}
      <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:6, gap:6}}>
        <div style={{fontWeight:600}}>{label}{value ? `: ${value}` : ''}</div>
        <div style={{display:'flex', alignItems:'center', gap:6}}>
          <button onClick={prevMonth} title="Previous month">«</button>
          <select value={viewMonth} onChange={(e)=>setViewMonth(+e.target.value)} title="Month">
            {monthOptions.map(m => <option key={m.idx} value={m.idx}>{m.name}</option>)}
          </select>
          <select value={viewYear} onChange={(e)=>setViewYear(+e.target.value)} title="Year">
            {yearOptions.map(y => <option key={y} value={y}>{y}</option>)}
          </select>
          <button onClick={nextMonth} title="Next month">»</button>
        </div>
      </div>

      <div style={{display:'grid', gridTemplateColumns:'repeat(7, 1fr)', gap:4, marginBottom:4, color:'#666', fontSize:12}}>
        {dow.map((d,i)=><div key={i} style={{textAlign:'center'}}>{d}</div>)}
      </div>
      <div style={{display:'grid', gap:4}}>
        {weeks}
      </div>

      {/* Time section ALWAYS visible; disable selects based on granularity */}
      <div style={{marginTop:8}}>
        <div style={{fontSize:12, color:'#555', marginBottom:4}}>Time</div>
        <div style={{display:'grid', gridTemplateColumns:'repeat(3, minmax(0,1fr))', gap:6}}>
          <select
            value={H}
            disabled={!g.hour}
            onChange={(e)=>{ const v=+e.target.value; setH(v); onChange(joinDateTime(selDate, v, M, S, true)); }}
            title={g.hour ? "Hour" : "Hour (disabled for date-only field)"}
          >
            {hours.map(h=><option key={h} value={h}>{pad2(h)} h</option>)}
          </select>
          <select
            value={M}
            disabled={!g.minute}
            onChange={(e)=>{ const v=+e.target.value; setM(v); onChange(joinDateTime(selDate, H, v, S, true)); }}
            title={g.minute ? "Minute" : "Minute (disabled for date-only field)"}
          >
            {mins.map(m=><option key={m} value={m}>{pad2(m)} m</option>)}
          </select>
          <select
            value={S}
            disabled={!g.second}
            onChange={(e)=>{ const v=+e.target.value; setS(v); onChange(joinDateTime(selDate, H, M, v, true)); }}
            title={g.second ? "Second" : "Second (disabled for date-only field)"}
          >
            {secs.map(s=><option key={s} value={s}>{pad2(s)} s</option>)}
          </select>
        </div>
      </div>
    </div>
  );
}

/* ---------- MAIN APP ---------- */
function App() {
  /* ─── STATE ─────────────────────────────────────────────────────────────── */
  const [datasets, setDatasets] = useState([]);
  const [selected, setSelected] = useState("");
  const [schema, setSchema] = useState(null);
  const [sample, setSample] = useState(null);
  const [attributes, setAttributes] = useState([]);

  /* Styling & LLM */
  const [prompt, setPrompt] = useState("");
  const [colorConf, setColorConf] = useState({
    attribute: "",
    type: "basic",
    fillColor: "#2b6cb0",       // 🔵 default blue (better for roads/lines)
    strokeColor: "#333333",
    gradStart: "#0000ff",
    gradEnd: "#ff0000",
    catColors: {},
    otherColor: "#cccccc",
  });
  const [manual, setManual] = useState({
    attribute: "",
    styleType: "basic",
    fillColor: "#2b6cb0",       // 🔵 default blue
    strokeColor: "#333333",
    gradStart: "#0000ff",
    gradEnd: "#ff0000",
    catColors: {},
    otherColor: "#cccccc",
    labelAttribute: "",
    labelFill: "#000000",
    labelStroke: "#ffffff",
  });

  const [labelConf, setLabelConf] = useState({
    enabled: false,
    attribute: "",
    fillColor: "#000000",
    strokeColor: "#ffffff"
  });

  const [suggestions, setSuggestions] = useState([]);
  const [autoSuggestions, setAutoSuggestions] = useState([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [autoModalOpen, setAutoModalOpen] = useState(false);
  const [llmLoading, setLlmLoading] = useState(false);
  const [autoLoading, setAutoLoading] = useState(false);
  const [aiError, setAiError] = useState("");
  const [autoError, setAutoError] = useState("");

  /* Date/time filtering */
  const [datetimeAttrs, setDatetimeAttrs] = useState([]);
  const [timeRanges, setTimeRanges] = useState({}); // {attr:{min,max,minFull,maxFull}}
  const [timeGran, setTimeGran] = useState({});     // {attr:{hour,minute,second}}
  const [datetimeFilter, setDatetimeFilter] = useState({attribute: "", start: "", end: ""});

  // Cache tracking
  const [enhancedLoadedFromCache, setEnhancedLoadedFromCache] = useState(false);
  const [enhancedSaved, setEnhancedSaved] = useState(false);

  /* Schema modal & selected field */
  const [schemaModalOpen, setSchemaModalOpen] = useState(false);
  const [selectedField, setSelectedField] = useState(null);

  /* Attribute sample preview */
  const [attrSample, setAttrSample] = useState({name: "", values: []});

  /* Map */
  const mapDiv = useRef();
  const [mapObj, setMapObj] = useState(null);
  const [vectorLayer, setVectorLayer] = useState(null);

  /* Legend */
  let [legend, setLegend] = useState(null);

  /* ─── Performance: style caches ─────────────────────────────────────────── */
  const stylePool = useRef(new Map());       // base (geomType|fill|stroke) -> Style
  const textCache = useRef(new Map());       // (text|fill|stroke) -> Text
  const pointLabelCache = useRef(new Map()); // (fill|stroke|text|tfill|tstroke) -> Style
  const labelOnlyCache = useRef(new Map());  // (text|tfill|tstroke) -> Style

  const clearAllStyleCaches = () => {
    stylePool.current.clear();
    textCache.current.clear();
    pointLabelCache.current.clear();
    labelOnlyCache.current.clear();
  };

  /* ─── Load datasets ─────────────────────────────────────────────────────── */
  useEffect(() => {
    fetch('/datasets.json')
      .then(r => r.json())
      .then(setDatasets)
      .catch(console.error);
  }, []);

  /* ─── Init map ──────────────────────────────────────────────────────────── */
  useEffect(() => {
    if (!mapDiv.current || mapObj) return;
    const map = new ol.Map({
      target: mapDiv.current,
      layers: [new ol.layer.Tile({source: new ol.source.OSM()})],
      view: new ol.View({center: ol.proj.fromLonLat([0, 0]), zoom: 2})
    });
    mapDiv.current.style.width = '100%';
    mapDiv.current.style.height = '100%';
    setMapObj(map);
  }, [mapDiv, mapObj]);

  /* ─── On dataset select ─────────────────────────────────────────────────── */
  useEffect(() => {
    if (!mapObj) return;
    if (vectorLayer) {
      mapObj.removeLayer(vectorLayer);
      setVectorLayer(null);
    }
    /* Reset UI state */
    setSchema(null);
    setAttributes([]);
    setSample(null);
    setDatetimeAttrs([]);
    setTimeRanges({});
    setTimeGran({});
    setEnhancedLoadedFromCache(false);
    setEnhancedSaved(false);
    setDatetimeFilter({attribute: "", start: "", end: ""});
    setSuggestions([]);
    setAutoSuggestions([]);
    setPrompt("");
    setLabelConf(c => ({...c, enabled: false, attribute: ""}));
    setSchemaModalOpen(false);
    setSelectedField(null);
    setAttrSample({name: "", values: []});
    setLegend(null);
    setAiError(""); setAutoError("");
    setColorConf(c => ({...c, attribute: "", type: "basic", fillColor: "#2b6cb0"})); // ensure default blue
    clearAllStyleCaches();

    if (!selected) return;
    let canceled = false;

    /* schema */
    (async () => {
      const info = await fetch(`/datasets/${selected}.json`).then(r => r.json());
      if (!canceled && info.schema) {
        setSchema(info.schema);
        setAttributes(info.schema.map(f => f.name));
        const dt = info.schema
          .filter(f => f?.metadata?.isDatetime || /timestamp|date|datetime/i.test(String(f.type || "")))
          .map(f => f.name);
        setDatetimeAttrs(dt);
      }
    })().catch(console.error);

    /* preview sample */
    (async () => {
      try {
        const res = await fetch(`/datasets/${selected}/export`, {method: 'POST'});
        const samp = await res.json();
        if (!canceled) setSample(Array.isArray(samp) ? samp : (samp?.rows || samp));
      } catch (e) {
        console.warn("export preview failed, continuing:", e);
      }
    })();

    /* vector layer */
    const src = new ol.source.Vector({
      url: `/datasets/${selected}`,
      format: new ol.format.GeoJSON()
    });
    const lyr = new ol.layer.Vector({
      source: src,
      declutter: true,
      renderMode: 'vector',
      renderBuffer: 48,
      updateWhileInteracting: false,
      updateWhileAnimating: false
    });
    mapObj.addLayer(lyr);
    setVectorLayer(lyr);

    src.once('change', () => {
      if (src.getState() === 'ready') {
        const e = src.getExtent();
        if (!ol.extent.isEmpty(e)) mapObj.getView().fit(e, {padding: [20, 20, 20, 20]});
      }
    });

    return () => { canceled = true; };
  }, [selected, mapObj]);

  /* ─── Cache helpers (server) ───────────────────────────────────────────── */
  const loadEnhancedFromCache = async (name) => {
    try {
      const res = await fetch(`/datasets/${name}/enhanced_schema.json`, {cache: 'no-store'});
      if (!res.ok) return null;
      return await res.json();
    } catch { return null; }
  };
  const saveEnhancedToCache = async (name, payload) => {
    try {
      await fetch(`/datasets/${name}/enhanced_schema.json`, {
        method: 'PUT', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
      });
    } catch (e) { console.warn("saveEnhancedToCache failed", e); }
  };
  const loadAutoStyleFromCache = async (name) => {
    try {
      const res = await fetch(`/datasets/${name}/auto_style.json`, {cache: 'no-store'});
      if (!res.ok) return null;
      return await res.json();
    } catch { return null; }
  };
  const saveAutoStyleToCache = async (name, suggestions) => {
    try {
      await fetch(`/datasets/${name}/auto_style.json`, {
        method: 'PUT', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({suggestions})
      });
    } catch (e) { console.warn("saveAutoStyleToCache failed", e); }
  };

  /* ─── Try to load enhanced schema cache once schema is known ───────────── */
  useEffect(() => {
    if (!selected || !schema) return;
    (async () => {
      const cached = await loadEnhancedFromCache(selected);
      if (cached && (cached.timeRanges || cached.timeGran)) {
        setTimeRanges(cached.timeRanges || {});
        setTimeGran(cached.timeGran || {});
        setEnhancedLoadedFromCache(true);
      }
    })();
  }, [selected, schema]);

  /* ─── Compute ranges & granularity for one attr (only if needed) ───────── */
  const computeRangeAndGranFor = (attr) => {
    if (!vectorLayer || !attr) return;
    if (timeRanges[attr] && timeGran[attr]) return; // already have it
    const src = vectorLayer.getSource();
    const run = () => {
      const feats = src.getFeatures();
      if (!feats?.length) return;

      const raw = feats.map(f => f.get(attr)).filter(v => v != null);
      if (!raw.length) return;

      const ds = raw.map(v => new Date(v)).filter(d => !isNaN(d));
      if (ds.length) {
        const minD = new Date(Math.min(...ds));
        const maxD = new Date(Math.max(...ds));
        setTimeRanges(prev => ({
          ...prev,
          [attr]: {
            min: toYMD(minD),
            max: toYMD(maxD),
            minFull: toYMDHMS(minD),
            maxFull: toYMDHMS(maxD),
          }
        }));
      }

      let hasH=false, hasM=false, hasS=false;
      for (let i=0;i<raw.length;i++){
        const v = raw[i];
        if (typeof v === 'string') {
          const match = v.match(/(?:T|\s)(\d{1,2}):(\d{2})(?::(\d{2}))?/);
          if (match) { hasH=true; hasM = match[2]!==undefined; hasS = match[3]!==undefined; }
          else if (v.includes(':')) { hasH=true; hasM=true; }
        } else {
          const d = new Date(v);
          if (!isNaN(d)) {
            if (d.getHours() || d.getMinutes() || d.getSeconds()) hasH = true;
            if (d.getMinutes()) hasM = true;
            if (d.getSeconds()) hasS = true;
          }
        }
        if (hasH && hasM && hasS) break;
      }
      setTimeGran(prev => ({ ...prev, [attr]: { hour:hasH, minute:hasM, second:hasS } }));
    };

    if (src.getState()==='ready') run();
    else src.once('change', ()=>{ if (src.getState()==='ready') run(); });
  };

  /* ─── Compute ranges & granularity for ALL dt attrs if no cache ────────── */
  useEffect(() => {
    if (!vectorLayer || !datetimeAttrs.length) return;
    if (enhancedLoadedFromCache) return; // cache already loaded
    const src = vectorLayer.getSource();
    const computeAll = () => datetimeAttrs.forEach(a => computeRangeAndGranFor(a));
    if (src.getState()==='ready') computeAll();
    else src.once('change', ()=>{ if(src.getState()==='ready') computeAll(); });
  }, [vectorLayer, datetimeAttrs, enhancedLoadedFromCache]);

  /* ─── When we have a complete enhanced schema, save it to server cache ─── */
  useEffect(() => {
    if (!selected || !datetimeAttrs.length) return;
    const haveAll = datetimeAttrs.every(a => timeRanges[a] && timeGran[a]);
    if (haveAll && !enhancedSaved) {
      saveEnhancedToCache(selected, {
        dataset: selected,
        computedAt: new Date().toISOString(),
        timeRanges, timeGran
      });
      setEnhancedSaved(true);
    }
  }, [selected, datetimeAttrs, timeRanges, timeGran, enhancedSaved]);

  /* ─── Helpers for OL styles (with memoized text/label styles) ───────────── */
  const getGeomType = (feature) => {
    const g = feature.getGeometry();
    if (!g) return "Unknown";
    const t = g.getType();
    if (t.includes("Point")) return "Point";
    if (t.includes("LineString")) return "LineString";
    if (t.includes("Polygon")) return "Polygon";
    return t;
  };

  const getStyleFor = (feature, fillColor, strokeColor = "#333333") => {
    const geomType = getGeomType(feature);
    const key = `${geomType}|${fillColor}|${strokeColor}`;
    if (stylePool.current.has(key)) return stylePool.current.get(key);

    let style;
    if (geomType === "Point") {
      style = new ol.style.Style({
        image: new ol.style.Circle({
          radius: 6,
          fill: new ol.style.Fill({color: fillColor}),
          stroke: new ol.style.Stroke({color: strokeColor, width: 1})
        })
      });
    } else if (geomType.includes("LineString")) {
      style = new ol.style.Style({stroke: new ol.style.Stroke({color: fillColor, width: 2})});
    } else {
      style = new ol.style.Style({
        fill: new ol.style.Fill({color: fillColor}),
        stroke: new ol.style.Stroke({color: strokeColor, width: 1})
      });
    }
    stylePool.current.set(key, style);
    return style;
  };

  // Fast small hash for down-sampling decisions
  const hash32 = (s) => {
    let h = 2166136261 >>> 0;
    for (let i=0;i<s.length;i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
    return h >>> 0;
  };

  // Resolution -> zoom (WebMercator)
  const WORLD_RES_Z0 = 156543.03392804097;
  const resToZoom = (res) => Math.log2(WORLD_RES_Z0 / res);

  // How aggressively to skip labels for points by zoom
  const pointLabelSkip = (res) => {
    const z = resToZoom(res);
    if (z < 4) return 8;     // world view → 1/8 labels
    if (z < 6) return 4;     // country view → 1/4
    if (z < 8) return 2;     // region view → 1/2
    return 1;                // city+ → show all
  };

  // Memoized Text object
  const getTextObj = (text, fill, stroke) => {
    const key = `${text}|${fill}|${stroke}`;
    let t = textCache.current.get(key);
    if (!t) {
      t = new ol.style.Text({
        text: String(text),
        font: '12px sans-serif',
        fill: new ol.style.Fill({ color: fill }),
        stroke: new ol.style.Stroke({ color: stroke, width: 2 }),
        overflow: true,
        offsetY: -10,
        placement: 'point'
      });
      textCache.current.set(key, t);
      // Keep memory bounded
      if (textCache.current.size > 20000) textCache.current.clear();
    }
    return t;
  };

  // Memoized combined style for point symbol + label text
  const getPointLabelStyle = (circleColor, circleStroke, text, tFill, tStroke) => {
    const key = `${circleColor}|${circleStroke}|${text}|${tFill}|${tStroke}`;
    let st = pointLabelCache.current.get(key);
    if (!st) {
      st = new ol.style.Style({
        image: new ol.style.Circle({
          radius: 6,
          fill: new ol.style.Fill({color: circleColor}),
          stroke: new ol.style.Stroke({color: circleStroke, width:1})
        }),
        text: getTextObj(text, tFill, tStroke)
      });
      pointLabelCache.current.set(key, st);
      if (pointLabelCache.current.size > 20000) pointLabelCache.current.clear();
    }
    return st;
  };

  // Memoized label-only style (for lines/polygons)
  const getLabelOnlyStyle = (text, tFill, tStroke) => {
    const key = `${text}|${tFill}|${tStroke}`;
    let st = labelOnlyCache.current.get(key);
    if (!st) {
      st = new ol.style.Style({ text: getTextObj(text, tFill, tStroke) });
      labelOnlyCache.current.set(key, st);
      if (labelOnlyCache.current.size > 20000) labelOnlyCache.current.clear();
    }
    return st;
  };

  // Clear text caches when label attribute/colors change (prevents stale growth)
  useEffect(() => { textCache.current.clear(); labelOnlyCache.current.clear(); pointLabelCache.current.clear(); }, [labelConf.attribute, labelConf.fillColor, labelConf.strokeColor]);

  /* ─── Build style cache + legend (GRADUATED = equal-width 10 bins) ─────── */
  const styleProducts = useMemo(() => {
    if (!vectorLayer) return {cache: {}, legend: null};
    const {
      attribute, type, fillColor,
      gradStart, gradEnd, catColors, otherColor
    } = colorConf;

    const src = vectorLayer.getSource();
    const feats = src.getFeatures();
    const cache = {};
    let legend = null;

    const numfmt = (x) => {
      if (!Number.isFinite(x)) return String(x);
      if (Math.abs(x) >= 1000 || Math.abs(x) < 0.01) return Number(x).toExponential(2);
      return Number(x).toLocaleString(undefined, {maximumFractionDigits: 2});
    };

    if (type === 'categorized' && attribute) {
      const counts = new Map();
      feats.forEach(f => {
        const v = f.get(attribute);
        counts.set(v, (counts.get(v) || 0) + 1);
      });
      const distinct = Array.from(counts.keys());
      const sorted = distinct.map(v => ({v, n: counts.get(v)})).sort((a, b) => b.n - a.n);

      const fallback = (i) => `hsl(${(i * 137.5) % 360},60%,60%)`;
      sorted.forEach(({v}, i) => { cache[v] = (catColors && catColors[v]) ? catColors[v] : fallback(i); });
      cache.default = otherColor || '#cccccc';

      const top = sorted.slice(0, 10);
      const otherCount = sorted.slice(10).reduce((s, x) => s + x.n, 0);
      const items = top.map(({v}) => ({ label: String(v), color: cache[v] }));
      if (sorted.length>10) items.push({ label:`Other (${otherCount})`, color: cache.default });
      legend = { title:`${attribute} (Top 10)`, items };

    } else if (type === 'graduated' && attribute) {
      // ✅ Equal-width bins (10) between min and max
      const vals = feats
        .map(f => Number(f.get(attribute)))
        .filter(v => Number.isFinite(v));

      if (vals.length) {
        let min = Math.min(...vals);
        let max = Math.max(...vals);
        if (max === min) max = min + 1; // avoid zero-width
        const steps = 10;
        const width = (max - min) / steps;

        // Bin edges: edges[0]=min ... edges[steps]=max
        const edges = Array.from({ length: steps + 1 }, (_, i) => min + i * width);

        const usingDefaultRamp =
          (gradStart || "#0000ff").toLowerCase() === "#0000ff" &&
          (gradEnd   || "#ff0000").toLowerCase() === "#ff0000";
        const getGradColor = (t) => usingDefaultRamp ? multiStopColor(t) : gradientColor(gradStart, gradEnd, t);

        cache.__BINS__ = { edges, steps, gradStart, gradEnd };
        cache.default = fillColor;

        const items = [];
        for (let i=0;i<steps;i++){
          const a = edges[i];
          const b = edges[i+1];
          const t = i / (steps - 1); // color position per discrete bin
          items.push({
            label: `[${numfmt(a)} – ${numfmt(b)}${i === steps - 1 ? ']' : ')'}`,
            color: getGradColor(t)
          });
        }
        legend = { title: `${attribute} (Graduated, 10 bins)`, items };
      } else {
        // No numeric data → basic fallback
        cache['*'] = fillColor;
        legend = { title:'Basic', items:[{ label:'All features', color: fillColor }] };
      }
    } else {
      cache['*'] = fillColor;
      legend = { title:'Basic', items:[{ label:'All features', color: fillColor }] };
    }

    return {cache, legend};
  }, [vectorLayer, colorConf, schema]);

  /* ─── Apply style + time filter + labels (with perf fixes) ──────────────── */
  useEffect(() => {
    if (!vectorLayer) return;
    const { cache } = styleProducts;
    const { attribute, type, gradStart, gradEnd } = colorConf;
    const strokeColor = colorConf.strokeColor;

    const styleFn = (feature, resolution) => {
      // time filter (inclusive)
      if (datetimeFilter.attribute) {
        const val = feature.get(datetimeFilter.attribute);
        const d = new Date(val);
        const start = parseFilterToDate(datetimeFilter.start, 'start');
        const end   = parseFilterToDate(datetimeFilter.end, 'end');
        if ((start && d < start) || (end && d > end)) return null;
      }

      // base color by style type
      let fillCol;
      if (type === 'categorized' && attribute) {
        const key = feature.get(attribute);
        fillCol = (key in cache) ? cache[key] : cache.default;

      } else if (type === 'graduated' && attribute) {
        const v = +feature.get(attribute);
        const B = cache.__BINS__;
        if (B && Number.isFinite(v)) {
          const min = B.edges[0];
          const max = B.edges[B.steps];
          const width = (max - min) / B.steps;

          // Map value to a bin index [0, steps-1], inclusive on last bin
          let i = Math.floor((v - min) / width);
          if (v === max) i = B.steps - 1; // include max in last bin
          i = Math.max(0, Math.min(B.steps - 1, i));

          const t = i / (B.steps - 1);
          const defaultRamp =
            (gradStart || "#0000ff").toLowerCase() === "#0000ff" &&
            (gradEnd   || "#ff0000").toLowerCase() === "#ff0000";
          fillCol = defaultRamp ? multiStopColor(t) : gradientColor(gradStart, gradEnd, t);
        } else {
          fillCol = cache.default;
        }

      } else {
        fillCol = cache['*'];
      }

      const base = getStyleFor(feature, fillCol, strokeColor);

      // Labels (optimized)
      if (labelConf.enabled && labelConf.attribute) {
        const txtVal = feature.get(labelConf.attribute);
        if (txtVal==null || txtVal===undefined) return base;

        const geomType = (feature.getGeometry()?.getType() || "");
        if (geomType.includes("Point")) {
          // Down-sample point labels at low zooms
          const skip = pointLabelSkip(resolution);
          if (skip > 1) {
            // Stable hash per feature & label
            const g = feature.getGeometry();
            let cx = 0, cy = 0;
            if (g && typeof g.getFirstCoordinate === 'function') {
              const c = g.getFirstCoordinate();
              cx = Math.round(c[0]); cy = Math.round(c[1]);
            }
            const h = hash32(String(txtVal) + '|' + cx + ',' + cy);
            if ((h % skip) !== 0) return base; // render point, skip its label
          }
          // Combined cached style for point glyph + label
          return getPointLabelStyle(
            fillCol, strokeColor,
            String(txtVal),
            labelConf.fillColor, labelConf.strokeColor
          );
        }

        // Non-point: overlay a cached text style on top of base
        return [base, getLabelOnlyStyle(String(txtVal), labelConf.fillColor, labelConf.strokeColor)];
      }

      return base;
    };

    vectorLayer.setStyle(styleFn);
    setLegend(styleProducts.legend);
  }, [vectorLayer, styleProducts, colorConf, labelConf, datetimeFilter]);

  /* ─── Handlers ──────────────────────────────────────────────────────────── */
  const applyColor = (conf) => { setColorConf(c => ({...c, ...conf})); clearAllStyleCaches(); };
  const applyLabel = (conf) => {
    setLabelConf({
      enabled: true,
      attribute: conf.attribute,
      fillColor: conf.fillColor || "#000000",
      strokeColor: conf.strokeColor || "#ffffff"
    });
  };

  /* LLM helpers (trimmed) */
  const parseMaybeJSON = (data) => {
    try {
      if (Array.isArray(data)) return data;
      if (typeof data === 'object' && data !== null) {
        if (Array.isArray(data.suggestions)) return data.suggestions;
        const text = data.content || data.text || JSON.stringify(data);
        return parseMaybeJSON(text);
      }
      if (typeof data === 'string') {
        const s = data.trim().replace(/^```json/i, '').replace(/^```/i, '').replace(/```$/i, '').trim();
        const parsed = JSON.parse(s);
        return Array.isArray(parsed) ? parsed
             : Array.isArray(parsed.suggestions) ? parsed.suggestions
             : parsed;
      }
    } catch {}
    return [];
  };
  const normalizeSuggestion = (s) => {
    if (!s || typeof s !== 'object') return null;
    return {
      attribute: s.attribute || s.attr || "",
      type: (s.type || s.style || "basic").toLowerCase(),
      fillColor: s.fillColor || s.fill || "#2b6cb0",
      strokeColor: s.strokeColor || s.stroke || "#333333",
      explanation: s.explanation || s.reason || ""
    };
  };

  /* Ask LLM for a specific prompt (unchanged) */
  const askAI = async () => {
    setAiError("");
    if (!selected) return alert("Select a dataset first.");
    if (!prompt.trim()) return alert("Enter a styling prompt first.");
    setLlmLoading(true);
    try {
      const res = await fetch(`/datasets/${selected}/style.json?prompt=${encodeURIComponent(prompt)}`);
      const maybeText = await res.text();
      let payload; try { payload = JSON.parse(maybeText); } catch { payload = maybeText; }
      const raw = parseMaybeJSON(payload);
      const arr = (Array.isArray(raw) ? raw : (Array.isArray(raw.suggestions) ? raw.suggestions : []))
                  .map(normalizeSuggestion).filter(Boolean);
      setSuggestions(arr);
      if (arr.length) {
        const s = arr[0];
        if (s.type === 'label') applyLabel(s);
        else applyColor({ attribute: s.attribute, type: s.type, fillColor: s.fillColor, strokeColor: s.strokeColor });
      } else throw new Error("No suggestions returned");
    } catch (e) {
      setAiError(e.message || "LLM error");
      alert("LLM error: " + (e.message || "unknown"));
    } finally { setLlmLoading(false); }
  };

  /* Auto suggestions (cached server-side) */
  const askAuto = async () => {
    setAutoError("");
    if (!schema || !sample) return alert("Wait for schema/sample");
    setAutoLoading(true);

    const inst = "Instruction: For each attribute, suggest styling config as a JSON array of objects with keys {attribute,type,explanation}. Allowed types: basic, categorized, graduated, label. Keep it concise.";
    const openModalWith = (arr) => {
      const norm = arr.map(normalizeSuggestion).filter(Boolean);
      setAutoSuggestions(norm);
      setAutoModalOpen(true);
    };

    try {
      const cached = await loadAutoStyleFromCache(selected);
      if (cached?.suggestions?.length) {
        openModalWith(cached.suggestions);
        setAutoLoading(false);
        return;
      }

      let used = [];
      try {
        let resp = await fetch(`/datasets/${selected}/auto_style.json`, {
          method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({
            schema, sample: (Array.isArray(sample)?sample.slice(0,5):sample),
            instruction: inst
          })
        });
        if (resp.ok) {
          const data = await resp.json();
          const arr = parseMaybeJSON(data);
          if (Array.isArray(arr) && arr.length) used = arr;
          else if (Array.isArray(data?.suggestions)) used = data.suggestions;
        }
      } catch {}

      if (!used.length) {
        // last-resort Gemini call
        const genBody = {
          contents:[{
            parts:[{ text: `Schema:${JSON.stringify(schema)}\nSample:${JSON.stringify((Array.isArray(sample)?sample.slice(0,5):sample))}\n\n${inst}` }]
          }]
        };
        const g = await fetch(
          `https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${API_KEY}`,
          { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(genBody) }
        );
        if (g.ok) {
          const js  = await g.json();
          const raw = js.candidates?.[0]?.content?.parts?.[0]?.text || "";
          const arr = parseMaybeJSON(raw);
          if (Array.isArray(arr) && arr.length) used = arr;
        }
      }

      openModalWith(used);
      try { if (used.length) await saveAutoStyleToCache(selected, used); } catch {}

    } catch (e) {
      setAutoError(e.message || "Auto suggestion error");
      alert("Auto suggestion error: " + (e.message || "unknown"));
    } finally {
      setAutoLoading(false);
    }
  };

  const applyTimeFilter = () => {
    if (!datetimeFilter.attribute) return alert("Select a datetime attribute first");
    if (datetimeFilter.start && datetimeFilter.end) {
      const s = parseFilterToDate(datetimeFilter.start, 'start');
      const e = parseFilterToDate(datetimeFilter.end, 'end');
      if (s && e && s > e) return alert("Start must be before End");
    }
    setDatetimeFilter({ ...datetimeFilter });
  };
  const clearTimeFilter = () => setDatetimeFilter({ attribute:"", start:"", end:"" });

  /* ─── Example Prompts ───────────────────────────────────────────────────── */
  const examplePrompts = useMemo(() => {
    if (!selected) return null;
    if (DATASET_PROMPTS[selected]) return DATASET_PROMPTS[selected];
    if (schema && Array.isArray(schema)) {
      const numeric = schema.filter(f => /int|double|float|decimal|number/i.test(f.type)).map(f => f.name);
      const categorical = schema.filter(f => /string|varchar|text/i.test(f.type)).map(f => f.name);
      const picks = [];
      if (numeric.length) picks.push(`Visualize ${numeric[0]} with graduated colors`);
      if (categorical.length) picks.push(`Color code by ${categorical[0]} using categories`);
      if (categorical.length > 1) picks.push(`Label features using ${categorical[1]}`);
      picks.push("Display features with a uniform style");
      return picks;
    }
    return DEFAULT_PROMPTS;
  }, [selected, schema]);

  /* Helpers */
  const getAttributeSample = (attr, n = 5) => {
    let vals = [];
    if (Array.isArray(sample) && sample.length) {
      try { vals = sample.map(r => r?.[attr]).filter(v => v !== undefined && v !== null).slice(0, n); } catch {}
    }
    if ((!vals || vals.length === 0) && vectorLayer) {
      const feats = vectorLayer.getSource().getFeatures();
      vals = feats.map(f => f.get(attr)).filter(v => v !== undefined && v !== null).slice(0, n);
    }
    setAttrSample({ name: attr, values: vals });
  };
  const topCategories = (attr, max=10) => {
    if (!vectorLayer || !attr) return [];
    const feats = vectorLayer.getSource().getFeatures();
    const counts = new Map();
    feats.forEach(f => { const v = f.get(attr); counts.set(v, (counts.get(v)||0)+1); });
    return Array.from(counts.entries()).sort((a,b)=>b[1]-a[1]).slice(0, max).map(([v])=>v);
  };

  /* ─── UI ────────────────────────────────────────────────────────────────── */
  const AutoModal = () => !autoModalOpen ? null : (
    <div style={{
      position:'fixed', top:0,left:0,right:0,bottom:0,
      background:'rgba(0,0,0,0.35)', display:'flex',
      alignItems:'center', justifyContent:'center', zIndex:2000
    }} onClick={()=>setAutoModalOpen(false)}>
      <div style={{
        background:'#fff', padding:16, borderRadius:6,
        width:420, maxHeight:'75%', overflowY:'auto'
      }} onClick={e=>e.stopPropagation()}>
        <h3 style={{marginTop:0}}>AI Attribute Suggestions</h3>
        {autoError && <div style={{color:'#b00', marginBottom:8}}>{autoError}</div>}
        {autoSuggestions.length===0 ? (
          <div>No suggestions</div>
        ) : (
          autoSuggestions.map((s,i)=>(
            <div key={i} style={{border:'1px solid #eee', borderRadius:6, padding:8, marginBottom:8}}>
              <div><b>{s.attribute}</b> — <i>{s.type}</i></div>
              {s.explanation && <div style={{color:'#555', fontSize:12, margin:'4px 0 6px'}}>{s.explanation}</div>}
              <div style={{display:'flex', gap:8}}>
                <button onClick={()=>applyColor({ attribute:s.attribute, type:s.type, fillColor:s.fillColor, strokeColor:s.strokeColor })}>Apply color</button>
                {s.type==='label' && <button onClick={()=>applyLabel({ attribute:s.attribute })}>Apply label</button>}
              </div>
            </div>
          ))
        )}
        <div style={{textAlign:'right', marginTop:8}}>
          <button onClick={()=>setAutoModalOpen(false)}>Close</button>
        </div>
      </div>
    </div>
  );

  return (
    <div className="app" style={{display: 'flex', height: '100vh'}}>
      {/* SIDEBAR */}
      <div className="sidebar" style={{
        width: 280, padding: 10, overflowY: 'auto', borderRight: '1px solid #ccc', fontSize: 13, lineHeight: 1.25
      }}>
        {/* Datasets */}
        <h3 style={{margin: '6px 0'}}>Datasets</h3>
        <select value={selected} onChange={e => setSelected(e.target.value)} style={{width: '100%', marginBottom: 8}}>
          <option value="">— select —</option>
          {datasets.map(d => (<option key={d.name} value={d.name}>{displayName(d.name)}</option>))}
        </select>

        {/* Example Prompts */}
        <h3 style={{margin: '10px 0 6px'}}>Example Prompts</h3>
        {!selected ? (
          <div style={{fontSize: 12, color: '#555', border: '1px dashed #bbb', borderRadius: 6, padding: 8, marginBottom: 8}}>
            Select a dataset to view example prompts
          </div>
        ) : (
          <select value={prompt} onChange={e => setPrompt(e.target.value)} style={{width: '100%', marginBottom: 8}}>
            <option value="">— choose one —</option>
            {(examplePrompts || DEFAULT_PROMPTS).map((p, idx) => (<option key={idx} value={p}>{p}</option>))}
            <option value="">Custom…</option>
          </select>
        )}

        {/* AI Styling */}
        <h3 style={{margin:'10px 0 6px'}}>AI Styling</h3>
        <textarea rows={3} style={{ width:'100%' }} placeholder="Describe what styling you want…" value={prompt} onChange={e=>setPrompt(e.target.value)} />
        <div style={{display:'flex', gap:6, alignItems:'center', marginTop:6}}>
          <button onClick={askAI} disabled={llmLoading}>{llmLoading ? "Loading…" : "Ask LLM"}</button>
          <button onClick={askAuto} disabled={autoLoading}>{autoLoading ? "Loading…" : "Suggest Attribute Styles"}</button>
        </div>
        {aiError && <div style={{color:'#b00', marginTop:6}}>{aiError}</div>}

        {suggestions.length>0 && (
          <>
            <h4 style={{margin:'10px 0 6px'}}>LLM Suggestions</h4>
            <div style={{display:'grid', gap:6}}>
              {suggestions.map((s,i)=>(
                <div key={i} style={{ border:'1px solid #eee', borderRadius:6, padding:8 }}>
                  <b>{s.attribute}</b> — <i>{s.type}</i>
                </div>
              ))}
            </div>
          </>
        )}

        {/* Manual Styling */}
        <h3 style={{margin:'12px 0 6px'}}>Manual Styling</h3>
        <button
          onClick={()=>setModalOpen(true)}
          style={{ width:'100%', background:'#e7f3ff', border:'1px solid #b6daff', color:'#084298', padding:'6px 10px', borderRadius:6 }}
          title="Open manual styling controls"
        >Open Manual Styling</button>

        {/* Labels */}
        <h3 style={{margin:'12px 0 6px'}}>Labels</h3>
        <div style={{display:'flex', gap:6, alignItems:'center'}}>
          <label style={{display:'flex', alignItems:'center', gap:6, whiteSpace:'nowrap'}}>
            <input type="checkbox" checked={labelConf.enabled} onChange={e=>setLabelConf(c=>({...c,enabled:e.target.checked}))}/>
            Show
          </label>
          <select disabled={!labelConf.enabled} value={labelConf.attribute} onChange={e=>setLabelConf(c=>({...c,attribute:e.target.value}))} style={{flex:1}}>
            <option value="">— label attribute —</option>
            {attributes.map(a=><option key={a} value={a}>{a}</option>)}
          </select>
        </div>

        {/* Time Filter — stacked with visible valid range */}
        {datetimeAttrs.length>0 && (
          <>
            <h3 style={{margin:'12px 0 6px'}}>Time Filter</h3>
            <select
              value={datetimeFilter.attribute}
              onChange={(e)=>{
                const attr = e.target.value;
                setDatetimeFilter(f=>({ ...f, attribute:attr, start:"", end:"" }));
                computeRangeAndGranFor(attr);
              }}
              style={{width:'100%',marginBottom:8}}
            >
              <option value="">— select attribute —</option>
              {datetimeAttrs.map(a=>(<option key={a} value={a}>{a}</option>))}
            </select>

            {datetimeFilter.attribute && (
              <>
                <div style={{
                  fontSize:12, color:'#fff', background:'#333',
                  padding:'6px 8px', borderRadius:6, marginBottom:6
                }}>
                  {timeRanges[datetimeFilter.attribute]?.minFull
                    ? <>Valid range: <b>{timeRanges[datetimeFilter.attribute].minFull}</b> — <b>{timeRanges[datetimeFilter.attribute].maxFull}</b></>
                    : <>Valid range: <i>scanning features…</i></>}
                </div>

                <div style={{display:'grid', gridTemplateColumns:'1fr', gap:8}}>
                  <DateTimePicker
                    label="Start"
                    value={datetimeFilter.start}
                    onChange={(v)=>setDatetimeFilter(f=>({...f,start:v}))}
                    min={timeRanges[datetimeFilter.attribute]?.min}
                    max={timeRanges[datetimeFilter.attribute]?.max}
                    granularity={timeGran[datetimeFilter.attribute]}
                    defaultTimeRole="start"
                  />
                  <DateTimePicker
                    label="End"
                    value={datetimeFilter.end}
                    onChange={(v)=>setDatetimeFilter(f=>({...f,end:v}))}
                    min={timeRanges[datetimeFilter.attribute]?.min}
                    max={timeRanges[datetimeFilter.attribute]?.max}
                    granularity={timeGran[datetimeFilter.attribute]}
                    defaultTimeRole="end"
                  />
                </div>

                <div style={{display:'flex', gap:6, marginTop:8}}>
                  <button onClick={applyTimeFilter} style={{flex:1}}>Apply</button>
                  <button onClick={clearTimeFilter} style={{flex:1}}>Clear</button>
                </div>
              </>
            )}
          </>
        )}

        {/* Schema */}
        {schema && (
          <>
            <h3 style={{margin:'12px 0 6px'}}>Schema</h3>
            <table style={{ width:'100%', borderCollapse:'collapse', marginBottom:8 }}>
              <thead>
                <tr>
                  <th style={{border:'1px solid #ccc',padding:4,textAlign:'left'}}>Attribute</th>
                  <th style={{border:'1px solid #ccc',padding:4,textAlign:'left'}}>Type</th>
                </tr>
              </thead>
              <tbody>
                {schema.map((f,i)=>(
                  <tr key={i}
                      style={{cursor:'pointer'}}
                      onClick={()=>{
                        setSelectedField(f);
                        setSchemaModalOpen(true);
                        getAttributeSample(f.name);
                      }}>
                    <td style={{border:'1px solid #ccc',padding:4}}>{f.name}</td>
                    <td style={{border:'1px solid #ccc',padding:4}}>{f.type}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            {attrSample.name && (
              <div style={{border:'1px solid #eee', borderRadius:6, padding:8, marginBottom:8}}>
                <div style={{fontWeight:600, marginBottom:4}}>Sample values for <code>{attrSample.name}</code></div>
                {attrSample.values.length ? (
                  <ul style={{margin:'0 0 0 16px', padding:0}}>
                    {attrSample.values.map((v,idx)=>(<li key={idx} style={{margin:'2px 0'}}>{String(v)}</li>))}
                  </ul>
                ) : (
                  <div style={{color:'#666'}}>No sample values available.</div>
                )}
              </div>
            )}
          </>
        )}

        {/* Sample */}
        {sample && (
          <>
            <h3 style={{margin:'10px 0 6px'}}>Sample (5 rows)</h3>
            <pre style={{ maxHeight:140, overflowY:'auto', margin:0 }}>
              {JSON.stringify(Array.isArray(sample)?sample.slice(0,5):sample,null,2)}
            </pre>
          </>
        )}
      </div>

      {/* MAIN MAP */}
      <div className="main" style={{ flex:1, position:'relative' }}>
        <div id="map" ref={mapDiv} />
        {legend && legend.items?.length>0 && (
          <div style={{
            position:'absolute', left:10, bottom:10, zIndex:1000,
            background:'rgba(255,255,255,0.95)', padding:'10px 12px',
            border:'1px solid #ccc', borderRadius:6, maxWidth:300,
            boxShadow:'0 2px 8px rgba(0,0,0,0.1)', fontSize:12
          }}>
            <div style={{fontWeight:600, marginBottom:6}}>{legend.title}</div>
            <div>
              {legend.items.map((it, idx)=>(
                <div key={idx} style={{display:'flex', alignItems:'center', margin:'4px 0'}}>
                  <span style={{ width:16, height:12, border:'1px solid #999', background: it.color, marginRight:8, display:'inline-block' }}/>
                  <span style={{whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}} title={it.label}>{it.label}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Manual Styling Modal */}
      {modalOpen && (
        <div style={{
          position:'fixed', top:0,left:0,right:0,bottom:0,
          background:'rgba(0,0,0,0.3)', display:'flex',
          alignItems:'center', justifyContent:'center'
        }} onClick={()=>setModalOpen(false)}>
          <div style={{
            background:'#fff', padding:16, borderRadius:6,
            width:460, maxHeight:'85%', overflowY:'auto'
          }} onClick={e=>e.stopPropagation()}>
            <h3 style={{marginTop:0}}>Manual Styling</h3>

            <label style={{fontWeight:600}}>Attribute</label>
            <select
              value={manual.attribute}
              onChange={e=>{
                const attr = e.target.value;
                setManual(m=>({...m, attribute:attr}));
                if (manual.styleType === 'categorized' && attr) {
                  const tops = topCategories(attr);
                  setManual(m=>{
                    const copy = {...m};
                    copy.catColors = {...copy.catColors};
                    tops.forEach((v,i)=>{
                      if (!copy.catColors.hasOwnProperty(v)) {
                        const hue = (i*137.5)%360;
                        copy.catColors[v] = `hsl(${hue},60%,60%)`;
                      }
                    });
                    return copy;
                  });
                }
              }}
              style={{width:'100%',marginBottom:8}}
            >
              <option value="">— select —</option>
              {attributes.map(a=><option key={a} value={a}>{a}</option>)}
            </select>

            <label style={{fontWeight:600}}>Style</label>
            <select
              value={manual.styleType}
              onChange={e=>{
                const st = e.target.value;
                setManual(m=>({...m, styleType:st}));
                if (st==='categorized' && manual.attribute) {
                  const tops = topCategories(manual.attribute);
                  setManual(m=>{
                    const copy = {...m};
                    copy.catColors = {...copy.catColors};
                    tops.forEach((v,i)=>{
                      if (!copy.catColors.hasOwnProperty(v)) {
                        const hue = (i*137.5)%360;
                        copy.catColors[v] = `hsl(${hue},60%,60%)`;
                      }
                    });
                    return copy;
                  });
                }
              }}
              style={{width:'100%',marginBottom:12}}
            >
              <option value="basic">basic</option>
              <option value="categorized">categorized</option>
              <option value="graduated">graduated</option>
              <option value="label">label</option>
            </select>

            {manual.styleType==='basic' && (
              <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:12, alignItems:'center'}}>
                <div>
                  <div style={{fontSize:12, marginBottom:4}}>Fill</div>
                  <input type="color" value={manual.fillColor} onChange={e=>setManual(m=>({...m, fillColor:e.target.value}))} style={colorBoxStyle}/>
                </div>
                <div>
                  <div style={{fontSize:12, marginBottom:4}}>Stroke</div>
                  <input type="color" value={manual.strokeColor} onChange={e=>setManual(m=>({...m, strokeColor:e.target.value}))} style={colorBoxStyle}/>
                </div>
              </div>
            )}

            {manual.styleType==='categorized' && (
              <>
                {!manual.attribute && <div style={{color:'#b00', marginBottom:8}}>Pick an attribute first.</div>}
                {manual.attribute && (
                  <>
                    <div style={{fontWeight:600, margin:'8px 0 4px'}}>Top categories (edit colors)</div>
                    <div style={{display:'grid', gridTemplateColumns:'1fr auto', gap:10, alignItems:'center'}}>
                      {topCategories(manual.attribute).map((v,i)=>(
                        <React.Fragment key={String(v)}>
                          <div title={String(v)} style={{overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap'}}>{String(v)}</div>
                          <input type="color"
                                 value={manual.catColors?.[v] || '#cccccc'}
                                 onChange={e=>{
                                   const color = e.target.value;
                                   setManual(m=>{
                                     const cc = {...(m.catColors||{})};
                                     cc[v] = color;
                                     return {...m, catColors: cc};
                                   });
                                 }}
                                 style={colorBoxStyle}/>
                        </React.Fragment>
                      ))}
                    </div>
                    <div style={{marginTop:12, display:'grid', gridTemplateColumns:'1fr 1fr', gap:12, alignItems:'center'}}>
                      <div>
                        <div style={{fontSize:12, marginBottom:4}}>Other (default)</div>
                        <input type="color" value={manual.otherColor} onChange={e=>setManual(m=>({...m, otherColor:e.target.value}))} style={colorBoxStyle}/>
                      </div>
                      <div>
                        <div style={{fontSize:12, marginBottom:4}}>Stroke</div>
                        <input type="color" value={manual.strokeColor} onChange={e=>setManual(m=>({...m, strokeColor:e.target.value}))} style={colorBoxStyle}/>
                      </div>
                    </div>
                  </>
                )}
              </>
            )}

            {manual.styleType==='graduated' && (
              <>
                {!manual.attribute && <div style={{color:'#b00', marginBottom:8}}>Pick a numeric attribute first.</div>}
                <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:12, alignItems:'center'}}>
                  <div>
                    <div style={{fontSize:12, marginBottom:4}}>Start (low)</div>
                    <input type="color" value={manual.gradStart} onChange={e=>setManual(m=>({...m, gradStart:e.target.value}))} style={colorBoxStyle}/>
                  </div>
                  <div>
                    <div style={{fontSize:12, marginBottom:4}}>End (high)</div>
                    <input type="color" value={manual.gradEnd} onChange={e=>setManual(m=>({...m, gradEnd:e.target.value}))} style={colorBoxStyle}/>
                  </div>
                </div>
                <div style={{marginTop:12}}>
                  <div style={{fontSize:12, marginBottom:4}}>Stroke</div>
                  <input type="color" value={manual.strokeColor} onChange={e=>setManual(m=>({...m, strokeColor:e.target.value}))} style={colorBoxStyle}/>
                </div>
              </>
            )}

            {manual.styleType==='label' && (
              <>
                <label style={{fontWeight:600, marginTop:6, display:'block'}}>Label attribute</label>
                <select value={manual.labelAttribute} onChange={e=>setManual(m=>({...m, labelAttribute:e.target.value}))} style={{width:'100%',marginBottom:8}}>
                  <option value="">— select —</option>
                  {attributes.map(a=><option key={a} value={a}>{a}</option>)}
                </select>
                <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:12, alignItems:'center'}}>
                  <div>
                    <div style={{fontSize:12, marginBottom:4}}>Text</div>
                    <input type="color" value={manual.labelFill} onChange={e=>setManual(m=>({...m, labelFill:e.target.value}))} style={colorBoxStyle}/>
                  </div>
                  <div>
                    <div style={{fontSize:12, marginBottom:4}}>Halo</div>
                    <input type="color" value={manual.labelStroke} onChange={e=>setManual(m=>({...m, labelStroke:e.target.value}))} style={colorBoxStyle}/>
                  </div>
                </div>
              </>
            )}

            <div style={{display:'flex', gap:8, marginTop:12}}>
              <button
                onClick={()=>{
                  if (manual.styleType==='basic') {
                    applyColor({ attribute: "", type: "basic", fillColor: manual.fillColor, strokeColor: manual.strokeColor });
                  } else if (manual.styleType==='categorized') {
                    if (!manual.attribute) return alert("Pick an attribute.");
                    applyColor({
                      attribute: manual.attribute, type: "categorized",
                      strokeColor: manual.strokeColor, fillColor: "#eeeeee",
                      catColors: manual.catColors || {}, otherColor: manual.otherColor || "#cccccc",
                    });
                  } else if (manual.styleType==='graduated') {
                    if (!manual.attribute) return alert("Pick a numeric attribute.");
                    applyColor({
                      attribute: manual.attribute, type: "graduated",
                      strokeColor: manual.strokeColor, fillColor: "#eeeeee",
                      gradStart: manual.gradStart, gradEnd: manual.gradEnd
                    });
                  } else if (manual.styleType==='label') {
                    if (!manual.labelAttribute) return alert("Pick a label attribute.");
                    applyLabel({ attribute: manual.labelAttribute, fillColor: manual.labelFill, strokeColor: manual.labelStroke });
                  }
                  setModalOpen(false);
                }}
                style={{flex:1}}
              >Apply</button>
              <button onClick={()=>setModalOpen(false)} style={{flex:1}}>Close</button>
            </div>
          </div>
        </div>
      )}

      {/* Enhanced Schema Modal */}
      {schemaModalOpen && selectedField && (
        <div style={{
          position:'fixed', top:0,left:0,right:0,bottom:0,
          background:'rgba(0,0,0,0.3)', display:'flex',
          alignItems:'center', justifyContent:'center'
        }} onClick={()=>setSchemaModalOpen(false)}>
          <div style={{
            background:'#fff', padding:16, borderRadius:6,
            width:360, maxHeight:'75%', overflowY:'auto'
          }} onClick={e=>e.stopPropagation()}>
            <h3 style={{marginTop:0}}>Field: {selectedField.name}</h3>
            <div style={{fontSize:12, color:'#555', marginBottom:6}}>Type: <b>{selectedField.type}</b></div>
            <div style={{marginBottom:8}}>
              <div style={{fontWeight:600, marginBottom:4}}>Metadata</div>
              <pre style={{ maxHeight:180, overflowY:'auto', margin:0 }}>
                {JSON.stringify(selectedField.metadata, null, 2)}
              </pre>
            </div>
            <div style={{marginBottom:8}}>
              <div style={{fontWeight:600, marginBottom:4}}>
                Sample values (5) — {attrSample.name ? <code>{attrSample.name}</code> : null}
              </div>
              {attrSample.values.length ? (
                <ul style={{margin:'0 0 0 16px', padding:0}}>
                  {attrSample.values.map((v,idx)=>(<li key={idx} style={{margin:'2px 0'}}>{String(v)}</li>))}
                </ul>
              ) : (
                <div style={{color:'#666'}}>No sample values available.</div>
              )}
            </div>
            <button onClick={()=>setSchemaModalOpen(false)}>Close</button>
          </div>
        </div>
      )}

      {/* Auto Suggestions Modal */}
      <AutoModal />
    </div>
  );
}

ReactDOM.render(<App/>, document.getElementById('root'));