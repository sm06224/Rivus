//! Live observability HTTP server (Epic #30, Pillar B — issue #32).
//!
//! `rivus run … --serve [ADDR]` runs the flow on a worker thread and exposes a
//! **std-only** HTTP/1.1 server (no third-party deps): a `TcpListener`, a
//! hand-written request line parser, and Server-Sent Events. Heavy rendering is
//! pushed to the browser (an embedded HTML/JS/SVG dashboard served as a static
//! string); the Rust side only ships JSON snapshots from Pillar A's
//! [`RuntimeSnapshot`]. `cargo deny` stays green — zero new crates.
//!
//! Routes:
//!   `GET /`         → the embedded dashboard (auto-configures from the snapshot)
//!   `GET /snapshot` → the latest snapshot as one JSON object (polling)
//!   `GET /events`   → `text/event-stream` live feed of snapshots (SSE)

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Cap on concurrent connection-handler threads. A small bound so a burst of
/// idle/slow clients can't spawn unbounded threads; excess connections are
/// dropped immediately (the dashboard is a single local viewer in practice).
const MAX_CONNS: usize = 64;

/// Per-connection read timeout. A client that opens a socket but never sends a
/// complete request line must not pin a handler thread forever.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared observability state the worker publishes into and the HTTP handlers
/// read from. `seq` increments on every publish so SSE handlers can detect new
/// snapshots by polling cheaply; `done` flips when the run finishes.
#[derive(Default)]
pub struct Hub {
    inner: Mutex<HubState>,
    /// The static DAG topology JSON (`viz::render_graph_json`), served once at
    /// `/graph` so the dashboard can lay out the SVG before any snapshot.
    graph: String,
}

#[derive(Default)]
struct HubState {
    /// Latest snapshot rendered as a JSON object (viz::render_snapshot_json).
    latest: Option<String>,
    /// Monotonic publish counter (SSE handlers send only when this advances).
    seq: u64,
    /// True once the run has finished (lets SSE handlers close gracefully).
    done: bool,
}

impl Hub {
    pub fn new(graph: String) -> Arc<Hub> {
        Arc::new(Hub {
            graph,
            ..Hub::default()
        })
    }

    /// Publish a new snapshot JSON (called from the run's progress hook).
    pub fn publish(&self, json: String) {
        let mut s = self.inner.lock().unwrap();
        s.latest = Some(json);
        s.seq += 1;
    }

    /// Mark the run finished (SSE streams then send a final event and close).
    pub fn finish(&self) {
        let mut s = self.inner.lock().unwrap();
        s.done = true;
        s.seq += 1;
    }

    fn read(&self) -> (Option<String>, u64, bool) {
        let s = self.inner.lock().unwrap();
        (s.latest.clone(), s.seq, s.done)
    }
}

/// The embedded dashboard. Pure HTML/JS/SVG (no CDN, no framework): it fetches
/// the static DAG topology from `/graph` once, lays it out as a left→right
/// layered flow diagram, then opens an SSE connection to `/events` and
/// **animates** the live run — particles stream along each data edge at a rate
/// set by that node's throughput, nodes pulse blue while active, turn green when
/// finished and red on errors (error side-channel edges flow red). Kept
/// dependency-free; all heavy rendering is in the browser.
pub const DASHBOARD_HTML: &str = r##"<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Rivus live</title>
<style>
 :root{--bg:#0b0e14;--fg:#cdd6f4;--mut:#a6adc8;--blue:#89b4fa;--grn:#a6e3a1;--red:#f38ba8;--amb:#f9e2af;--pan:#11151f;--brd:#313244}
 *{box-sizing:border-box} body{font:13px/1.4 ui-sans-serif,system-ui,sans-serif;margin:0;background:var(--bg);color:var(--fg)}
 header{display:flex;align-items:baseline;gap:1rem;padding:.7rem 1rem;border-bottom:1px solid var(--brd);position:sticky;top:0;background:var(--bg);z-index:2}
 h1{font-size:1rem;color:var(--blue);margin:0;font-weight:700;letter-spacing:.03em}
 #meta{color:var(--mut)} #meta b{color:var(--fg);font-weight:600}
 #status{margin-left:auto;font-size:11px;color:var(--mut);border:1px solid var(--brd);border-radius:999px;padding:2px 10px}
 #main{display:flex;align-items:flex-start} #wrap{flex:1;padding:1rem;overflow:auto} svg{display:block;max-width:100%;height:auto}
 #side{width:300px;max-width:42vw;border-left:1px solid var(--brd);padding:.7rem 1rem;align-self:stretch}
 #side h2{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--mut);margin:.2rem 0 .4rem}
 #script{font:11.5px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--fg);white-space:pre;overflow:auto;max-height:80vh;margin:0}
 .node text{font-family:ui-sans-serif,system-ui,sans-serif}
 .lab{fill:var(--fg);font-weight:600;font-size:12.5px} .knd{fill:var(--mut);font-size:10px;text-transform:uppercase;letter-spacing:.07em}
 .src{fill:var(--blue);font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:10.5px;opacity:.85}
 .num{fill:var(--blue);font-size:12px;font-variant-numeric:tabular-nums}
 .estream{fill:none;stroke:#2a3147;stroke-width:2} .eerror{fill:none;stroke:#5a2a37;stroke-width:1.5;stroke-dasharray:4 5}
 .part{fill:var(--blue)}
 .legend{padding:.45rem 1rem;color:var(--mut);font-size:11px;display:flex;flex-wrap:wrap;gap:1.2rem;border-top:1px solid var(--brd)}
 .legend i{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px;vertical-align:-1px}
</style></head><body>
<header><h1>RIVUS</h1><span id="meta">connecting…</span><span id="status">● live</span></header>
<div id="main">
 <div id="wrap"><svg id="cv" xmlns="http://www.w3.org/2000/svg"></svg></div>
 <aside id="side"><h2>flow source</h2><pre id="script">…</pre></aside>
</div>
<div class="legend">
 <span><i style="background:#89b4fa"></i>flowing</span>
 <span><i style="background:#f9e2af"></i>buffering (blocking op)</span>
 <span><i style="background:#a6e3a1"></i>done</span>
 <span><i style="background:#f38ba8"></i>errors</span>
 <span>moving dots = live throughput</span>
</div>
<script>
const NS='http://www.w3.org/2000/svg';
// Vertical layout (UX-J): depth runs top→down (matching the script order), so a
// linear flow is a single readable column; branches step out horizontally.
const NW=210,NH=66,VSTEP=112,HSTEP=240,MX=26,MY=22;
const fmt=n=>(n||0).toLocaleString();
const cv=document.getElementById('cv');
let N={},E=[];
const mk=(t,a)=>{const e=document.createElementNS(NS,t);for(const k in a)e.setAttribute(k,a[k]);return e;};
const cut=(s,n)=>{s=s||'';n=n||28;return s.length>n?s.slice(0,n-1)+'…':s;};

function layout(g){
 N={};(g.nodes||[]).forEach(n=>N[n.node_id]={id:n.node_id,label:n.label,kind:n.kind,src:n.src||'',blocking:!!n.blocking,ro:0,ri:0,err:0,fin:false,buf:false,prevO:0,prevI:0,act:0});
 const adj={},indeg={};Object.keys(N).forEach(i=>{adj[i]=[];indeg[i]=0;});
 (g.edges||[]).forEach(e=>{if(N[e.from]!=null&&N[e.to]!=null){adj[e.from].push(e.to);indeg[e.to]++;}});
 const depth={},din=Object.assign({},indeg);let q=Object.keys(N).filter(i=>indeg[i]===0);q.forEach(i=>depth[i]=0);
 while(q.length){const u=q.shift();for(const v of adj[u]){depth[v]=Math.max(depth[v]||0,(depth[u]||0)+1);if(--din[v]===0)q.push(v);}}
 const rows={};let maxd=0;Object.keys(N).forEach(i=>{const d=depth[i]||0;(rows[d]=rows[d]||[]).push(+i);maxd=Math.max(maxd,d);});
 let maxw=1;for(const d in rows){rows[d].sort((a,b)=>a-b);maxw=Math.max(maxw,rows[d].length);}
 const W=MX*2+(maxw-1)*HSTEP+NW,H=MY*2+maxd*VSTEP+NH;
 cv.setAttribute('viewBox',`0 0 ${W} ${H}`);cv.setAttribute('width',W);cv.setAttribute('height',H);
 // depth → y (down the page); breadth-within-depth → x (centered, branches out).
 for(const d in rows){const c=rows[d],off=(maxw-c.length)/2;c.forEach((id,i)=>{N[id].x=MX+(off+i)*HSTEP;N[id].y=MY+d*VSTEP;});}
 const gE=mk('g'),gP=mk('g'),gN=mk('g');E=[];
 (g.edges||[]).forEach(e=>{const a=N[e.from],b=N[e.to];if(!a||!b)return;
  const x1=a.x+NW/2,y1=a.y+NH,x2=b.x+NW/2,y2=b.y,dy=Math.max(28,(y2-y1)*0.45);
  const p=mk('path',{d:`M${x1},${y1} C${x1},${y1+dy} ${x2},${y2-dy} ${x2},${y2}`,class:e.kind==='error'?'eerror':'estream'});
  gE.appendChild(p);const parts=[];
  if(e.kind!=='error')for(let k=0;k<3;k++){const c=mk('circle',{r:3,class:'part'});c.style.opacity=0;gP.appendChild(c);parts.push({c,t:k/3});}
  E.push({src:e.from,dst:e.to,kind:e.kind,path:p,len:1,parts});});
 Object.values(N).forEach(o=>{const grp=mk('g',{class:'node',transform:`translate(${o.x},${o.y})`});
  o.rect=mk('rect',{width:NW,height:NH,rx:9,fill:'#11151f',stroke:'#313244','stroke-width':1.5});
  const lab=mk('text',{x:12,y:20,class:'lab'});lab.textContent=cut(o.label,22);
  const knd=mk('text',{x:12,y:35,class:'knd'});knd.textContent=o.kind;
  // The IR source line — *what* this node does (UX-J); full text on hover.
  const src=mk('text',{x:12,y:53,class:'src'});src.textContent=cut(o.src,30);
  const tip=mk('title');tip.textContent=o.src||o.kind;
  o.num=mk('text',{x:NW-12,y:20,'text-anchor':'end',class:'num'});o.num.textContent='0';
  grp.append(o.rect,lab,knd,src,o.num,tip);gN.appendChild(grp);});
 cv.replaceChildren(gE,gP,gN);
 E.forEach(e=>{try{e.len=e.path.getTotalLength()||1;}catch(_){}});
}

function onSnap(s){
 document.getElementById('meta').innerHTML=`<b>${fmt(s.rows_seen)}</b> rows · <b>${(s.elapsed_ms/1000).toFixed(1)}s</b> · ${s.mode}`;
 for(const n of (s.nodes||[])){const o=N[n.node_id];if(!o)continue;
  // A blocking op (sort/group/…) that is still accumulating (rows_in grew but
  // rows_out hasn't, and it isn't finished) is "buffering", not stuck (UX-J).
  o.buf=o.blocking&&!n.finished&&n.rows_in>n.rows_out;
  if(n.rows_out>o.prevO||(o.buf&&n.rows_in>o.prevI))o.act=1;
  o.prevO=n.rows_out;o.prevI=n.rows_in;o.ro=n.rows_out;o.ri=n.rows_in;o.err=n.errors;o.fin=n.finished;
  o.num.textContent=o.buf?('⏳ '+fmt(n.rows_in-n.rows_out)):fmt(n.rows_out);}
}

let last=performance.now();
function frame(now){const dt=Math.min(64,now-last);last=now;
 for(const id in N){const o=N[id];o.act*=Math.pow(0.94,dt/16);
  if(o.buf)o.act=Math.max(o.act,0.5); // keep a blocking buffer visibly working
  let st='#313244',sw=1.5,fl='#11151f';
  if(o.buf){st='#f9e2af';sw=2;fl='#1f1d16';}      // amber: buffering / working
  else if(o.act>0.05){st='#89b4fa';sw=1.5+o.act*1.8;fl='#1a2335';}
  else if(o.fin){st='#a6e3a1';}
  if(o.err>0)st='#f38ba8';
  o.rect.setAttribute('stroke',st);o.rect.setAttribute('stroke-width',sw.toFixed(2));o.rect.setAttribute('fill',fl);}
 for(const e of E){const src=N[e.src],a=src?src.act:0;
  if(e.kind==='error'){const on=(src&&src.err>0)||(N[e.dst]&&N[e.dst].err>0);
   e.path.style.strokeDashoffset=on?((-now*0.04)%18):0;e.path.setAttribute('stroke',on?'#f38ba8':'#5a2a37');continue;}
  const spd=dt*0.00016*(0.4+a);
  for(const pt of e.parts){pt.t=(pt.t+spd)%1;const P=e.path.getPointAtLength(pt.t*e.len);
   pt.c.setAttribute('cx',P.x);pt.c.setAttribute('cy',P.y);pt.c.style.opacity=a>0.04?Math.min(1,a+0.15):0;}}
 requestAnimationFrame(frame);
}

fetch('/graph').then(r=>r.json()).then(g=>{layout(g);
 document.getElementById('script').textContent=g.script||'(no source)';
 requestAnimationFrame(frame);
 const es=new EventSource('/events');
 es.onmessage=e=>{try{onSnap(JSON.parse(e.data))}catch(_){}};
 es.onerror=()=>{const st=document.getElementById('status');st.textContent='● finished';st.style.color='#a6e3a1';es.close();};
}).catch(()=>{document.getElementById('meta').textContent='failed to load /graph';});
</script></body></html>"##;

/// Bind a server on `addr` (e.g. `127.0.0.1:0` for an ephemeral port). Returns
/// the listener and its resolved address. A bind failure is returned to the
/// caller for a graceful fallback (run without `--serve`).
pub fn bind(addr: &str) -> std::io::Result<(TcpListener, String)> {
    let listener = TcpListener::bind(addr)?;
    let local = listener.local_addr()?.to_string();
    Ok((listener, local))
}

/// Serve until the run is `done` and the listener stops. Each connection is
/// handled inline (one short-lived request, or a long-lived SSE stream on its
/// own thread). Runs on the calling thread; `hub` is shared with the worker.
pub fn serve(listener: TcpListener, hub: Arc<Hub>) {
    // Poll accept() so we notice `done` and exit even with no further requests.
    listener.set_nonblocking(true).ok();
    // Keep serving briefly after `done` so the browser receives the terminal
    // snapshot and a viewer can still poll `/snapshot` (a short grace window).
    let mut grace: u32 = 0;
    // Bound the number of live handler threads (idle/slow clients can't pile up).
    let conns = Arc::new(AtomicUsize::new(0));
    loop {
        match listener.accept() {
            Ok((s, _)) => {
                // Shed load past the cap rather than spawn an unbounded thread.
                if conns.load(Ordering::Relaxed) >= MAX_CONNS {
                    drop(s);
                    continue;
                }
                conns.fetch_add(1, Ordering::Relaxed);
                let hub = Arc::clone(&hub);
                let conns = Arc::clone(&conns);
                std::thread::spawn(move || {
                    handle(s, hub);
                    conns.fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if hub.read().2 {
                    grace += 1;
                    if grace > 40 {
                        // ~2 s after finish with no traffic → stop.
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }
}

fn handle(mut stream: TcpStream, hub: Arc<Hub>) {
    // A stalled client that never sends a full request line must not pin this
    // thread; the SSE stream only writes, so this read bound doesn't affect it.
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let path = match read_request_target(&mut stream) {
        Some(p) => p,
        None => return,
    };
    match path.as_str() {
        "/" => respond(
            &mut stream,
            "200 OK",
            "text/html; charset=utf-8",
            DASHBOARD_HTML,
        ),
        "/graph" => {
            let body = if hub.graph.is_empty() {
                "{\"nodes\":[],\"edges\":[]}"
            } else {
                hub.graph.as_str()
            };
            respond(&mut stream, "200 OK", "application/json", body);
        }
        "/snapshot" => {
            let body = hub.read().0.unwrap_or_else(|| "{}".to_string());
            respond(&mut stream, "200 OK", "application/json", &body);
        }
        "/events" => stream_events(stream, hub),
        _ => respond(&mut stream, "404 Not Found", "text/plain", "not found"),
    }
}

/// Parse just the request target from the first request line: `GET /path HTTP/1.1`.
fn read_request_target(stream: &mut TcpStream) -> Option<String> {
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    r.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let _method = parts.next()?;
    let target = parts.next()?.to_string();
    // Drain the rest of the headers (we don't need them).
    let mut hdr = String::new();
    while r.read_line(&mut hdr).ok()? > 0 {
        if hdr == "\r\n" || hdr == "\n" {
            break;
        }
        hdr.clear();
    }
    Some(target)
}

/// Write a complete, closed HTTP/1.1 response.
fn respond(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.flush();
}

/// Stream snapshots as Server-Sent Events until the run finishes. Sends the
/// current snapshot immediately, then a new event whenever `seq` advances.
fn stream_events(mut stream: TcpStream, hub: Arc<Hub>) {
    if write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n"
    )
    .is_err()
    {
        return;
    }
    let mut last_seq = 0u64;
    loop {
        let (latest, seq, done) = hub.read();
        if seq != last_seq {
            last_seq = seq;
            if let Some(json) = &latest {
                // SSE frame: `data: <json>\n\n`.
                if write!(stream, "data: {json}\n\n").is_err() || stream.flush().is_err() {
                    return;
                }
            }
        }
        if done {
            // One last flush of the terminal snapshot already happened above.
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A blocking one-shot GET used by the CLI test helper (and handy for scripts):
/// connect, request `path`, return the response body.
#[allow(dead_code)]
pub fn get_body(addr: &str, path: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect(addr)?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
    )?;
    let mut buf = String::new();
    s.read_to_string(&mut buf)?;
    Ok(buf.split("\r\n\r\n").nth(1).unwrap_or("").to_string())
}
