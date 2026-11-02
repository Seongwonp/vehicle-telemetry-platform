// One dashboard-like load through the Grafana HTTP API (no browser, no auto-refresh):
// variable query first, then the 5 provisioned panel queries in parallel (as the frontend fires them),
// ${vehicle:json} replaced by the variable's options (= All). Prints per-query duration, series, points.
// Usage: GU/GP in env (never printed). node time_panels.js <dashboard.json> <from> <to> <intervalMs> <maxDataPoints>
const fs=require("fs");
const [,, dashFile, from, to, iv, mdp] = process.argv;
const intervalMs=Number(iv), maxDataPoints=Number(mdp);
const auth="Basic "+Buffer.from(process.env.GU+":"+process.env.GP).toString("base64");
async function q(body){const t=performance.now();const r=await fetch("http://localhost:3000/api/ds/query",{method:"POST",headers:{Authorization:auth,"Content-Type":"application/json"},body:JSON.stringify(body)});const j=await r.json();return {ms:Math.round(performance.now()-t),http:r.status,j};}
(async()=>{
  const dash=JSON.parse(fs.readFileSync(dashFile,"utf8"));
  const v=dash.templating.list[0];
  const t0=performance.now();
  const pg=await q({queries:[{refId:"A",datasource:{uid:v.datasource.uid},rawSql:v.query,format:"table",rawQuery:true}],from,to});
  const opts=pg.j.results.A.frames[0].data.values[0];
  console.log(`range ${from} .. ${to}  intervalMs=${intervalMs} maxDataPoints=${maxDataPoints}  auto-refresh: none (single load)`);
  console.log(`variable query: ${pg.ms} ms, HTTP ${pg.http}, options ${JSON.stringify(opts)}`);
  const t1=performance.now();
  const res=await Promise.all(dash.panels.map(async p=>{
    const query=p.targets[0].query.replace("${vehicle:json}",JSON.stringify(opts));
    const r=await q({queries:[{refId:"A",datasource:{uid:"P951FEA4DE68E13C5",type:"influxdb"},query,intervalMs,maxDataPoints}],from,to});
    const a=r.j.results&&r.j.results.A||{}; const fr=a.frames||[];
    const ids=[...new Set(fr.map(f=>(f.schema.fields[1]&&f.schema.fields[1].labels||{}).vehicle_id))].sort();
    const points=fr.reduce((s,f)=>s+((f.data&&f.data.values[0])||[]).length,0);
    return {p,ms:r.ms,http:r.http,err:a.error||"",ids,points};
  }));
  const wall=Math.round(performance.now()-t1), total=Math.round(performance.now()-t0);
  for(const x of res) console.log(`panel ${x.p.id} "${x.p.title}": ${x.ms} ms, HTTP ${x.http}, ${x.ids.length} series ${JSON.stringify(x.ids)}, ${x.points} points${x.err?"  ERROR "+x.err:""}`);
  console.log(`5 panels in parallel: wall ${wall} ms; variable + panels: ${total} ms`);
})().catch(e=>{console.error(e);process.exit(1);});
