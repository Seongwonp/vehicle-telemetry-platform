// Runs the 5 provisioned panel queries through Grafana /api/ds/query, with the
// vehicle variable interpolated the way the frontend does (${vehicle:json} -> JSON array of
// the variable's current values; All = every option returned by the PostgreSQL query),
// and the same query with the filter line removed (= before this change). Prints vehicle ids per panel.
const fs=require("fs");
const [,, dashFile, from, to, iv] = process.argv; const intervalMs=Number(iv||60000);
const auth="Basic "+Buffer.from(process.env.GU+":"+process.env.GP).toString("base64");
const BS=String.fromCharCode(92);
async function q(body){const r=await fetch("http://localhost:3000/api/ds/query",{method:"POST",headers:{Authorization:auth,"Content-Type":"application/json"},body:JSON.stringify(body)});return r.json();}
(async()=>{
  const dash=JSON.parse(fs.readFileSync(dashFile,"utf8"));
  const v=dash.templating.list[0];
  const pg=await q({queries:[{refId:"A",datasource:{uid:v.datasource.uid},rawSql:v.query,format:"table",rawQuery:true}],from,to});
  const opts=pg.results.A.frames[0].data.values[0];
  console.log(`range ${from} .. ${to}  intervalMs=${intervalMs} maxDataPoints=1000`);
  console.log(`variable "vehicle" options (PostgreSQL, active): ${JSON.stringify(opts)}  -> All = ${JSON.stringify(opts)}`);
  const filterLine=String.fromCharCode(10)+"  |> filter(fn: (r) => contains(value: r.vehicle_id, set: ${vehicle:json}))";
  for(const p of dash.panels){
    const raw=p.targets[0].query;
    if(!raw.includes(filterLine)) throw new Error("panel "+p.id+" has no vehicle filter");
    const after=raw.replace("${vehicle:json}",JSON.stringify(opts));
    const before=raw.replace(filterLine,"");
    const ids=async(query)=>{const r=await q({queries:[{refId:"A",datasource:{uid:"P951FEA4DE68E13C5",type:"influxdb"},query,intervalMs,maxDataPoints:1000}],from,to});
      const fr=r.results.A.frames||[]; if(r.results.A.error) return "ERROR "+r.results.A.error;
      return [...new Set(fr.map(f=>(f.schema.fields[1]&&f.schema.fields[1].labels||{}).vehicle_id))].sort();};
    const b=await ids(before), a=await ids(after);
    const hidden=Array.isArray(b)&&Array.isArray(a)?b.filter(x=>!a.includes(x)):[];
    console.log(`\npanel ${p.id} "${p.title}"\n  before (no vehicle filter): ${b.length} series ${JSON.stringify(b)}\n  after  (vehicle=All):       ${a.length} series ${JSON.stringify(a)}\n  hidden by filter: ${JSON.stringify(hidden)}`);
  }
})().catch(e=>{console.error(e);process.exit(1);});
