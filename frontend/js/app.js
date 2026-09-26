(() => {
  "use strict";

  const CONFIG = window.VAULT_CONFIG || { mode: "mock", baseUrl: "/api/v1" };

  const MAX_UPLOAD_BYTES = 100 * 1024 * 1024;
  const SAFE_FILENAME = /^[^\x00-\x1f\x7f]+$/;

  function resolveApiBaseUrl(raw) {
    const candidate = new URL(String(raw || "/api/v1"), window.location.origin);
    if (!["http:", "https:"].includes(candidate.protocol)) {
      throw new Error("Unsupported API protocol.");
    }
    candidate.hash = "";
    candidate.username = "";
    candidate.password = "";
    return candidate.href.replace(/\/$/, "");
  }

  function validateUploadFile(file) {
    if (!(file instanceof File)) throw new Error("Please select a valid file.");
    if (!Number.isFinite(file.size) || file.size < 0) throw new Error("Invalid file size.");
    if (file.size > MAX_UPLOAD_BYTES) throw new Error("File exceeds the 100 MB demo upload limit.");
    if (!SAFE_FILENAME.test(file.name)) throw new Error("File name contains invalid control characters.");
  }
  const ROUTES = ["overview","nodes","objects","repairs","integrity","rebalance","events","policies"];
  const DATA = {
    dashboard:{objects:12842},
    nodes:[
      {id:"node-01",status:"healthy",capacity:"500 GB",used:"205 GB",percent:41,objects:4287,heartbeat:"2.0s",lifecycle:"HEALTHY"},
      {id:"node-02",status:"healthy",capacity:"500 GB",used:"340 GB",percent:68,objects:3914,heartbeat:"2.1s",lifecycle:"HEALTHY"},
      {id:"node-03",status:"healthy",capacity:"500 GB",used:"285 GB",percent:57,objects:3669,heartbeat:"2.0s",lifecycle:"HEALTHY"},
      {id:"node-04",status:"attention",capacity:"500 GB",used:"405 GB",percent:81,objects:4210,heartbeat:"2.4s",lifecycle:"HEALTHY"}
    ],
    objects:[
      {id:"obj_8fd21a",size:"24.8 MB",version:"v7",replicas:"3/3",checksum:"6d1f…9a20",status:"healthy",updated:"2m ago",type:"application/zip",created:"Sep 26, 2026 · 03:44:12",currentVersionId:"v7",replicaRows:[["node-01","HEALTHY","24.8 MB"],["node-02","HEALTHY","24.8 MB"],["node-03","HEALTHY","24.8 MB"]]},
      {id:"obj_4c11b7",size:"1.2 GB",version:"v3",replicas:"3/3",checksum:"21a9…b641",status:"healthy",updated:"14m ago",type:"application/octet-stream",created:"Sep 26, 2026 · 03:31:07",currentVersionId:"v3",replicaRows:[["node-02","HEALTHY","1.2 GB"],["node-03","HEALTHY","1.2 GB"],["node-04","HEALTHY","1.2 GB"]]},
      {id:"obj_3da909",size:"84.2 MB",version:"v2",replicas:"2/3",checksum:"a57b…c1e9",status:"degraded",updated:"28m ago",type:"application/json",created:"Sep 26, 2026 · 03:14:30",currentVersionId:"v2",replicaRows:[["node-01","HEALTHY","84.2 MB"],["node-02","HEALTHY","84.2 MB"],["node-04","UNAVAILABLE","—"]]},
      {id:"obj_711ce2",size:"412 MB",version:"v9",replicas:"3/3",checksum:"b44e…8c71",status:"healthy",updated:"1h ago",type:"video/mp4",created:"Sep 26, 2026 · 02:52:13",currentVersionId:"v9",replicaRows:[["node-01","HEALTHY","412 MB"],["node-03","HEALTHY","412 MB"],["node-04","HEALTHY","412 MB"]]},
      {id:"obj_c92e10",size:"18.6 MB",version:"v4",replicas:"3/3",checksum:"1f91…73ae",status:"healthy",updated:"2h ago",type:"application/pdf",created:"Sep 26, 2026 · 02:04:51",currentVersionId:"v4",replicaRows:[["node-01","HEALTHY","18.6 MB"],["node-02","HEALTHY","18.6 MB"],["node-03","HEALTHY","18.6 MB"]]},
      {id:"obj_aa9120",size:"6.8 GB",version:"v12",replicas:"2/3",checksum:"e21c…10bd",status:"corrupted",updated:"3h ago",type:"application/x-tar",created:"Sep 26, 2026 · 01:26:09",currentVersionId:"v12",replicaRows:[["node-01","HEALTHY","6.8 GB"],["node-02","CORRUPTED","6.8 GB"],["node-03","HEALTHY","6.8 GB"]]}
    ],
    repairs:[
      {id:"repair-203",object:"obj_8fd21a",source:"node-01",target:"node-04",progress:72,status:"RUNNING",eta:"8s",note:"Rebuilding replica after node-04 storage pressure"},
      {id:"repair-202",object:"obj_aa9120",source:"node-03",target:"node-02",progress:100,status:"COMPLETED",eta:"—",note:"Replaced corrupted replica after checksum mismatch"},
      {id:"repair-201",object:"obj_31c0e9",source:"node-01",target:"node-03",progress:100,status:"COMPLETED",eta:"—",note:"Recovered missing replica after heartbeat loss"}
    ],
    integrity:[
      {object:"obj_aa9120",replica:"node-02",expected:"e21c…10bd",observed:"b8d3…9ca0",result:"CORRUPTED",checked:"3h ago"},
      {object:"obj_8fd21a",replica:"node-01",expected:"6d1f…9a20",observed:"6d1f…9a20",result:"VALID",checked:"2m ago"},
      {object:"obj_4c11b7",replica:"node-03",expected:"21a9…b641",observed:"21a9…b641",result:"VALID",checked:"14m ago"},
      {object:"obj_711ce2",replica:"node-04",expected:"b44e…8c71",observed:"b44e…8c71",result:"VALID",checked:"1h ago"}
    ],
    rebalance:[
      {job:"rebalance-088",object:"obj_8fd21a",source:"node-04",target:"node-01",progress:64,status:"RUNNING",updated:"1m ago"},
      {job:"rebalance-087",object:"obj_4c11b7",source:"node-04",target:"node-03",progress:100,status:"SUCCEEDED",updated:"22m ago"}
    ],
    events:[
      {type:"success",icon:"✓",title:"Repair completed",body:"obj_aa9120 restored to three healthy replicas after checksum verification.",relative:"3m ago"},
      {type:"warning",icon:"!",title:"node-04 crossed high watermark",body:"Storage usage is 81%. Rebalancing is eligible.",relative:"4m ago"},
      {type:"success",icon:"↻",title:"Integrity scan completed",body:"38,526 replicas checked. One mismatch was isolated.",relative:"8m ago"},
      {type:"success",icon:"↑",title:"Object committed",body:"obj_8fd21a committed at version v7 with write quorum 2.",relative:"16m ago"},
      {type:"warning",icon:"◌",title:"Replica became unavailable",body:"node-04 missed the control-plane heartbeat window.",relative:"18m ago"},
      {type:"failure",icon:"×",title:"Checksum mismatch detected",body:"obj_aa9120 on node-02 failed SHA-256 verification and entered CORRUPTED.",relative:"21m ago"}
    ],
    liveHealth:null,
    sync:{status:CONFIG.mode==="api"?"connecting":"demo",at:null,error:null},
    detailLoading:false
  };

  const state = {view:"overview",nodeFilter:"all",objectFilter:"all",eventFilter:"all",selectedObject:null,commandIndex:0,drillRunning:false};
  const $=(s,r=document)=>r.querySelector(s);
  const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));
  const escapeHtml=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const formatBytes=bytes=>{
    if(!Number.isFinite(bytes)||bytes<0)return "—";
    if(bytes===0)return "0 B";
    const units=["B","KB","MB","GB","TB"];let i=0,value=bytes;
    while(value>=1024&&i<units.length-1){value/=1024;i++;}
    return (value>=100?value.toFixed(0):value.toFixed(1))+" "+units[i];
  };
  const formatTimestamp=value=>{
    if(!value)return "—";
    const d=new Date(value);
    return Number.isNaN(d.getTime())?String(value):d.toLocaleString();
  };
  const formatHeartbeat=value=>{
    if(!value)return "—";
    const d=new Date(value);
    if(Number.isNaN(d.getTime()))return String(value);
    return Math.max(0,Math.round((Date.now()-d.getTime())/1000))+"s ago";
  };
  const debounce=(fn,wait=150)=>{let timer=0;return (...args)=>{clearTimeout(timer);timer=window.setTimeout(()=>fn(...args),wait);};};
  const bytesFromDisplay=value=>{
    const m=String(value||"").trim().match(/^([\d.]+)\s*(B|KB|MB|GB|TB)$/i);
    if(!m)return 0;
    const units={B:1,KB:1024,MB:1024**2,GB:1024**3,TB:1024**4};
    return Number(m[1])*units[m[2].toUpperCase()];
  };

  const API={
    mode:CONFIG.mode,
    baseUrl:resolveApiBaseUrl(CONFIG.baseUrl),
    async request(path,options={}){
      const headers=new Headers(options.headers||{});
      const requestId=options.requestId||"req_"+Math.random().toString(16).slice(2,10);
      headers.set("Accept","application/json");
      headers.set("Cache-Control","no-cache");
      headers.set("X-Request-ID",requestId);
      const base=this.baseUrl.replace(/\/$/,"");
      const url=base+(path.startsWith("/")?path:"/"+path);
      let response;
      try{response=await fetch(new URL(url,location.origin),{...options,headers,redirect:"error"});}
      catch(error){const err=new Error(error?.message||"Unable to reach the Vault API");err.code="NETWORK_ERROR";err.requestId=requestId;throw err;}
      const body=await response.json().catch(()=>({}));
      if(!response.ok){const e=body.error||{};const err=new Error(e.message||"API request failed");err.code=e.code||"HTTP_"+response.status;err.requestId=e.request_id||requestId;throw err;}
      return body;
    },
    get(path){return this.request(path);},
    async action(name,payload){
      if(this.mode==="mock")return {job_id:name+"-"+Date.now(),status:"RUNNING"};
      const paths={repair:"/admin/repair",integrity:"/admin/integrity/check",rebalance:"/admin/rebalance"};
      if(!paths[name])throw new Error("Unsupported admin action.");
      return this.request(paths[name],{method:"POST",body:JSON.stringify(payload||{}),headers:{"Content-Type":"application/json"}});
    },
    async job(name,id){
      const paths={repair:"/admin/repair/",integrity:"/admin/integrity/check/",rebalance:"/admin/rebalance/"};
      return this.get(paths[name]+encodeURIComponent(id));
    },
    async upload(file){
      validateUploadFile(file);
      if(this.mode==="mock")return {object_id:"obj_"+Math.random().toString(16).slice(2,8),version_id:"v1"};
      const objectName=file.name.replace(/\\/g,"/").split("/").pop()||"upload";
      return this.request("/objects/"+encodeURIComponent(objectName),{method:"PUT",body:file,headers:{"Content-Type":file.type||"application/octet-stream"}});
    },
    async objectDetails(name){
      const encoded=encodeURIComponent(name);
      const [metadata,versions]=await Promise.all([this.get("/objects/"+encoded+"/metadata"),this.get("/objects/"+encoded+"/versions")]);
      return {metadata,versions:Array.isArray(versions)?versions:[]};
    },
    async sync(){
      if(this.mode!=="api")return;
      stateBusy(true);
      const results=await Promise.allSettled([this.get("/health"),this.get("/nodes"),this.get("/objects")]);
      const [health,nodes,objects]=results;
      const errors=[];
      if(health.status==="fulfilled")DATA.liveHealth=health.value;else errors.push("health");
      if(nodes.status==="fulfilled"&&Array.isArray(nodes.value)){
        DATA.nodes=nodes.value.map(n=>{
          const capacity=Number(n.capacity_bytes||0),used=Number(n.used_bytes||0);
          const percent=capacity?Math.min(100,Math.round((used/capacity)*100)):0;
          const rawStatus=String(n.status||"UNKNOWN").toUpperCase();
          const healthy=["HEALTHY","READY"].includes(rawStatus);
          return {id:n.node_id,status:healthy?"healthy":"attention",capacity:formatBytes(capacity),used:formatBytes(used),percent,objects:"—",heartbeat:formatHeartbeat(n.last_heartbeat_at),lifecycle:rawStatus,capacityBytes:capacity,usedBytes:used};
        });
      }else errors.push("nodes");
      if(objects.status==="fulfilled"&&Array.isArray(objects.value)){DATA.objects=objects.value.map(normalizeLiveObject);DATA.dashboard.objects=DATA.objects.length;}else errors.push("objects");
      DATA.sync={status:errors.length?"degraded":"live",at:new Date(),error:errors.length?("Failed: "+errors.join(", ")):null};
      stateBusy(false);renderAll();
      if(errors.length)throw new Error(DATA.sync.error);
    }
  };

  function normalizeLiveObject(o){
    const active=String(o.state||"").toUpperCase()==="ACTIVE";
    return {id:o.name||o.object_id||"unknown",size:"—",version:o.current_version_id?"current":"—",replicas:"—",checksum:"—",status:active?"healthy":"attention",updated:formatTimestamp(o.updated_at),type:"object",created:formatTimestamp(o.created_at),currentVersionId:o.current_version_id||null,replicaRows:[],live:true};
  }
  function toast(title,message){const t=document.createElement("div");t.className="toast";t.innerHTML="<b>"+escapeHtml(title)+"</b><small>"+escapeHtml(message)+"</small>";$("#toasts").appendChild(t);setTimeout(()=>t.remove(),4200);}
  function stateBusy(flag){document.body.classList.toggle("is-syncing",!!flag);const refresh=$("#refresh");if(refresh)refresh.disabled=!!flag;}
  function showView(view){
    if(!ROUTES.includes(view))view="overview";
    state.view=view;
    $$(".nav-item").forEach(b=>b.classList.toggle("active",b.dataset.view===view));
    $$(".view").forEach(v=>v.classList.toggle("visible",v.id==="view-"+view));
    $("#view-title").textContent=view.charAt(0).toUpperCase()+view.slice(1);
    history.replaceState(null,"","#"+view);
    window.scrollTo({top:0,behavior:"smooth"});
    renderAll();
  }
  function status(s){
    const map={healthy:["green","HEALTHY"],attention:["amber","ATTENTION"],degraded:["amber","DEGRADED"],corrupted:["red","CORRUPTED"],running:["amber","RUNNING"],success:["green","SUCCEEDED"],draining:["amber","DRAINING"]};
    const m=map[s]||["green",String(s||"unknown").toUpperCase()];
    return "<span class='status "+m[0]+"'><i></i>"+m[1]+"</span>";
  }
  function renderEnvironment(){
    const live=CONFIG.mode==="api", ok=live?DATA.sync.status==="live":true;
    $("#env-label").textContent=live?(ok?"LIVE API":"API "+String(DATA.sync.status||"CONNECTING").toUpperCase()):"DEMO MODE";
    $("#env-detail").textContent=live?(DATA.sync.at?"Last sync "+formatTimestamp(DATA.sync.at):"Connecting to Part B…"):"Safe local simulation";
    $("#api-badge").textContent=live?"LIVE API":"MOCK MODE";
    $("#api-badge").className="chip "+(live?"green":"amber");
    $("#api-base-url").textContent=CONFIG.baseUrl;
    $("#topology-live-label").textContent=live?"live telemetry":"demo telemetry";
    $("#request-label").textContent=DATA.sync.at&&live?"req_live_"+String(DATA.sync.at.getTime()).slice(-6):"req_demo_7F2A";
    $("#request-dot").style.background=live?(ok?"var(--green)":"var(--amber)"):"var(--blue)";
    $("#signal-control").textContent=live?(ok?"Connected":"Degraded"):"Connected";
    $("#signal-control-icon").textContent=live?(ok?"✓":"!"):"✓";
  }
  function renderSummary(){
    const nodes=DATA.nodes,healthy=nodes.filter(n=>n.status==="healthy").length,attention=nodes.length-healthy;
    const totalCapacity=nodes.reduce((s,n)=>s+(n.capacityBytes||bytesFromDisplay(n.capacity)),0);
    const totalUsed=nodes.reduce((s,n)=>s+(n.usedBytes||bytesFromDisplay(n.used)),0);
    const usedPct=totalCapacity?Math.round(totalUsed/totalCapacity*100):72,headroom=Math.max(0,100-usedPct),liveBad=CONFIG.mode==="api"&&DATA.liveHealth?.status==="degraded";
    const score=Math.max(0,Math.min(100,CONFIG.mode==="mock"?98.7:Math.round((healthy/Math.max(nodes.length,1))*1000)/10-(liveBad?4:0)));
    $("#resilience-score").innerHTML=score.toFixed(1)+"<small>/100</small>";$("#score-meter").style.width=score+"%";$("#score-meter").parentElement.setAttribute("aria-valuenow",String(score));$("#score-delta").textContent=attention?("−"+Math.min(3,attention*0.9).toFixed(1)+"%"):"+1.8%";
    $("#storage-used").innerHTML=(totalUsed?formatBytes(totalUsed):"1.44 TB").replace(" ","<small> ")+"</small>";$("#storage-chip").textContent=usedPct+"%";$("#storage-meter").style.width=Math.min(100,usedPct)+"%";$("#storage-meter").parentElement.setAttribute("aria-valuenow",String(Math.min(100,usedPct)));$("#storage-caption").textContent=totalCapacity?formatBytes(totalCapacity)+" total logical capacity":"2.0 TB logical capacity";
    $("#signal-headroom").textContent=headroom+"% remaining";$("#healthy-replicas").innerHTML=(attention?Math.max(0,100-attention*4):99.4).toFixed(1)+"<small>%</small>";
    $("#nodes-count").textContent=nodes.length;$("#nodes-summary").textContent=healthy+" healthy · "+attention+" attention";
    const free=totalCapacity-totalUsed;$("#available-capacity").innerHTML=(free?formatBytes(free):"560 GB").replace(" ","<small> ")+"</small>";$("#capacity-summary").textContent=headroom+"% cluster headroom";
    $("#write-acceptance").innerHTML=(attention?Math.round((healthy/Math.max(nodes.length,1))*100):100)+"<small>%</small>";$("#write-summary").textContent=attention?"Some nodes need attention":"All nodes accepting writes";
    const heartbeatSeconds=nodes.map(n=>parseFloat(String(n.heartbeat).replace("s",""))).filter(Number.isFinite);const median=heartbeatSeconds.length?heartbeatSeconds.sort((a,b)=>a-b)[Math.floor(heartbeatSeconds.length/2)]:2.1;$("#median-heartbeat").innerHTML=median.toFixed(1)+"<small>s</small>";
    $("#health-ring-score").textContent=score.toFixed(1);$("#health-ring").setAttribute("aria-valuenow",String(score));$("#health-ring").style.background="conic-gradient(var(--green) 0 "+score+"%,#193042 "+score+"% 100%)";
    $("#cluster-badge").textContent=attention||liveBad?"ATTENTION":"HEALTHY";$("#cluster-badge").className="badge "+(attention||liveBad?"amber":"green");$("#cluster-summary").textContent=attention||liveBad?"Cluster requires attention":"All core services operational";
    const attentionNode=nodes.find(n=>n.status==="attention");$("#cluster-copy").textContent=attentionNode?(attentionNode.id+" needs attention. Reads remain available."): "No current loss of read availability.";
    const hot=nodes.find(n=>n.percent>=80);$("#ops-banner").classList.toggle("good-news",!hot);$("#ops-banner-title").textContent=hot?hot.id+" is above the 80% watermark":"Cluster operating inside the configured watermark";$("#ops-banner-copy").textContent=hot?"Rebalance is eligible before capacity pressure becomes a failure mode.":"No capacity watermark is currently breached.";
  }
  function updateTopology(){
    DATA.nodes.slice(0,4).forEach(n=>{const el=$("#topology-"+n.id);if(!el)return;const dot=el.querySelector(".dot"),name=el.querySelector("b"),meta=el.querySelector("small");dot.className="dot "+(n.status==="healthy"?"good":"warn");name.textContent=n.id;meta.textContent=n.percent+"% used";el.classList.toggle("attention-node",n.status!=="healthy");});
  }
  function renderNodes(){
    const q=($("#node-search")?.value||"").toLowerCase();
    const rows=DATA.nodes.filter(n=>(state.nodeFilter==="all"||(state.nodeFilter==="healthy"&&n.status==="healthy")||(state.nodeFilter==="attention"&&n.status==="attention"))&&(!q||(n.id+" "+n.lifecycle).toLowerCase().includes(q)));
    $("#nodes-body").innerHTML=rows.length?rows.map(n=>{const actionLabel=n.lifecycle==="DRAINING"?"Resume":"Drain";return "<tr><td class='objid'>"+escapeHtml(n.id)+"</td><td>"+status(n.status)+"</td><td>"+n.capacity+"</td><td><b>"+n.used+"</b> <small>"+n.percent+"%</small></td><td>"+(typeof n.objects==="number"?n.objects.toLocaleString():"—")+"</td><td>"+escapeHtml(n.heartbeat)+"</td><td><span class='chip "+(n.lifecycle==="HEALTHY"?"green":"amber")+"'>"+escapeHtml(n.lifecycle)+"</span></td><td><button class='table-action' data-node='"+escapeHtml(n.id)+"' data-node-action='"+actionLabel.toLowerCase()+"' aria-label='"+actionLabel+" "+escapeHtml(n.id)+"'>"+actionLabel+"</button></td></tr>";}).join(""):"<tr><td colspan='8' class='empty-row'>No nodes match this filter.</td></tr>";
  }
  function renderObjects(){
    const q=($("#object-search")?.value||"").toLowerCase(),rows=DATA.objects.filter(o=>(state.objectFilter==="all"||o.status===state.objectFilter)&&(!q||(o.id+" "+o.checksum+" "+o.status).toLowerCase().includes(q)));
    $("#objects-body").innerHTML=rows.length?rows.map(o=>"<tr><td class='objid'>"+escapeHtml(o.id)+"</td><td>"+escapeHtml(o.size)+"</td><td>"+escapeHtml(o.version)+"</td><td>"+escapeHtml(o.replicas)+"</td><td class='hash'>"+escapeHtml(o.checksum)+"</td><td>"+status(o.status)+"</td><td>"+escapeHtml(o.updated)+"</td><td><button class='table-action' data-object='"+escapeHtml(o.id)+"' aria-label='Inspect "+escapeHtml(o.id)+"'>Inspect</button></td></tr>").join(""):"<tr><td colspan='8' class='empty-row'>No objects match this filter.</td></tr>";
    const selected=DATA.objects.find(o=>o.id===state.selectedObject);
    if(state.detailLoading)$("#object-detail").innerHTML="<div class='empty loading-state'><b>↻</b><h3>Loading object details</h3><p>Fetching metadata and version history from Part B…</p></div>";
    else if(selected)renderDetail(selected);
    else $("#object-detail").innerHTML="<div class='empty'><b>▦</b><h3>Select an object</h3><p>Replica placement and version details will appear here.</p></div>";
  }
  function renderDetail(o){
    const liveVersion=o.currentVersion||{};
    const replicaCards=o.replicaRows?.length?o.replicaRows.map(r=>"<div class='replica'><b>"+escapeHtml(r[0])+"</b><small>"+escapeHtml(r[2])+"</small>"+status(r[1]==="HEALTHY"?"healthy":r[1]==="CORRUPTED"?"corrupted":"attention")+"</div>").join(""):"<div class='replica'><b>Healthy replicas</b><small>Public API count</small><span class='detail-number'>"+escapeHtml(o.replicas)+"</span></div><div class='replica'><b>Version state</b><small>Current committed version</small>"+status(o.status)+"</div><div class='replica'><b>Placement</b><small>Not exposed by public catalog contract</small><span class='status green'><i></i>API-safe</span></div>";
    $("#object-detail").innerHTML="<div class='detail-grid'><div><div class='detail-hero'><p class='eyebrow'>OBJECT DETAIL</p><h2>"+escapeHtml(o.id)+"</h2><div class='detail-meta'><span>"+escapeHtml(o.size)+"</span><span>Version "+escapeHtml(o.version)+"</span><span>"+status(o.status)+"</span><span>SHA-256 "+escapeHtml(o.checksum)+"</span></div></div><div style='padding-top:12px'><p class='eyebrow'>REPLICA / VERSION SIGNAL</p><div class='replicas'>"+replicaCards+"</div></div></div><div><p class='eyebrow'>METADATA</p><div class='service-list'><div><span>Content type</span><b>"+escapeHtml(o.type)+"</b></div><div><span>Created</span><b>"+escapeHtml(o.created)+"</b></div><div><span>Version ID</span><b>"+escapeHtml(o.currentVersionId||"—")+"</b></div><div><span>Version number</span><b>"+escapeHtml(liveVersion.version_number?("v"+liveVersion.version_number):o.version)+"</b></div><div><span>Healthy replicas</span><b>"+escapeHtml(o.replicas)+"</b></div><div><span>Placement policy</span><b>RF 3 · W2 · R1</b></div></div></div></div>";
  }
  function renderRepairs(){$("#repair-active").textContent=DATA.repairs.filter(r=>r.status==="RUNNING").length;$("#repair-grid").innerHTML=DATA.repairs.map(r=>"<article class='repair-card'><div class='repair-top'><div><p class='eyebrow'>"+escapeHtml(r.id)+"</p><h3>"+escapeHtml(r.object)+"</h3></div>"+status(r.status==="RUNNING"?"running":"success")+"</div><p>"+escapeHtml(r.note)+"</p><div class='route'><b>"+escapeHtml(r.source)+"</b><span>→ verified stream →</span><b>"+escapeHtml(r.target)+"</b></div><div class='progress-label'><span>Recovery progress</span><b>"+r.progress+"%</b></div><div class='meter'><i style='width:"+r.progress+"%'></i></div><footer><span>"+(r.status==="RUNNING"?"ETA "+escapeHtml(r.eta):"Durability floor restored")+"</span><span>checksum verified</span></footer></article>").join("");}
  function renderIntegrity(){$("#integrity-body").innerHTML=DATA.integrity.map(r=>"<tr><td class='objid'>"+escapeHtml(r.object)+"</td><td>"+escapeHtml(r.replica)+"</td><td class='hash'>"+escapeHtml(r.expected)+"</td><td class='hash'>"+escapeHtml(r.observed)+"</td><td>"+(r.result==="VALID"?status("healthy"):status("corrupted"))+"</td><td>"+escapeHtml(r.checked)+"</td></tr>").join("");}
  function renderRebalance(){$("#capacity-bars").innerHTML=DATA.nodes.map(n=>"<div class='capacity-row'><b>"+escapeHtml(n.id)+"</b><div class='capacity "+(n.percent>=80?"high":"")+"'><i style='width:"+n.percent+"%'></i></div><span class='capacity-value'>"+n.percent+"%</span></div>").join("");$("#rebalance-body").innerHTML=DATA.rebalance.map(r=>"<tr><td class='objid'>"+escapeHtml(r.job)+"</td><td>"+escapeHtml(r.object)+"</td><td>"+escapeHtml(r.source)+"</td><td>"+escapeHtml(r.target)+"</td><td style='min-width:120px'><div class='meter'><i style='width:"+r.progress+"%'></i></div><small>"+r.progress+"%</small></td><td>"+status(r.status==="RUNNING"?"running":"success")+"</td><td>"+escapeHtml(r.updated)+"</td></tr>").join("");}
  function renderEvents(){const rows=DATA.events.filter(e=>state.eventFilter==="all"||e.type===state.eventFilter);$("#event-feed").innerHTML=rows.length?rows.map(e=>"<article class='event'><span class='event-icon "+e.type+"'>"+escapeHtml(e.icon)+"</span><div><b>"+escapeHtml(e.title)+"</b><p>"+escapeHtml(e.body)+"</p></div><time>"+escapeHtml(e.relative)+"</time></article>").join(""):"<div class='empty'><b>≋</b><h3>No events in this filter</h3><p>The event feed is clear.</p></div>";}
  function renderTimeline(){$("#timeline").innerHTML=DATA.events.slice(0,4).map(e=>"<div class='event' style='grid-template-columns:8px 1fr auto;background:transparent;border:0;border-bottom:1px solid rgba(167,192,216,.07);border-radius:0;padding:10px 0'><span class='dot "+(e.type==="success"?"good":e.type==="warning"?"warn":"")+"' style='margin-top:4px'></span><div><b>"+escapeHtml(e.title)+"</b><p>"+escapeHtml(e.body)+"</p></div><time>"+escapeHtml(e.relative)+"</time></div>").join("");}
  function renderAll(){renderEnvironment();renderSummary();updateTopology();if(state.view==="nodes")renderNodes();if(state.view==="objects")renderObjects();if(state.view==="repairs")renderRepairs();if(state.view==="integrity")renderIntegrity();if(state.view==="rebalance")renderRebalance();if(state.view==="events")renderEvents();renderTimeline();$("#objects-kpi").textContent=DATA.dashboard.objects.toLocaleString();}
  function setFilter(group,value){state[group]=value;const id=group==="nodeFilter"?"node-filters":group==="objectFilter"?"object-filters":"event-filters";const box=$("#"+id);$$("button",box).forEach(b=>b.classList.toggle("active",b.dataset.filter===value));renderAll();}
  function jobIdFromResponse(name,result){const keys={repair:["repair_id","job_id","id"],integrity:["integrity_id","job_id","id"],rebalance:["rebalance_id","job_id","id"]};return(keys[name]||[]).map(k=>result?.[k]).find(Boolean)||null;}
  async function pollJob(name,id){
    if(CONFIG.mode!=="api"||!id)return;
    let last="";
    for(let i=0;i<30;i++){
      await new Promise(r=>setTimeout(r,1000));
      try{
        const data=await API.job(name,id),statusText=String(data.status||data.state||"").toUpperCase();
        if(statusText&&statusText!==last){toast(name.charAt(0).toUpperCase()+name.slice(1)+" job",statusText+" · "+id);last=statusText;}
        if(["SUCCEEDED","COMPLETED","DONE","FAILED","ERROR"].includes(statusText)){await API.sync().catch(()=>{});toast(statusText==="FAILED"||statusText==="ERROR"?"Operation failed":"Operation completed",id);return;}
      }catch(error){toast("Job polling paused",error.message);return;}
    }
    toast("Operation still running","The API accepted "+id+"; refresh to check its latest state.");
  }
  async function runDemoAction(name){
    await new Promise(r=>setTimeout(r,850));
    if(name==="integrity")DATA.events.unshift({type:"success",icon:"✓",title:"Integrity scan completed",body:"Demo scan verified the protected replica set.",relative:"just now"});
    else if(name==="repair"){const target=DATA.objects.find(o=>o.status==="degraded"||o.status==="corrupted")||DATA.objects[0];DATA.repairs.unshift({id:"repair-"+Date.now().toString().slice(-4),object:target.id,source:"node-03",target:"node-04",progress:100,status:"COMPLETED",eta:"—",note:"Demo repair completed after independent verification"});DATA.events.unshift({type:"success",icon:"↻",title:"Replica repaired",body:target.id+" returned to the durability floor after verification.",relative:"just now"});}
    else if(name==="rebalance"){const hot=DATA.nodes.find(n=>n.percent>=80);if(hot){hot.percent=74;hot.used="370 GB";hot.usedBytes=bytesFromDisplay(hot.used);}DATA.events.unshift({type:"success",icon:"⇄",title:"Rebalance completed",body:"Verified data moved away from the highest-pressure node.",relative:"just now"});}
    renderAll();toast("Demo operation completed","Local simulation state has been updated.");
  }
  async function runAction(name){
    try{
      const selected=DATA.objects.find(o=>o.id===state.selectedObject),versionId=selected?.currentVersionId;
      if(CONFIG.mode==="api"){
        if((name==="repair"||name==="rebalance")&&!versionId){
          showView("objects");
          throw new Error("Open Objects, inspect an object with a current version, then start this operation.");
        }
        if(name==="integrity")showView("integrity");
        if(name==="repair")showView("repairs");
        if(name==="rebalance")showView("rebalance");
        let payload={};
        if(name==="repair")payload={version_id:versionId,reason:"admin-request"};
        if(name==="integrity"&&versionId)payload={version_id:versionId};
        if(name==="rebalance"){const source=DATA.nodes.find(n=>n.percent>=80)||DATA.nodes.find(n=>n.status==="attention"),target=DATA.nodes.find(n=>n.id!==source?.id&&n.percent<60);if(!source||!target)throw new Error("No safe high-pressure source and low-pressure target node pair is available.");payload={version_id:versionId,source_node_id:source.id,target_node_id:target.id};}
        const result=await API.action(name,payload),id=jobIdFromResponse(name,result);
        toast(name.charAt(0).toUpperCase()+name.slice(1)+" accepted",id?"Tracking "+id:"Part B accepted the request.");if(id)pollJob(name,id);return;
      }
      toast(name.charAt(0).toUpperCase()+name.slice(1)+" started","Safe demo control-plane simulation is running.");await runDemoAction(name);
    }catch(error){toast("Operation failed",error.message||"Unexpected frontend error");}
  }
  async function selectObject(id){
    state.selectedObject=id;state.detailLoading=CONFIG.mode==="api";renderAll();if(CONFIG.mode!=="api")return;
    try{
      const o=DATA.objects.find(item=>item.id===id);if(!o)throw new Error("Object not found in the current catalog.");
      const detail=await API.objectDetails(id),metadata=detail.metadata||{},versions=detail.versions||[],current=versions.find(v=>v.version_id===metadata.current_version_id)||versions[0]||{},replicas=Number(current.healthy_replicas);
      o.currentVersionId=metadata.current_version_id||current.version_id||o.currentVersionId;o.currentVersion=current;o.version=current.version_number?"v"+current.version_number:(o.currentVersionId||"current");o.size=Number.isFinite(Number(current.size_bytes))?formatBytes(Number(current.size_bytes)):"—";o.checksum=current.checksum||"—";o.replicas=Number.isFinite(replicas)?replicas+"/3":"—";o.status=String(current.state||metadata.state||"ACTIVE").toUpperCase()==="CORRUPTED"?"corrupted":(Number.isFinite(replicas)&&replicas<3?"degraded":"healthy");o.type=metadata.content_type||metadata.type||o.type||"object";o.created=formatTimestamp(metadata.created_at||o.created);o.updated=formatTimestamp(metadata.updated_at||o.updated);o.replicaRows=[];
    }catch(error){toast("Object details unavailable",error.message);}finally{state.detailLoading=false;renderAll();}
  }
  function openModal(){const m=$("#modal");m.classList.add("open");m.setAttribute("aria-hidden","false");setTimeout(()=>$("#file-input").focus(),20);}
  function closeModal(){const m=$("#modal");m.classList.remove("open");m.setAttribute("aria-hidden","true");$("#progress-wrap").hidden=true;$("#progress-bar").style.width="0%";$("#progress-value").textContent="0%";$("#file-name").textContent="No file selected";$("#upload-btn").disabled=true;$("#file-input").value="";}
  async function uploadFile(){
    const file=$("#file-input").files[0];if(!file)return;$("#upload-btn").disabled=true;$("#progress-wrap").hidden=false;
    if(CONFIG.mode==="mock"){
      let p=0;const timer=setInterval(async()=>{p=Math.min(100,p+Math.max(8,Math.floor(Math.random()*14)));$("#progress-value").textContent=p+"%";$("#progress-bar").style.width=p+"%";$("#progress-bar").setAttribute("aria-valuenow",String(p));$("#progress-text").textContent=p<70?"Streaming object":"Verifying replicas";if(p<100)return;clearInterval(timer);const r=await API.upload(file),mb=(file.size/1048576).toFixed(1);DATA.dashboard.objects+=1;DATA.objects.unshift({id:r.object_id,size:mb+" MB",version:r.version_id,replicas:"3/3",checksum:"pending…",status:"healthy",updated:"just now",type:file.type||"application/octet-stream",created:new Date().toLocaleString(),currentVersionId:r.version_id,replicaRows:[["node-01","HEALTHY",mb+" MB"],["node-02","HEALTHY",mb+" MB"],["node-03","HEALTHY",mb+" MB"]]});DATA.events.unshift({type:"success",icon:"↑",title:"Object committed",body:file.name+" uploaded in demo mode with RF 3.",relative:"just now"});$("#progress-text").textContent="Committed · replicas verified";setTimeout(()=>{closeModal();showView("objects");toast("Upload committed",file.name+" is protected with three replicas.");},450);},120);return;
    }
    $("#progress-value").textContent="25%";$("#progress-bar").style.width="25%";$("#progress-bar").setAttribute("aria-valuenow","25");$("#progress-text").textContent="Uploading through Part B…";
    try{await API.upload(file);$("#progress-value").textContent="100%";$("#progress-bar").style.width="100%";$("#progress-bar").setAttribute("aria-valuenow","100");$("#progress-text").textContent="Accepted · syncing catalog";await API.sync();setTimeout(()=>{closeModal();showView("objects");toast("Upload accepted",file.name+" is now in the live object catalog.");},350);}
    catch(error){closeModal();toast("Upload failed",error.message);}
  }
  function openDrill(){if(CONFIG.mode==="api"){toast("Demo-only control","The resilience drill never changes Part B state.");return;}const m=$("#drill-modal");m.classList.add("open");m.setAttribute("aria-hidden","false");state.drillRunning=false;resetDrill();setTimeout(()=>$("#drill-run").focus(),20);}
  function closeDrill(){const m=$("#drill-modal");m.classList.remove("open");m.setAttribute("aria-hidden","true");state.drillRunning=false;}
  function resetDrill(){$$(".drill-step").forEach((el,i)=>el.classList.toggle("active",i===0));$("#drill-status-title").textContent="Ready to simulate a node failure";$("#drill-status-copy").textContent="Detect → isolate → repair → verify.";$("#drill-console").innerHTML="<code>&gt; awaiting start...</code>";$("#drill-run").disabled=false;$("#drill-run").textContent="Start drill";}
  async function runDrill(){
    if(state.drillRunning)return;state.drillRunning=true;$("#drill-run").disabled=true;$("#drill-run").textContent="Running…";
    const steps=[["Detect","node-04 misses its heartbeat window.","failure detector → SUSPECTED"],["Isolate","Reads are pinned to healthy replicas.","read path → node-01 / node-02 / node-03"],["Repair","A verified copy restores the missing replica.","repair worker → RF 3 restored"],["Verify","Checksum matches the committed version.","integrity scanner → PASS"]];
    for(let i=0;i<steps.length;i++){$$(".drill-step").forEach((el,j)=>el.classList.toggle("active",j<=i));$("#drill-status-title").textContent=steps[i][0];$("#drill-status-copy").textContent=steps[i][1];$("#drill-console").innerHTML="<code>&gt; "+escapeHtml(steps[i][2])+"</code>";await new Promise(r=>setTimeout(r,700));}
    DATA.events.unshift({type:"success",icon:"⚡",title:"Resilience drill completed",body:"A simulated node failure was detected, isolated, repaired and checksum-verified without data loss.",relative:"just now"});state.drillRunning=false;$("#drill-run").disabled=false;$("#drill-run").textContent="Run again";toast("Resilience drill complete","Zero-data-loss recovery path demonstrated locally.");renderAll();
  }
  const commands=[
    {label:"Go to Overview",keys:"1",run:()=>showView("overview")},{label:"Go to Nodes",keys:"2",run:()=>showView("nodes")},{label:"Go to Objects",keys:"3",run:()=>showView("objects")},{label:"Go to Repairs",keys:"4",run:()=>showView("repairs")},{label:"Go to Integrity",keys:"5",run:()=>showView("integrity")},{label:"Go to Rebalance",keys:"6",run:()=>showView("rebalance")},{label:"Open Events",keys:"7",run:()=>showView("events")},{label:"Open Policies",keys:"8",run:()=>showView("policies")},{label:"Upload object",keys:"U",run:openModal},{label:"Run integrity scan",keys:"I",run:()=>runAction("integrity")},{label:"Run resilience drill",keys:"D",run:openDrill},{label:"Refresh telemetry",keys:"R",run:async()=>{try{await API.sync();renderAll();toast("Refreshed",CONFIG.mode==="api"?"Live Vault telemetry synchronized.":"Demo telemetry refreshed.");}catch(e){toast("Refresh failed",e.message);}}}
  ];
  function renderCommands(){const q=($("#command-input")?.value||"").toLowerCase(),list=commands.filter(c=>c.label.toLowerCase().includes(q));state.commandIndex=Math.min(state.commandIndex,Math.max(0,list.length-1));$("#command-list").innerHTML=list.map((c,i)=>"<button class='command-item "+(i===state.commandIndex?"active":"")+"' data-command-label='"+escapeHtml(c.label)+"'><span>"+escapeHtml(c.label)+"</span><kbd>"+escapeHtml(c.keys)+"</kbd></button>").join("")||"<div class='empty command-empty'><b>⌕</b><h3>No command found</h3><p>Try “upload”, “repair”, or a view name.</p></div>";}
  function openCommand(){const m=$("#command-modal");m.classList.add("open");m.setAttribute("aria-hidden","false");state.commandIndex=0;renderCommands();setTimeout(()=>$("#command-input").focus(),20);}
  function closeCommand(){const m=$("#command-modal");m.classList.remove("open");m.setAttribute("aria-hidden","true");}
  function runCommand(label){const c=commands.find(item=>item.label===label);if(c){closeCommand();c.run();}}
  document.addEventListener("click",async e=>{
    const nav=e.target.closest("[data-view]");if(nav){showView(nav.dataset.view);return;}
    const action=e.target.closest("[data-action]");
    if(action){
      const a=action.dataset.action;
      if(a==="upload")openModal();else if(a==="close-modal")closeModal();else if(a==="upload-file")await uploadFile();else if(a==="integrity")await runAction("integrity");else if(a==="repair")await runAction("repair");else if(a==="rebalance")await runAction("rebalance");else if(a==="refresh"){try{await API.sync();renderAll();toast("Refreshed",CONFIG.mode==="api"?"Live Vault telemetry synchronized.":"Demo telemetry refreshed.");}catch(error){toast("Refresh failed",error.message);}}else if(a==="clear-events"){if(CONFIG.mode==="api")toast("Read-only feed","Live event history comes from Part B and is not deleted by the frontend.");else{DATA.events=[];renderAll();toast("Demo alerts cleared","Local event history was cleared.");}}else if(a==="drill")openDrill();else if(a==="close-drill")closeDrill();else if(a==="run-drill")await runDrill();
    }
    const obj=e.target.closest("[data-object]");if(obj){await selectObject(obj.dataset.object);return;}
    const node=e.target.closest("[data-node]");
    if(node){const n=DATA.nodes.find(x=>x.id===node.dataset.node);if(!n)return;if(CONFIG.mode==="api"){toast("Demo-only control","Part B exposes no public drain/resume endpoint in the documented contract.");return;}n.lifecycle=node.dataset.nodeAction==="drain"?"DRAINING":"HEALTHY";n.status=n.lifecycle==="DRAINING"?"attention":"healthy";renderAll();toast(n.id,n.lifecycle==="DRAINING"?"Node is now draining in the local simulation.":"Node resumed in the local simulation.");}
    const cmd=e.target.closest("[data-command-label]");if(cmd)runCommand(cmd.dataset.commandLabel);
  });
  const handleSearchInput=debounce(e=>{if(e.target.id==="node-search")renderNodes();if(e.target.id==="object-search")renderObjects();if(e.target.id==="command-input"){state.commandIndex=0;renderCommands();}});
  document.addEventListener("input",handleSearchInput);
  $("#node-filters").addEventListener("click",e=>{const b=e.target.closest("[data-filter]");if(b)setFilter("nodeFilter",b.dataset.filter);});
  $("#object-filters").addEventListener("click",e=>{const b=e.target.closest("[data-filter]");if(b)setFilter("objectFilter",b.dataset.filter);});
  $("#event-filters").addEventListener("click",e=>{const b=e.target.closest("[data-filter]");if(b)setFilter("eventFilter",b.dataset.filter);});
  $("#refresh").addEventListener("click",async()=>{try{await API.sync();renderAll();toast("Refreshed",CONFIG.mode==="api"?"Live Vault telemetry synchronized.":"Demo telemetry refreshed.");}catch(error){renderAll();toast("Refresh failed",error.message);}});
  $("#command-open").addEventListener("click",openCommand);
  $("#command-input").addEventListener("keydown",e=>{const items=$$("#command-list .command-item");if(e.key==="ArrowDown"){e.preventDefault();state.commandIndex=Math.min(state.commandIndex+1,Math.max(0,items.length-1));renderCommands();}if(e.key==="ArrowUp"){e.preventDefault();state.commandIndex=Math.max(0,state.commandIndex-1);renderCommands();}if(e.key==="Enter"){e.preventDefault();items[state.commandIndex]?.click();}});
  const drop=document.querySelector(".drop");
  $("#file-input").addEventListener("change",()=>{const f=$("#file-input").files[0];if(f){try{validateUploadFile(f);}catch(error){$("#file-input").value="";$("#file-name").textContent="No file selected";$("#upload-btn").disabled=true;toast("Invalid file",error.message);return;}}$("#file-name").textContent=f?f.name+" · "+(f.size/1048576).toFixed(2)+" MB":"No file selected";$("#upload-btn").disabled=!f;});
  ["dragenter","dragover"].forEach(type=>drop.addEventListener(type,e=>{e.preventDefault();drop.classList.add("dragging");}));
  ["dragleave","drop"].forEach(type=>drop.addEventListener(type,e=>{e.preventDefault();drop.classList.remove("dragging");}));
  drop.addEventListener("drop",e=>{const file=e.dataTransfer.files[0];if(!file)return;try{validateUploadFile(file);}catch(error){toast("Invalid file",error.message);return;}const input=$("#file-input");if(typeof DataTransfer!=="undefined"){const transfer=new DataTransfer();transfer.items.add(file);input.files=transfer.files;}$("#file-name").textContent=file.name+" · "+(file.size/1048576).toFixed(2)+" MB";$("#upload-btn").disabled=false;});
  $("#modal").addEventListener("click",e=>{if(e.target.id==="modal")closeModal();});
  $("#drill-modal").addEventListener("click",e=>{if(e.target.id==="drill-modal")closeDrill();});
  $("#command-modal").addEventListener("click",e=>{if(e.target.id==="command-modal")closeCommand();});
  document.addEventListener("keydown",e=>{
    if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==="k"){e.preventDefault();openCommand();return;}
    if(e.key==="Escape"){closeModal();closeDrill();closeCommand();}
    const tag=(e.target.tagName||"").toLowerCase();
    if(!["input","textarea","select"].includes(tag)){
      const key=e.key.toLowerCase();
      const map={u:openModal,d:openDrill,r:()=>API.sync().then(()=>{renderAll();toast("Refreshed","Telemetry synchronized.");}).catch(err=>toast("Refresh failed",err.message)),i:()=>runAction("integrity")};
      if(map[key]&&!((e.ctrlKey||e.metaKey)))map[key]();
    }
  });
  renderAll();
  const hash=location.hash.slice(1);showView(ROUTES.includes(hash)?hash:"overview");
  API.sync().then(()=>renderAll()).catch(error=>{if(CONFIG.mode==="api")toast("API unavailable",error.message);renderAll();});
  window.VaultFrontend={API,DATA,state,showView,openDrill,runDrill};
})();