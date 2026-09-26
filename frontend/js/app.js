(() => {
  "use strict";

  const CONFIG = window.VAULT_CONFIG || { mode: "api", baseUrl: "/api/v1" };

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
    if (file.size > MAX_UPLOAD_BYTES) throw new Error("File exceeds the 100 MB upload limit.");
    if (!SAFE_FILENAME.test(file.name)) throw new Error("File name contains invalid control characters.");
  }
  const ROUTES = ["overview","nodes","objects","repairs","integrity","rebalance","events","policies","guide","about"];
  const DATA = {
    dashboard:{objects:0},
    nodes:[],
    objects:[],
    repairs:[],
    integrity:[],
    rebalance:[],
    events:[],
    liveHealth:null,
    policies:null,
    sync:{status:"connecting",at:null,error:null,admin:"unknown"},
    detailLoading:false
  };

  const state = {view:"overview",nodeFilter:"all",objectFilter:"all",eventFilter:"all",selectedObject:null,commandIndex:0};
  const STATUS_META=Object.freeze({
    healthy:["green","HEALTHY"],
    attention:["amber","ATTENTION"],
    degraded:["amber","DEGRADED"],
    corrupted:["red","CORRUPTED"],
    running:["amber","RUNNING"],
    success:["green","SUCCEEDED"],
    draining:["amber","DRAINING"]
  });
  const BYTE_UNITS=Object.freeze(["B","KB","MB","GB","TB"]);
  const BYTE_MULTIPLIERS=Object.freeze({B:1,KB:1024,MB:1024**2,GB:1024**3,TB:1024**4});
  const ADMIN_PATHS=Object.freeze({
    repair:"/admin/repair",
    integrity:"/admin/integrity/check",
    rebalance:"/admin/rebalance"
  });
  const ADMIN_JOB_PATHS=Object.freeze({
    repair:"/admin/repair/",
    integrity:"/admin/integrity/check/",
    rebalance:"/admin/rebalance/"
  });
  const TERMINAL_JOB_STATES=new Set(["SUCCEEDED","COMPLETED","DONE","FAILED","ERROR"]);
  const $=(s,r=document)=>r.querySelector(s);
  const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));
  const escapeHtml=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const formatBytes=bytes=>{
    if(!Number.isFinite(bytes)||bytes<0)return "—";
    if(bytes===0)return "0 B";
    let i=0,value=bytes;
    while(value>=1024&&i<BYTE_UNITS.length-1){value/=1024;i++;}
    return (value>=100?value.toFixed(0):value.toFixed(1))+" "+BYTE_UNITS[i];
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
    return Number(m[1])*BYTE_MULTIPLIERS[m[2].toUpperCase()];
  };

  const API={
    mode:CONFIG.mode,
    baseUrl:resolveApiBaseUrl(CONFIG.baseUrl),
    async request(path,options={}){
      const headers=new Headers(options.headers||{});
      const requestId=options.requestId||("req_"+(crypto.randomUUID?crypto.randomUUID().replaceAll("-",""):Date.now().toString(36)));
      headers.set("Accept","application/json");
      headers.set("Cache-Control","no-cache");
      headers.set("X-Request-ID",requestId);
      const adminKey=sessionStorage.getItem("vault_admin_key")||"";
      if(adminKey && path.startsWith("/admin/"))headers.set("X-Admin-Key",adminKey);
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
      if(!ADMIN_PATHS[name])throw new Error("Unsupported admin action.");
      return this.request(ADMIN_PATHS[name],{method:"POST",body:JSON.stringify(payload||{}),headers:{"Content-Type":"application/json"}});
    },
    async job(name,id){
      if(!ADMIN_JOB_PATHS[name])throw new Error("Unsupported admin job.");
      return this.get(ADMIN_JOB_PATHS[name]+encodeURIComponent(id));
    },
    async upload(file){
      validateUploadFile(file);
      const objectName=file.name.replace(/\\/g,"/").split("/").pop()||"upload";
      return this.request("/objects/"+encodeURIComponent(objectName),{method:"PUT",body:file,headers:{"Content-Type":file.type||"application/octet-stream"}});
    },
    async objectDetails(name){
      const encoded=encodeURIComponent(name);
      const [metadata,versions]=await Promise.all([this.get("/objects/"+encoded+"/metadata"),this.get("/objects/"+encoded+"/versions")]);
      return {metadata,versions:Array.isArray(versions)?versions:[]};
    },
    async jobs(name){
      const paths={repair:"/admin/repair",integrity:"/admin/integrity/check",rebalance:"/admin/rebalance"};
      return this.get(paths[name]);
    },
    async sync(){
      stateBusy(true);
      const results=await Promise.allSettled([
        this.get("/health"),
        this.get("/nodes"),
        this.get("/objects"),
        this.get("/policies"),
        this.jobs("repair"),
        this.jobs("integrity"),
        this.jobs("rebalance")
      ]);
      const [health,nodes,objects,policies,repairs,integrity,rebalance]=results;
      const errors=[];
      if(health.status==="fulfilled")DATA.liveHealth=health.value;else errors.push("health");
      if(nodes.status==="fulfilled"&&Array.isArray(nodes.value)){
        DATA.nodes=nodes.value.map(n=>{
          const capacity=Number(n.capacity_bytes||0),used=Number(n.used_bytes||0);
          const percent=capacity?Math.min(100,Math.round((used/capacity)*100)):0;
          const rawStatus=String(n.status||"UNKNOWN").toUpperCase();
          const healthy=["HEALTHY","READY"].includes(rawStatus);
          return {id:n.node_id,status:healthy?"healthy":"attention",capacity:formatBytes(capacity),used:formatBytes(used),percent,objects:"—",heartbeat:formatHeartbeat(n.last_heartbeat_at),lifecycle:rawStatus,capacityBytes:capacity,usedBytes:used,address:n.address,lastHeartbeat:n.last_heartbeat_at};
        });
      }else errors.push("nodes");
      if(policies.status==="fulfilled")DATA.policies=policies.value;else errors.push("policies");
      if(objects.status==="fulfilled"&&Array.isArray(objects.value)){DATA.objects=objects.value.map(normalizeLiveObject);DATA.dashboard.objects=DATA.objects.length;}else errors.push("objects");
      DATA.sync.admin="unknown";
      if(repairs.status==="fulfilled"&&Array.isArray(repairs.value)){DATA.repairs=repairs.value;DATA.sync.admin="ready";}else if(repairs.status==="rejected")DATA.sync.admin="locked";
      if(integrity.status==="fulfilled"&&Array.isArray(integrity.value))DATA.integrity=integrity.value;
      if(rebalance.status==="fulfilled"&&Array.isArray(rebalance.value))DATA.rebalance=rebalance.value;
      DATA.sync={...DATA.sync,status:errors.length?"degraded":"live",at:new Date(),error:errors.length?("Failed: "+errors.join(", ")):null};
      buildLiveEvents();
      stateBusy(false);renderAll();
      if(errors.length)throw new Error(DATA.sync.error);
    }
  };

  function normalizeLiveObject(o){
    const active=String(o.state||"").toUpperCase()==="ACTIVE";
    const name=o.name||"";
    const healthyReplicas=Number(o.healthy_replicas);
    const versionState=String(o.version_state||"").toUpperCase();
    const statusValue=versionState==="CORRUPTED"?"corrupted":(Number.isFinite(healthyReplicas)&&DATA.policies&&healthyReplicas<DATA.policies.replication_factor?"degraded":(active?"healthy":"attention"));
    return {id:name||o.object_id||"unknown",name:name||o.object_id||"unknown",objectId:o.object_id||"",size:Number.isFinite(Number(o.size_bytes))?formatBytes(Number(o.size_bytes)):"—",version:Number.isFinite(Number(o.current_version))?("v"+o.current_version):(o.current_version_id?"current":"—"),replicas:Number.isFinite(healthyReplicas)?healthyReplicas+"/"+(DATA.policies?.replication_factor||3):"—",checksum:o.checksum||"—",status:statusValue,updated:formatTimestamp(o.updated_at),type:"object",created:formatTimestamp(o.created_at),currentVersionId:o.current_version_id||null,currentVersion:o.current_version?{version_number:o.current_version,size_bytes:o.size_bytes,checksum:o.checksum,state:o.version_state}:null,replicaRows:[],live:true};
  }
  function toast(title,message){const t=document.createElement("div");t.className="toast";t.innerHTML="<b>"+escapeHtml(title)+"</b><small>"+escapeHtml(message)+"</small>";$("#toasts").appendChild(t);setTimeout(()=>t.remove(),4200);}
  function stateBusy(flag){document.body.classList.toggle("is-syncing",!!flag);const refresh=$("#refresh");if(refresh)refresh.disabled=!!flag;const main=document.getElementById("main-content");if(main)main.setAttribute("aria-busy",String(!!flag));}
  function showView(view){
    if(!ROUTES.includes(view))view="overview";
    state.view=view;
    $(".nav-item").forEach(b=>{const active=b.dataset.view===view;b.classList.toggle("active",active);if(active)b.setAttribute("aria-current","page");else b.removeAttribute("aria-current");});
    $$(".view").forEach(v=>v.classList.toggle("visible",v.id==="view-"+view));
    $("#view-title").textContent=view.charAt(0).toUpperCase()+view.slice(1);
    history.replaceState(null,"","#"+view);
    window.scrollTo({top:0,behavior:"smooth"});
    renderAll();
  }
  function status(s){
    const m=STATUS_META[s]||["green",String(s||"unknown").toUpperCase()];
    return "<span class='status "+m[0]+"'><i></i>"+escapeHtml(m[1])+"</span>";
  }
  function renderEnvironment(){
    const live=CONFIG.mode==="api", ok=live?DATA.sync.status==="live":true;
    $("#env-label").textContent=live?(ok?"LIVE API":"API "+String(DATA.sync.status||"CONNECTING").toUpperCase()):"API REQUIRED";
    $("#env-detail").textContent=live?(DATA.sync.at?(DATA.sync.admin==="locked"?"Connected · admin key required for controls":"Connected · Part B live"):"Connecting to Part B…"):"Live Part B required";
    $("#api-badge").textContent=live?(DATA.sync.admin==="locked"?"LIVE API · ADMIN LOCKED":"LIVE API"):"API";
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
    const usedPct=totalCapacity?Math.round(totalUsed/totalCapacity*100):0,headroom=Math.max(0,100-usedPct),liveBad=DATA.liveHealth?.status==="degraded";
    const score=nodes.length?Math.max(0,Math.min(100,Math.round((healthy/nodes.length)*100)-(liveBad?4:0))):0;
    $("#resilience-score").innerHTML=score.toFixed(1)+"<small>/100</small>";$("#score-meter").style.width=score+"%";$("#score-meter").parentElement.setAttribute("aria-valuenow",String(score));$("#score-delta").textContent=attention?("−"+Math.min(3,attention*0.9).toFixed(1)+"%"):"+1.8%";
    const usedLabel=totalUsed?formatBytes(totalUsed):"—";$("#storage-used").innerHTML=usedLabel.includes(" ")?usedLabel.replace(" ","<small> ")+"</small>":usedLabel;$("#storage-chip").textContent=usedPct+"%";$("#storage-meter").style.width=Math.min(100,usedPct)+"%";$("#storage-meter").parentElement.setAttribute("aria-valuenow",String(Math.min(100,usedPct)));$("#storage-caption").textContent=totalCapacity?formatBytes(totalCapacity)+" total logical capacity":"2.0 TB logical capacity";
    $("#signal-headroom").textContent=nodes.length?headroom+"% remaining":"—";$("#healthy-replicas").innerHTML=nodes.length?Math.max(0,100-(attention/Math.max(nodes.length,1)*100)).toFixed(1)+"<small>%</small>":"—";
    $("#nodes-count").textContent=nodes.length;$("#nodes-summary").textContent=healthy+" healthy · "+attention+" attention";
    const free=totalCapacity-totalUsed;const freeLabel=free>0?formatBytes(free):"—";$("#available-capacity").innerHTML=freeLabel.includes(" ")?freeLabel.replace(" ","<small> ")+"</small>":freeLabel;$("#capacity-summary").textContent=headroom+"% cluster headroom";
    $("#write-acceptance").innerHTML=(nodes.length?(healthy/Math.max(nodes.length,1)*100):0).toFixed(0)+"<small>%</small>";$("#write-summary").textContent=nodes.length?(attention?"Some nodes need attention":"All nodes accepting writes"):"Waiting for live node telemetry";
    const heartbeatSeconds=nodes.map(n=>parseFloat(String(n.heartbeat).replace("s",""))).filter(Number.isFinite);const median=heartbeatSeconds.length?heartbeatSeconds.sort((a,b)=>a-b)[Math.floor(heartbeatSeconds.length/2)]:0;$("#median-heartbeat").innerHTML=median.toFixed(1)+"<small>s</small>";
    $("#health-ring-score").textContent=score.toFixed(1);$("#health-ring").setAttribute("aria-valuenow",String(score));$("#health-ring").style.background="conic-gradient(var(--green) 0 "+score+"%,#193042 "+score+"% 100%)";
    $("#cluster-badge").textContent=!nodes.length?"WAITING":(attention||liveBad?"ATTENTION":"HEALTHY");$("#cluster-badge").className="badge "+(!nodes.length?"":(attention||liveBad?"amber":"green"));$("#cluster-summary").textContent=!nodes.length?"Waiting for Part B telemetry":(attention||liveBad?"Cluster requires attention":"All reported services operational");
    const attentionNode=nodes.find(n=>n.status==="attention");$("#cluster-copy").textContent=attentionNode?(attentionNode.id+" needs attention. Reads remain available."):(nodes.length?"No current node requires attention.":"Waiting for live node telemetry.");
    const hot=nodes.find(n=>n.percent>=80);$("#ops-banner").classList.toggle("good-news",!hot);$("#ops-banner-title").textContent=hot?hot.id+" is above the configured 80% watermark":(nodes.length?"Cluster is inside the configured capacity watermark":"Waiting for live capacity telemetry");$("#ops-banner-copy").textContent=hot?"Review Rebalance before capacity pressure becomes a failure mode.":(nodes.length?"No node is currently above 80% used.":"No capacity data is available yet.");
  }
  function updateTopology(){
    DATA.nodes.slice(0,4).forEach(n=>{const el=$("#topology-"+n.id);if(!el)return;const dot=el.querySelector(".dot"),name=el.querySelector("b"),meta=el.querySelector("small");dot.className="dot "+(n.status==="healthy"?"good":"warn");name.textContent=n.id;meta.textContent=n.percent+"% used";el.classList.toggle("attention-node",n.status!=="healthy");});
  }

  function objectByVersion(versionId){
    return DATA.objects.find(o=>o.currentVersionId===versionId)||null;
  }
  function objectLabelForVersion(versionId){
    const o=objectByVersion(versionId);
    return o?.name||versionId||"Unknown object";
  }
  function buildLiveEvents(){
    const rows=[];
    DATA.nodes.filter(n=>n.status!=="healthy").forEach(n=>{
      rows.push({type:"warning",icon:"!",title:n.id+" needs attention",body:(n.lifecycle||"NODE")+" · "+n.percent+"% capacity used.",relative:n.lastHeartbeat?formatTimestamp(n.lastHeartbeat):"now"});
    });
    DATA.repairs.slice(0,8).forEach(r=>{
      const stateText=String(r.status||"UNKNOWN").toUpperCase();
      rows.push({type:stateText==="FAILED"||stateText==="ERROR"?"failure":(["COMPLETED","SUCCEEDED"].includes(stateText)?"success":"warning"),icon:"↻",title:"Repair "+stateText.toLowerCase(),body:objectLabelForVersion(r.version_id)+" · "+(r.source_node_id||"source")+" → "+(r.target_node_id||"target"),relative:formatTimestamp(r.updated_at||r.created_at)});
    });
    DATA.integrity.slice(0,8).forEach(r=>{
      const stateText=String(r.status||"UNKNOWN").toUpperCase();
      rows.push({type:stateText==="FAILED"||stateText==="ERROR"?"failure":"success",icon:"✓",title:"Integrity check "+stateText.toLowerCase(),body:(r.node_id||"cluster")+" · checked "+(r.checked_count??0)+" replicas · corrupted "+(r.corrupted_count??0),relative:formatTimestamp(r.updated_at||r.created_at)});
    });
    DATA.rebalance.slice(0,8).forEach(r=>{
      const stateText=String(r.status||"UNKNOWN").toUpperCase();
      rows.push({type:stateText==="FAILED"||stateText==="ERROR"?"failure":(["COMPLETED","SUCCEEDED"].includes(stateText)?"success":"warning"),icon:"⇄",title:"Rebalance "+stateText.toLowerCase(),body:(r.source_node_id||"source")+" → "+(r.target_node_id||"target"),relative:formatTimestamp(r.updated_at||r.created_at)});
    });
    if(!rows.length&&DATA.sync.status==="live")rows.push({type:"success",icon:"✓",title:"Control plane synchronized",body:"No repair, integrity, or rebalance jobs are currently reported by Part B.",relative:formatTimestamp(DATA.sync.at)});
    DATA.events=rows.slice(0,30);
  }
  function renderServiceStatus(){
    const el=$("#service-status-list");
    if(!el)return;
    const apiOk=DATA.sync.status==="live";
    const rows=[
      ["Control plane",apiOk?"Connected":"Unavailable",apiOk?"good":"warn"],
      ["Storage nodes",DATA.nodes.length?DATA.nodes.filter(n=>n.status==="healthy").length+" / "+DATA.nodes.length+" healthy":"No telemetry",DATA.nodes.length&&DATA.nodes.every(n=>n.status==="healthy")?"good":"warn"],
      ["Repair jobs",DATA.repairs.length+" recorded",DATA.sync.admin==="ready"?"good":"warn"],
      ["Integrity jobs",DATA.integrity.length+" recorded",DATA.sync.admin==="ready"?"good":"warn"],
      ["Rebalance jobs",DATA.rebalance.length+" recorded",DATA.sync.admin==="ready"?"good":"warn"]
    ];
    el.innerHTML=rows.map(r=>"<div><span>"+escapeHtml(r[0])+"</span><b><i class='service "+r[2]+"'></i>"+escapeHtml(r[1])+"</b></div>").join("");
  }
  function renderWatchlist(){
    const el=$("#watchlist");
    if(!el)return;
    const items=[];
    DATA.nodes.filter(n=>n.percent>=80||n.status!=="healthy").slice(0,3).forEach(n=>{
      items.push("<div class='watch warning'><b>"+escapeHtml(n.id)+" needs attention</b><span>"+escapeHtml(n.lifecycle)+" · "+n.percent+"% used</span><button data-view='rebalance'>Review</button></div>");
    });
    DATA.repairs.filter(r=>!["SUCCEEDED","COMPLETED"].includes(String(r.status||"").toUpperCase())).slice(0,2).forEach(r=>{
      items.push("<div class='watch'><b>Repair "+escapeHtml(r.status||"PENDING")+"</b><span>"+escapeHtml(objectLabelForVersion(r.version_id))+"</span><button data-view='repairs'>Open</button></div>");
    });
    if(!items.length)items.push("<div class='empty' style='padding:16px'><b>✓</b><h3>Nothing needs attention</h3><p>Part B reports no current node or repair warning.</p></div>");
    el.innerHTML=items.join("");
  }
  function renderPolicy(){
    const p=DATA.policies;
    const keyLoaded=!!sessionStorage.getItem("vault_admin_key");
    const keyStatus=$("#admin-key-status");
    if(keyStatus){keyStatus.textContent=keyLoaded?"Key loaded for this session":"Key not loaded";keyStatus.className=keyLoaded?"green-text":"";}
    const keyInput=$("#admin-key-input");
    if(keyInput&&document.activeElement!==keyInput)keyInput.value="";
    if(!p)return;
    const set=(id,v)=>{const e=$("#"+id);if(e)e.value=v;};
    set("policy-rf",p.replication_factor);set("policy-wq",p.write_quorum);set("policy-rq",p.read_quorum);set("policy-chunk",formatBytes(p.chunk_size_bytes));
    set("policy-heartbeat",p.heartbeat_interval_seconds+" s");set("policy-suspect",p.suspect_after_seconds+" s");set("policy-unavailable",p.unavailable_after_seconds+" s");set("policy-parallel",p.max_concurrent_jobs);
    $("#signal-policy").textContent="RF "+p.replication_factor+" · W"+p.write_quorum+" · R"+p.read_quorum;
    $("#signal-heal").textContent=DATA.repairs.length+" repair jobs";
  }
  function renderNodes(){
    const q=($("#node-search")?.value||"").toLowerCase();
    const rows=DATA.nodes.filter(n=>{
      const filterOk=state.nodeFilter==="all"||(state.nodeFilter==="healthy"&&n.status==="healthy")||(state.nodeFilter==="attention"&&n.status==="attention");
      const searchOk=!q||(n.id+" "+n.lifecycle).toLowerCase().includes(q);
      return filterOk&&searchOk;
    });
    $("#nodes-body").innerHTML=rows.length?rows.map(n=>{
      return "<tr><td class='objid'>"+escapeHtml(n.id)+"</td><td>"+status(n.status)+"</td><td>"+n.capacity+"</td><td><b>"+n.used+"</b> <small>"+n.percent+"%</small></td><td>"+(typeof n.objects==="number"?n.objects.toLocaleString():"—")+"</td><td>"+escapeHtml(n.heartbeat)+"</td><td><span class='chip "+(n.lifecycle==="HEALTHY"?"green":"amber")+"'>"+escapeHtml(n.lifecycle)+"</span></td></tr>";
    }).join(""):"<tr><td colspan='7' class='empty-row'>No nodes match this filter.</td></tr>";
  }
  function renderObjects(){
    const q=($("#object-search")?.value||"").toLowerCase();
    const rows=DATA.objects.filter(o=>{
      const filterOk=state.objectFilter==="all"||o.status===state.objectFilter;
      const searchOk=!q||((o.name||o.id)+" "+o.id+" "+o.checksum+" "+o.status).toLowerCase().includes(q);
      return filterOk&&searchOk;
    });
    $("#objects-body").innerHTML=rows.length?rows.map(o=>"<tr><td class='objid'><b>"+escapeHtml(o.name||o.id)+"</b><small class='object-id'>"+escapeHtml(o.id)+"</small></td><td>"+escapeHtml(o.size)+"</td><td>"+escapeHtml(o.version)+"</td><td>"+escapeHtml(o.replicas)+"</td><td class='hash'>"+escapeHtml(o.checksum)+"</td><td>"+status(o.status)+"</td><td>"+escapeHtml(o.updated)+"</td><td><button class='table-action' data-object='"+escapeHtml(o.id)+"' aria-label='Inspect "+escapeHtml(o.name||o.id)+"'>Inspect</button></td></tr>").join(""):"<tr><td colspan='8' class='empty-row'>No objects match this filter.</td></tr>";
    const selected=DATA.objects.find(o=>o.id===state.selectedObject);
    if(state.detailLoading)$("#object-detail").innerHTML="<div class='empty loading-state'><b>↻</b><h3>Loading object details</h3><p>Fetching metadata, versions and replicas from Part B…</p></div>";
    else if(selected)renderDetail(selected);
    else $("#object-detail").innerHTML="<div class='empty'><b>▦</b><h3>Select an object</h3><p>Replica placement and version details will appear here.</p></div>";
  }
  function renderDetail(o){
    const liveVersion=o.currentVersion||{};
    const replicaCards=o.replicaRows?.length?o.replicaRows.map(r=>{
      const replicaStatus=String(r[1]||"").toUpperCase();
      return "<div class='replica'><b>"+escapeHtml(r[0])+"</b><small>"+escapeHtml(r[2]||"—")+"</small>"+status(replicaStatus==="HEALTHY"?"healthy":replicaStatus==="CORRUPTED"?"corrupted":"attention")+"</div>";
    }).join(""):"<div class='empty' style='grid-column:1/-1;padding:16px'><b>i</b><h3>Replica placement not returned</h3><p>Part B did not return replica details for this version.</p></div>";
    const placementPolicy=DATA.policies?"RF "+DATA.policies.replication_factor+" · W"+DATA.policies.write_quorum+" · R"+DATA.policies.read_quorum:"—";
    $("#object-detail").innerHTML="<div class='detail-grid'><div><div class='detail-hero'><p class='eyebrow'>OBJECT DETAIL</p><h2>"+escapeHtml(o.name||o.id)+"</h2><p class='object-id detail-id'>Object ID: "+escapeHtml(o.objectId||o.id)+"</p><div class='detail-meta'><span>"+escapeHtml(o.size)+"</span><span>Version "+escapeHtml(o.version)+"</span><span>"+status(o.status)+"</span><span>SHA-256 "+escapeHtml(o.checksum)+"</span></div></div><div style='padding-top:12px'><p class='eyebrow'>REPLICA PLACEMENT</p><div class='replicas'>"+replicaCards+"</div></div></div><div><p class='eyebrow'>METADATA</p><div class='service-list'><div><span>Content type</span><b>"+escapeHtml(o.type)+"</b></div><div><span>Created</span><b>"+escapeHtml(o.created)+"</b></div><div><span>Version ID</span><b>"+escapeHtml(o.currentVersionId||"—")+"</b></div><div><span>Version number</span><b>"+escapeHtml(liveVersion.version_number?("v"+liveVersion.version_number):o.version)+"</b></div><div><span>Healthy replicas</span><b>"+escapeHtml(o.replicas)+"</b></div><div><span>Placement policy</span><b>"+escapeHtml(placementPolicy)+"</b></div></div></div></div>";
  }
  function renderRepairs(){
    const active=DATA.repairs.filter(r=>!["SUCCEEDED","COMPLETED","FAILED","ERROR"].includes(String(r.status||"").toUpperCase())).length;
    $("#repair-active").textContent=active;
    $("#repair-total").textContent=DATA.repairs.length;
    $("#repair-completed").textContent=DATA.repairs.filter(r=>["SUCCEEDED","COMPLETED"].includes(String(r.status||"").toUpperCase())).length;
    $("#repair-failed").textContent=DATA.repairs.filter(r=>["FAILED","ERROR"].includes(String(r.status||"").toUpperCase())).length;
    $("#repair-grid").innerHTML=DATA.repairs.length?DATA.repairs.map(r=>{
      const stateText=String(r.status||"UNKNOWN").toUpperCase();
      const stateClass=["SUCCEEDED","COMPLETED"].includes(stateText)?"success":(["FAILED","ERROR"].includes(stateText)?"corrupted":"running");
      return "<article class='repair-card'><div class='repair-top'><div><p class='eyebrow'>"+escapeHtml(r.repair_id||"repair job")+"</p><h3>"+escapeHtml(objectLabelForVersion(r.version_id))+"</h3><small class='object-id'>Version: "+escapeHtml(r.version_id||"—")+"</small></div>"+status(stateClass)+"</div><p>"+escapeHtml(r.reason||r.last_error||"Durable repair job")+"</p><div class='route'><b>"+escapeHtml(r.source_node_id||"source pending")+"</b><span>→ verify →</span><b>"+escapeHtml(r.target_node_id||"target pending")+"</b></div><div class='service-list'><div><span>Attempts</span><b>"+escapeHtml(r.attempts??0)+"</b></div><div><span>Updated</span><b>"+escapeHtml(formatTimestamp(r.updated_at||r.created_at))+"</b></div><div><span>Last error</span><b>"+escapeHtml(r.last_error||"—")+"</b></div></div></article>";
    }).join(""):"<div class='empty-row'>Part B has no repair jobs.</div>";
  }
  function renderIntegrity(){
    const checked=DATA.integrity.reduce((sum,r)=>sum+Number(r.checked_count||0),0);
    const corrupted=DATA.integrity.reduce((sum,r)=>sum+Number(r.corrupted_count||0),0);
    $("#integrity-checked").textContent=checked.toLocaleString();
    $("#integrity-corrupted").textContent=corrupted.toLocaleString();
    const latest=DATA.integrity[0];
    $("#integrity-latest").textContent=latest?"Latest job "+(latest.integrity_id||""):"No integrity job reported yet";
    $("#integrity-copy").textContent=latest?((latest.status||"").toUpperCase()+" · "+(latest.node_id||"cluster")+" · updated "+formatTimestamp(latest.updated_at||latest.created_at)):"Run a check to create a durable verification job.";
    $("#integrity-meter").style.width=latest?(["SUCCEEDED","COMPLETED"].includes(String(latest.status||"").toUpperCase())?"100%":"25%"):"0%";
    $("#integrity-body").innerHTML=DATA.integrity.length?DATA.integrity.map(r=>{
      const stateText=String(r.status||"UNKNOWN").toUpperCase();
      const stateClass=["SUCCEEDED","COMPLETED"].includes(stateText)?"success":(["FAILED","ERROR"].includes(stateText)?"corrupted":"running");
      return "<tr><td class='objid'>"+escapeHtml(r.integrity_id||"integrity job")+"</td><td>"+escapeHtml(r.node_id||"cluster")+"</td><td class='hash'>"+escapeHtml(r.version_id||"all versions")+"</td><td>"+escapeHtml(r.checked_count??0)+"</td><td>"+escapeHtml(r.corrupted_count??0)+"</td><td>"+status(stateClass)+"</td><td>"+escapeHtml(formatTimestamp(r.updated_at||r.created_at))+"</td></tr>";
    }).join(""):"<tr><td colspan='7' class='empty-row'>Part B has no integrity jobs.</td></tr>";
  }
  function renderRebalance(){
    $("#capacity-bars").innerHTML=DATA.nodes.length?DATA.nodes.map(n=>{
      return "<div class='capacity-row'><b>"+escapeHtml(n.id)+"</b><div class='capacity "+(n.percent>=80?"high":"")+"' role='progressbar' aria-label='Storage usage for "+escapeHtml(n.id)+"' aria-valuemin='0' aria-valuemax='100' aria-valuenow='"+n.percent+"'><i style='width:"+n.percent+"%'></i></div><span class='capacity-value'>"+n.percent+"%</span></div>";
    }).join(""):"<div class='empty-row'>Waiting for node capacity telemetry.</div>";
    const policy=DATA.policies;
    $("#rebalance-policy-note").textContent=policy?("RF "+policy.replication_factor+" · "+policy.max_concurrent_jobs+" parallel jobs"):"Waiting for policy telemetry";
    $("#rebalance-body").innerHTML=DATA.rebalance.length?DATA.rebalance.map(r=>{
      const stateText=String(r.status||"UNKNOWN").toUpperCase();
      const stateClass=["SUCCEEDED","COMPLETED"].includes(stateText)?"success":(["FAILED","ERROR"].includes(stateText)?"corrupted":"running");
      return "<tr><td class='objid'>"+escapeHtml(r.rebalance_id||"rebalance job")+"</td><td class='hash'>"+escapeHtml(r.version_id||"—")+"</td><td>"+escapeHtml(r.source_node_id||"—")+"</td><td>"+escapeHtml(r.target_node_id||"—")+"</td><td>"+escapeHtml(r.attempts??0)+"</td><td>"+status(stateClass)+"</td><td>"+escapeHtml(formatTimestamp(r.updated_at||r.created_at))+"</td></tr>";
    }).join(""):"<tr><td colspan='7' class='empty-row'>Part B has no rebalance jobs.</td></tr>";
  }
  function renderEvents(){const rows=DATA.events.filter(e=>state.eventFilter==="all"||e.type===state.eventFilter);$("#event-feed").innerHTML=rows.length?rows.map(e=>"<article class='event'><span class='event-icon "+e.type+"'>"+escapeHtml(e.icon)+"</span><div><b>"+escapeHtml(e.title)+"</b><p>"+escapeHtml(e.body)+"</p></div><time>"+escapeHtml(e.relative)+"</time></article>").join(""):"<div class='empty'><b>≋</b><h3>No events in this filter</h3><p>The event feed is clear.</p></div>";}
  function renderTimeline(){$("#timeline").innerHTML=DATA.events.slice(0,4).map(e=>"<div class='event' style='grid-template-columns:8px 1fr auto;background:transparent;border:0;border-bottom:1px solid rgba(167,192,216,.07);border-radius:0;padding:10px 0'><span class='dot "+(e.type==="success"?"good":e.type==="warning"?"warn":"")+"' style='margin-top:4px'></span><div><b>"+escapeHtml(e.title)+"</b><p>"+escapeHtml(e.body)+"</p></div><time>"+escapeHtml(e.relative)+"</time></div>").join("");}
  function renderAll(){renderEnvironment();renderSummary();updateTopology();renderCurrentView();renderTimeline();renderServiceStatus();renderWatchlist();renderPolicy();$("#objects-kpi").textContent=DATA.dashboard.objects.toLocaleString();}
  function renderCurrentView(){
    if(state.view==="nodes")renderNodes();
    else if(state.view==="objects")renderObjects();
    else if(state.view==="repairs")renderRepairs();
    else if(state.view==="integrity")renderIntegrity();
    else if(state.view==="rebalance")renderRebalance();
    else if(state.view==="events")renderEvents();
  }
  function setFilter(group,value){
    state[group]=value;
    const id=group==="nodeFilter"?"node-filters":group==="objectFilter"?"object-filters":"event-filters";
    const box=$("#"+id);
    $("button",box).forEach(b=>{
      const active=b.dataset.filter===value;
      b.classList.toggle("active",active);
      b.setAttribute("aria-pressed",String(active));
    });
    renderCurrentView();
  }
  function jobIdFromResponse(name,result){const keys={repair:["repair_id","job_id","id"],integrity:["integrity_id","job_id","id"],rebalance:["rebalance_id","job_id","id"]};return(keys[name]||[]).map(k=>result?.[k]).find(Boolean)||null;}
  async function pollJob(name,id){
    if(CONFIG.mode!=="api"||!id)return;
    let last="";
    for(let i=0;i<30;i++){
      await new Promise(r=>setTimeout(r,1000));
      try{
        const data=await API.job(name,id),statusText=String(data.status||data.state||"").toUpperCase();
        if(statusText&&statusText!==last){toast(name.charAt(0).toUpperCase()+name.slice(1)+" job",statusText+" · "+id);last=statusText;}
        if(TERMINAL_JOB_STATES.has(statusText)){await API.sync().catch(()=>{});toast(statusText==="FAILED"||statusText==="ERROR"?"Operation failed":"Operation completed",id);return;}
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
      const selected=DATA.objects.find(o=>o.id===state.selectedObject);
      const versionId=selected?.currentVersionId;
      if(!sessionStorage.getItem("vault_admin_key")){
        showView("policies");
        throw new Error("Admin key required. Save the Part B key in Settings first.");
      }
      if((name==="repair"||name==="rebalance")&&!versionId){
        showView("objects");
        throw new Error("Select and inspect an object version before starting this operation.");
      }
      if(name==="integrity")showView("integrity");
      if(name==="repair")showView("repairs");
      if(name==="rebalance")showView("rebalance");
      let payload={};
      if(name==="repair")payload={version_id:versionId,reason:"admin-request"};
      if(name==="integrity"&&versionId)payload={version_id:versionId};
      if(name==="rebalance"){
        const source=DATA.nodes.find(n=>n.percent>=80)||DATA.nodes.find(n=>n.status==="attention");
        const target=DATA.nodes.find(n=>n.id!==source?.id&&n.percent<60);
        if(!source||!target)throw new Error("No safe source and target node pair is available from current Part B telemetry.");
        payload={version_id:versionId,source_node_id:source.id,target_node_id:target.id};
      }
      const result=await API.action(name,payload);
      const id=jobIdFromResponse(name,result);
      toast(name.charAt(0).toUpperCase()+name.slice(1)+" accepted",id?"Tracking "+id:"Part B accepted the request.");
      if(id)pollJob(name,id);
    }catch(error){
      toast("Operation failed",error.message||"Unexpected frontend error");
    }
  }
  async function selectObject(id){
    state.selectedObject=id;state.detailLoading=CONFIG.mode==="api";renderAll();if(CONFIG.mode!=="api")return;
    try{
      const o=DATA.objects.find(item=>item.id===id);if(!o)throw new Error("Object not found in the current catalog.");
      const detail=await API.objectDetails(id),metadata=detail.metadata||{},versions=detail.versions||[],current=versions.find(v=>v.version_id===metadata.current_version_id)||versions[0]||{},replicas=Number(current.healthy_replicas);
      o.objectId=metadata.object_id||o.objectId||o.id;o.currentVersionId=metadata.current_version_id||current.version_id||o.currentVersionId;o.currentVersion=current;o.version=current.version_number?("v"+current.version_number):(o.currentVersionId||"—");o.size=Number.isFinite(Number(current.size_bytes))?formatBytes(Number(current.size_bytes)):"—";o.checksum=current.checksum||"—";o.replicas=Number.isFinite(replicas)?replicas+"/"+(DATA.policies?.replication_factor||3):"—";o.status=String(current.state||metadata.state||"ACTIVE").toUpperCase()==="CORRUPTED"?"corrupted":(Number.isFinite(replicas)&&DATA.policies&&replicas<DATA.policies.replication_factor?"degraded":"healthy");o.type=metadata.content_type||metadata.type||o.type||"object";o.created=formatTimestamp(metadata.created_at||o.created);o.updated=formatTimestamp(metadata.updated_at||o.updated);o.replicaRows=(current.replicas||[]).map(r=>[r.node_id,r.status,r.size_bytes?formatBytes(Number(r.size_bytes)):"—"]);
    }catch(error){toast("Object details unavailable",error.message);}finally{state.detailLoading=false;renderAll();}
  }
  let activeOverlay=null;
  let restoreFocus=null;
  const FOCUSABLE_SELECTOR='a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])';
  function focusables(container){return $(FOCUSABLE_SELECTOR,container).filter(el=>!el.hidden&&el.offsetParent!==null);}
  function openOverlay(id,initialSelector){
    const overlay=$("#"+id);
    if(!overlay)return;
    const shell=$(".app-shell");
    restoreFocus=document.activeElement instanceof HTMLElement?document.activeElement:null;
    activeOverlay=overlay;
    overlay.classList.add("open");
    overlay.setAttribute("aria-hidden","false");
    if(shell)shell.inert=true;
    requestAnimationFrame(()=>requestAnimationFrame(()=>{
      const target=initialSelector?$(initialSelector,overlay):null;
      (target||focusables(overlay)[0])?.focus();
    }));
  }
  function closeOverlay(id){
    const overlay=$("#"+id);
    if(!overlay)return;
    overlay.classList.remove("open");
    overlay.setAttribute("aria-hidden","true");
    const shell=$(".app-shell");
    if(activeOverlay===overlay){
      activeOverlay=null;
      if(shell)shell.inert=false;
      const target=restoreFocus;
      restoreFocus=null;
      if(target&&document.contains(target))requestAnimationFrame(()=>target.focus());
    }
  }
  function trapOverlayFocus(event){
    if(!activeOverlay||event.key!=="Tab")return;
    const items=focusables(activeOverlay);
    if(!items.length){event.preventDefault();return;}
    const first=items[0],last=items[items.length-1];
    if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}
    else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}
  }
  function openModal(){openOverlay("modal","#file-input");}
  function closeModal(){closeOverlay("modal");$("#progress-wrap").hidden=true;$("#progress-bar").style.width="0%";$("#progress-value").textContent="0%";$("#file-name").textContent="No file selected";$("#upload-btn").disabled=true;$("#file-input").value="";}
  async function uploadFile(){
    const file=$("#file-input").files[0];
    if(!file)return;
    $("#upload-btn").disabled=true;
    $("#progress-wrap").hidden=false;
    $("#progress-value").textContent="25%";
    $("#progress-bar").style.width="25%";
    $("#progress-bar").setAttribute("aria-valuenow","25");
    $("#progress-text").textContent="Uploading through Part B…";
    try{
      await API.upload(file);
      $("#progress-value").textContent="100%";
      $("#progress-bar").style.width="100%";
      $("#progress-bar").setAttribute("aria-valuenow","100");
      $("#progress-text").textContent="Accepted · syncing catalog";
      await API.sync();
      setTimeout(()=>{
        closeModal();
        showView("objects");
        toast("Upload accepted",file.name+" is now in the live object catalog.");
      },350);
    }catch(error){
      closeModal();
      toast("Upload failed",error.message);
    }
  }
  const commands=[
    {label:"Go to Overview",keys:"1",run:()=>showView("overview")},{label:"Go to Nodes",keys:"2",run:()=>showView("nodes")},{label:"Go to Objects",keys:"3",run:()=>showView("objects")},{label:"Go to Repairs",keys:"4",run:()=>showView("repairs")},{label:"Go to Integrity",keys:"5",run:()=>showView("integrity")},{label:"Go to Rebalance",keys:"6",run:()=>showView("rebalance")},{label:"Open Activity",keys:"7",run:()=>showView("events")},{label:"Open Settings",keys:"8",run:()=>showView("policies")},{label:"Open User Guide",keys:"9",run:()=>showView("guide")},{label:"Open About",keys:"0",run:()=>showView("about")},{label:"Upload object",keys:"U",run:openModal},{label:"Run integrity scan",keys:"I",run:()=>runAction("integrity")},{label:"Refresh telemetry",keys:"R",run:async()=>{try{await API.sync();renderAll();toast("Refreshed","Live Part B telemetry synchronized.");}catch(e){toast("Refresh failed",e.message);}}}
  ];
  function renderCommands(){const q=($("#command-input")?.value||"").toLowerCase(),list=commands.filter(c=>c.label.toLowerCase().includes(q));state.commandIndex=Math.min(state.commandIndex,Math.max(0,list.length-1));$("#command-list").innerHTML=list.map((c,i)=>"<button class='command-item "+(i===state.commandIndex?"active":"")+"' data-command-label='"+escapeHtml(c.label)+"'><span>"+escapeHtml(c.label)+"</span><kbd>"+escapeHtml(c.keys)+"</kbd></button>").join("")||"<div class='empty command-empty'><b>⌕</b><h3>No command found</h3><p>Try “upload”, “repair”, or a view name.</p></div>";}
  function openCommand(){state.commandIndex=0;renderCommands();openOverlay("command-modal","#command-input");}
  function closeCommand(){closeOverlay("command-modal");}
  function runCommand(label){const c=commands.find(item=>item.label===label);if(c){closeCommand();c.run();}}
  document.addEventListener("click",async e=>{
    const nav=e.target.closest("[data-view]");if(nav){showView(nav.dataset.view);return;}
    const action=e.target.closest("[data-action]");
    if(action){
      const a=action.dataset.action;
      if(a==="upload")openModal();else if(a==="close-modal")closeModal();else if(a==="upload-file")await uploadFile();else if(a==="integrity")await runAction("integrity");else if(a==="repair")await runAction("repair");else if(a==="rebalance")await runAction("rebalance");else if(a==="refresh"){try{await API.sync();renderAll();toast("Refreshed","Live Part B telemetry synchronized.");}catch(error){toast("Refresh failed",error.message);}}else if(a==="save-admin-key"){const input=$("#admin-key-input"),key=input?.value.trim();if(!key){toast("Admin key not saved","Enter the VAULT_ADMIN_API_KEY supplied for this environment.");return;}sessionStorage.setItem("vault_admin_key",key);input.value="";toast("Admin access ready","Key stored only in this browser session.");try{await API.sync();}catch(error){toast("Telemetry refresh failed",error.message);}}
    }
    const obj=e.target.closest("[data-object]");if(obj){await selectObject(obj.dataset.object);return;}
    const node=e.target.closest("[data-node]");
    if(node){return;}
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
  $("#command-modal").addEventListener("click",e=>{if(e.target.id==="command-modal")closeCommand();});
  document.addEventListener("keydown",e=>{
    if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==="k"){e.preventDefault();openCommand();return;}
    if(e.key==="Tab"&&activeOverlay){trapOverlayFocus(e);return;}
    if(e.key==="Escape"){if(activeOverlay){e.preventDefault();closeOverlay(activeOverlay.id);}else{closeModal();closeCommand();}}
    const tag=(e.target.tagName||"").toLowerCase();
    if(!["input","textarea","select"].includes(tag)){
      const key=e.key.toLowerCase();
      const map={u:openModal,r:()=>API.sync().then(()=>{renderAll();toast("Refreshed","Live Part B telemetry synchronized.");}).catch(err=>toast("Refresh failed",err.message)),i:()=>runAction("integrity")};
      if(map[key]&&!((e.ctrlKey||e.metaKey)))map[key]();
    }
  });
  window.addEventListener("error",event=>{
    if(!event?.error)return;
    toast("Unexpected frontend error","The interface recovered; refresh to retry the last action.");
  });
  window.addEventListener("unhandledrejection",event=>{
    const reason=event?.reason;
    if(reason)toast("Unexpected async error",reason.message||String(reason));
  });
  renderAll();
  const hash=location.hash.slice(1);showView(ROUTES.includes(hash)?hash:"overview");
  API.sync().then(()=>renderAll()).catch(error=>{if(CONFIG.mode==="api")toast("API unavailable",error.message);renderAll();});
  window.VaultFrontend={API,DATA,state,showView};
})();