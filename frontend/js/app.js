(() => {
  "use strict";

  const CONFIG = window.VAULT_CONFIG || { mode: "mock", baseUrl: "/api/v1" };
  const DATA = {
    dashboard:{objects:12842},
    nodes:[
      {id:"node-01",status:"healthy",capacity:"500 GB",used:"205 GB",percent:41,objects:4287,heartbeat:"2.0s",lifecycle:"HEALTHY"},
      {id:"node-02",status:"healthy",capacity:"500 GB",used:"340 GB",percent:68,objects:3914,heartbeat:"2.1s",lifecycle:"HEALTHY"},
      {id:"node-03",status:"healthy",capacity:"500 GB",used:"285 GB",percent:57,objects:3669,heartbeat:"2.0s",lifecycle:"HEALTHY"},
      {id:"node-04",status:"attention",capacity:"500 GB",used:"405 GB",percent:81,objects:4210,heartbeat:"2.4s",lifecycle:"HEALTHY"}
    ],
    objects:[
      {id:"obj_8fd21a",size:"24.8 MB",version:"v7",replicas:"3/3",checksum:"6d1f…9a20",status:"healthy",updated:"2m ago",type:"application/zip",created:"Sep 26, 2026 · 03:44:12",replicaRows:[["node-01","HEALTHY","24.8 MB"],["node-02","HEALTHY","24.8 MB"],["node-03","HEALTHY","24.8 MB"]]},
      {id:"obj_4c11b7",size:"1.2 GB",version:"v3",replicas:"3/3",checksum:"21a9…b641",status:"healthy",updated:"14m ago",type:"application/octet-stream",created:"Sep 26, 2026 · 03:31:07",replicaRows:[["node-02","HEALTHY","1.2 GB"],["node-03","HEALTHY","1.2 GB"],["node-04","HEALTHY","1.2 GB"]]},
      {id:"obj_3da909",size:"84.2 MB",version:"v2",replicas:"2/3",checksum:"a57b…c1e9",status:"degraded",updated:"28m ago",type:"application/json",created:"Sep 26, 2026 · 03:14:30",replicaRows:[["node-01","HEALTHY","84.2 MB"],["node-02","HEALTHY","84.2 MB"],["node-04","UNAVAILABLE","—"]]},
      {id:"obj_711ce2",size:"412 MB",version:"v9",replicas:"3/3",checksum:"b44e…8c71",status:"healthy",updated:"1h ago",type:"video/mp4",created:"Sep 26, 2026 · 02:52:13",replicaRows:[["node-01","HEALTHY","412 MB"],["node-03","HEALTHY","412 MB"],["node-04","HEALTHY","412 MB"]]},
      {id:"obj_c92e10",size:"18.6 MB",version:"v4",replicas:"3/3",checksum:"1f91…73ae",status:"healthy",updated:"2h ago",type:"application/pdf",created:"Sep 26, 2026 · 02:04:51",replicaRows:[["node-01","HEALTHY","18.6 MB"],["node-02","HEALTHY","18.6 MB"],["node-03","HEALTHY","18.6 MB"]]},
      {id:"obj_aa9120",size:"6.8 GB",version:"v12",replicas:"2/3",checksum:"e21c…10bd",status:"corrupted",updated:"3h ago",type:"application/x-tar",created:"Sep 26, 2026 · 01:26:09",replicaRows:[["node-01","HEALTHY","6.8 GB"],["node-02","CORRUPTED","6.8 GB"],["node-03","HEALTHY","6.8 GB"]]}
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
    ]
  };

  const state = { view:"overview", nodeFilter:"all", objectFilter:"all", eventFilter:"all", selectedObject:null };
  const $ = (s,r=document) => r.querySelector(s);
  const $$ = (s,r=document) => Array.from(r.querySelectorAll(s));
  const escapeHtml = v => String(v).replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" })[c]);

  const API = {
    mode:CONFIG.mode, baseUrl:CONFIG.baseUrl,
    async request(path, options = {}) {
      const headers = new Headers(options.headers || {});
      const requestId = options.requestId || "req_" + Math.random().toString(16).slice(2,10);
      headers.set("Accept","application/json"); headers.set("X-Request-ID",requestId);
      const base = new URL(this.baseUrl, location.origin).toString().replace(/\/$/,"");
      const response = await fetch(new URL(path,base),{...options,headers,redirect:"error"});
      const body = await response.json().catch(() => ({}));
      if(!response.ok){const e=body.error || {}; const err=new Error(e.message || "API request failed"); err.code=e.code || "INTERNAL_ERROR"; err.requestId=e.request_id || requestId; throw err;}
      return body;
    },
    async action(name,payload) {
      if(this.mode === "mock") return {job_id:name + "-" + Date.now(),status:"RUNNING",payload:payload || null};
      const paths={repair:"/admin/repair",integrity:"/admin/integrity/check",rebalance:"/admin/rebalance"};
      return this.request(paths[name],{method:"POST",body:payload ? JSON.stringify(payload) : undefined,headers:payload ? {"Content-Type":"application/json"} : undefined});
    },
    async upload(file) {
      if(this.mode === "mock") return {object_id:"obj_"+Math.random().toString(16).slice(2,8),version_id:"v1"};
      const form=new FormData(); form.append("file",file); return this.request("/objects",{method:"POST",body:form});
    }
  };

  function toast(title,message){const t=document.createElement("div");t.className="toast";t.innerHTML="<b>"+escapeHtml(title)+"</b><small>"+escapeHtml(message)+"</small>";$("#toasts").appendChild(t);setTimeout(()=>t.remove(),3500)}
  function showView(view){state.view=view;$$(".nav-item").forEach(b=>b.classList.toggle("active",b.dataset.view===view));$$(".view").forEach(v=>v.classList.toggle("visible",v.id==="view-"+view));$("#view-title").textContent=view.charAt(0).toUpperCase()+view.slice(1);history.replaceState(null,"","#"+view);window.scrollTo({top:0,behavior:"smooth"});renderAll()}
  function status(s){const map={healthy:["green","HEALTHY"],attention:["amber","ATTENTION"],degraded:["amber","DEGRADED"],corrupted:["red","CORRUPTED"],running:["amber","RUNNING"],success:["green","SUCCEEDED"]};const m=map[s]||["green",String(s).toUpperCase()];return "<span class='status "+m[0]+"'><i></i>"+m[1]+"</span>"}
  function renderNodes(){const q=($("#node-search")?.value||"").toLowerCase();const rows=DATA.nodes.filter(n=>(state.nodeFilter==="all"||(state.nodeFilter==="healthy"&&n.status==="healthy")||(state.nodeFilter==="attention"&&n.status==="attention"))&&(!q||(n.id+" "+n.lifecycle).toLowerCase().includes(q)));$("#nodes-body").innerHTML=rows.map(n=>"<tr><td class='objid'>"+n.id+"</td><td>"+status(n.status)+"</td><td>"+n.capacity+"</td><td><b>"+n.used+"</b> <small>"+n.percent+"%</small></td><td>"+n.objects.toLocaleString()+"</td><td>"+n.heartbeat+"</td><td><span class='chip "+(n.lifecycle==="HEALTHY"?"green":"amber")+"'>"+n.lifecycle+"</span></td><td><button class='table-action' data-node='"+n.id+"' data-node-action='"+(n.lifecycle==="DRAINING"?"resume":"drain")+"'>"+(n.lifecycle==="DRAINING"?"Resume":"Drain")+"</button></td></tr>").join("")}
  function renderObjects(){const q=($("#object-search")?.value||"").toLowerCase();const rows=DATA.objects.filter(o=>(state.objectFilter==="all"||o.status===state.objectFilter)&&(!q||(o.id+" "+o.checksum+" "+o.status).toLowerCase().includes(q)));$("#objects-body").innerHTML=rows.map(o=>"<tr><td class='objid'>"+o.id+"</td><td>"+o.size+"</td><td>"+o.version+"</td><td>"+o.replicas+"</td><td class='hash'>"+escapeHtml(o.checksum)+"</td><td>"+status(o.status)+"</td><td>"+o.updated+"</td><td><button class='table-action' data-object='"+o.id+"'>Inspect</button></td></tr>").join("");const selected=DATA.objects.find(o=>o.id===state.selectedObject);if(selected)renderDetail(selected);else $("#object-detail").innerHTML="<div class='empty'><b>▦</b><h3>Select an object</h3><p>Replica placement and version details will appear here.</p></div>"}
  function renderDetail(o){$("#object-detail").innerHTML="<div class='detail-grid'><div><div class='detail-hero'><p class='eyebrow'>OBJECT DETAIL</p><h2>"+o.id+"</h2><div class='detail-meta'><span>"+o.size+"</span><span>Version "+o.version+"</span><span>"+status(o.status)+"</span><span>SHA-256 "+escapeHtml(o.checksum)+"</span></div></div><div style='padding-top:12px'><p class='eyebrow'>REPLICA SPREAD</p><div class='replicas'>"+o.replicaRows.map(r=>"<div class='replica'><b>"+r[0]+"</b><small>"+r[2]+"</small>"+status(r[1]==="HEALTHY"?"healthy":r[1]==="CORRUPTED"?"corrupted":"attention")+"</div>").join("")+"</div></div></div><div><p class='eyebrow'>METADATA</p><div class='service-list'><div><span>Content type</span><b>"+o.type+"</b></div><div><span>Created</span><b>"+o.created+"</b></div><div><span>Placement</span><b>RF 3 · W2 · R1</b></div><div><span>Read path</span><b>Healthy replica</b></div></div></div></div>"}
  function renderRepairs(){$("#repair-active").textContent=DATA.repairs.filter(r=>r.status==="RUNNING").length;$("#repair-grid").innerHTML=DATA.repairs.map(r=>"<article class='repair-card'><div class='repair-top'><div><p class='eyebrow'>"+r.id+"</p><h3>"+r.object+"</h3></div>"+status(r.status==="RUNNING"?"running":"success")+"</div><p>"+r.note+"</p><div class='route'><b>"+r.source+"</b><span>→ verified stream →</span><b>"+r.target+"</b></div><div class='progress-label'><span>Recovery progress</span><b>"+r.progress+"%</b></div><div class='meter'><i style='width:"+r.progress+"%'></i></div><footer><span>"+(r.status==="RUNNING"?"ETA "+r.eta:"Durability floor restored")+"</span><span>checksum verified</span></footer></article>").join("")}
  function renderIntegrity(){$("#integrity-body").innerHTML=DATA.integrity.map(r=>"<tr><td class='objid'>"+r.object+"</td><td>"+r.replica+"</td><td class='hash'>"+r.expected+"</td><td class='hash'>"+r.observed+"</td><td>"+(r.result==="VALID"?status("healthy"):status("corrupted"))+"</td><td>"+r.checked+"</td></tr>").join("")}
  function renderRebalance(){$("#capacity-bars").innerHTML=DATA.nodes.map(n=>"<div class='capacity-row'><b>"+n.id+"</b><div class='capacity "+(n.percent>=80?"high":"")+"'><i style='width:"+n.percent+"%'></i></div><span class='capacity-value'>"+n.percent+"%</span></div>").join("");$("#rebalance-body").innerHTML=DATA.rebalance.map(r=>"<tr><td class='objid'>"+r.job+"</td><td>"+r.object+"</td><td>"+r.source+"</td><td>"+r.target+"</td><td style='min-width:120px'><div class='meter'><i style='width:"+r.progress+"%'></i></div><small>"+r.progress+"%</small></td><td>"+status(r.status==="RUNNING"?"running":"success")+"</td><td>"+r.updated+"</td></tr>").join("")}
  function renderEvents(){const rows=DATA.events.filter(e=>state.eventFilter==="all"||e.type===state.eventFilter);$("#event-feed").innerHTML=rows.map(e=>"<article class='event'><span class='event-icon "+e.type+"'>"+e.icon+"</span><div><b>"+escapeHtml(e.title)+"</b><p>"+escapeHtml(e.body)+"</p></div><time>"+e.relative+"</time></article>").join("")}
  function renderTimeline(){$("#timeline").innerHTML=DATA.events.slice(0,4).map(e=>"<div class='event' style='grid-template-columns:8px 1fr auto;background:transparent;border:0;border-bottom:1px solid rgba(167,192,216,.07);border-radius:0;padding:10px 0'><span class='dot "+(e.type==="success"?"good":e.type==="warning"?"warn":"")+"' style='margin-top:4px'></span><div><b>"+escapeHtml(e.title)+"</b><p>"+escapeHtml(e.body)+"</p></div><time>"+e.relative+"</time></div>").join("")}
  function renderAll(){if(state.view==="nodes")renderNodes();if(state.view==="objects")renderObjects();if(state.view==="repairs")renderRepairs();if(state.view==="integrity")renderIntegrity();if(state.view==="rebalance")renderRebalance();if(state.view==="events")renderEvents();renderTimeline();$("#objects-kpi").textContent=DATA.dashboard.objects.toLocaleString();$("#api-badge").textContent=CONFIG.mode==="mock"?"MOCK MODE":"LIVE API"}
  function setFilter(group,value){state[group]=value;const box=$("#"+(group==="nodeFilter"?"node-filters":group==="objectFilter"?"object-filters":"event-filters"));$$("button",box).forEach(b=>b.classList.toggle("active",b.dataset.filter===value));renderAll()}

  async function runAction(name){
    try {
      await API.action(name);
      const title = name === "integrity" ? "Integrity scan started" : name === "repair" ? "Repair pass started" : "Rebalance started";
      toast(title, "The request is running through the demo control-plane boundary.");
      setTimeout(() => {
        if (name === "integrity") {
          DATA.events.unshift({type:"success",icon:"✓",title:"Integrity scan completed",body:"Demo scan verified the protected replica set.",relative:"just now"});
        } else if (name === "repair") {
          DATA.repairs.unshift({id:"repair-"+Date.now().toString().slice(-3),object:"obj_demo",source:"node-01",target:"node-03",progress:100,status:"COMPLETED",eta:"—",note:"Demo repair completed after independent verification"});
          DATA.events.unshift({type:"success",icon:"↻",title:"Replica repaired",body:"Durability floor restored by an independently verified copy.",relative:"just now"});
        } else if (name === "rebalance") {
          const node = DATA.nodes.find(item => item.id === "node-04");
          if (node) {
            node.percent = 74;
            node.used = "370 GB";
          }
          DATA.events.unshift({type:"success",icon:"⇄",title:"Rebalance completed",body:"Verified data moved away from node-04.",relative:"just now"});
        }
        renderAll();
        toast("Operation completed", "The cluster view has been updated.");
      }, 900);
    } catch (error) {
      toast("Operation failed", error.message);
    }
  }

  function openModal(){const m=$("#modal");m.classList.add("open");m.setAttribute("aria-hidden","false");setTimeout(()=>$("#file-input").focus(),20)}
  function closeModal(){const m=$("#modal");m.classList.remove("open");m.setAttribute("aria-hidden","true");$("#progress-wrap").hidden=true;$("#progress-bar").style.width="0%";$("#progress-value").textContent="0%";$("#file-name").textContent="No file selected";$("#upload-btn").disabled=true;$("#file-input").value=""}
  async function uploadFile(){const file=$("#file-input").files[0];if(!file)return;$("#upload-btn").disabled=true;$("#progress-wrap").hidden=false;let p=0;const timer=setInterval(async()=>{p+=Math.max(7,Math.floor(Math.random()*12));if(p>=100){p=100;clearInterval(timer);$("#progress-text").textContent="Committed · replicas verified";$("#progress-value").textContent="100%";$("#progress-bar").style.width="100%";const r=await API.upload(file);const mb=(file.size/1048576).toFixed(1);DATA.dashboard.objects+=1;DATA.objects.unshift({id:r.object_id,size:mb+" MB",version:r.version_id,replicas:"3/3",checksum:"pending…",status:"healthy",updated:"just now",type:file.type||"application/octet-stream",created:new Date().toLocaleString(),replicaRows:[["node-01","HEALTHY",mb+" MB"],["node-02","HEALTHY",mb+" MB"],["node-03","HEALTHY",mb+" MB"]]});DATA.events.unshift({type:"success",icon:"↑",title:"Object committed",body:file.name+" uploaded in demo mode with RF 3.",relative:"just now"});setTimeout(()=>{closeModal();showView("objects");toast("Upload committed",file.name+" is protected with three replicas.")},600)}else{$("#progress-value").textContent=p+"%";$("#progress-bar").style.width=p+"%";$("#progress-text").textContent=p<70?"Streaming object":"Verifying replicas"}},120)}

  document.addEventListener("click",async e=>{
    const nav=e.target.closest("[data-view]");if(nav){showView(nav.dataset.view);return}
    const action=e.target.closest("[data-action]");if(action){const a=action.dataset.action;if(a==="upload")openModal();if(a==="close-modal")closeModal();if(a==="upload-file")await uploadFile();if(a==="integrity"){showView("integrity");runAction("integrity")}if(a==="repair"){showView("repairs");runAction("repair")}if(a==="rebalance"){showView("rebalance");runAction("rebalance")}if(a==="refresh"){renderAll();toast("Telemetry refreshed","All dashboard views are synchronized.")}if(a==="clear-events"){DATA.events=[];renderAll();toast("Demo alerts cleared","Local event history was cleared.")}}
    const obj=e.target.closest("[data-object]");if(obj){state.selectedObject=obj.dataset.object;showView("objects")}
    const node=e.target.closest("[data-node]");if(node){const n=DATA.nodes.find(x=>x.id===node.dataset.node);if(n){n.lifecycle=node.dataset.nodeAction==="drain"?"DRAINING":"HEALTHY";n.status=n.lifecycle==="DRAINING"?"attention":"healthy";}renderNodes();toast(n.id,n.lifecycle==="DRAINING"?"Node is now draining.":"Node resumed and accepts writes.")}
  });
  document.addEventListener("input",e=>{if(e.target.id==="node-search")renderNodes();if(e.target.id==="object-search")renderObjects()});
  $("#node-filters").addEventListener("click",e=>{const b=e.target.closest("[data-filter]");if(b)setFilter("nodeFilter",b.dataset.filter)});
  $("#object-filters").addEventListener("click",e=>{const b=e.target.closest("[data-filter]");if(b)setFilter("objectFilter",b.dataset.filter)});
  $("#event-filters").addEventListener("click",e=>{const b=e.target.closest("[data-filter]");if(b)setFilter("eventFilter",b.dataset.filter)});
  $("#refresh").addEventListener("click",()=>{renderAll();toast("Refreshed","Vault telemetry is current.")});
  const drop = document.querySelector(".drop");
  $("#file-input").addEventListener("change",()=>{const f=$("#file-input").files[0];$("#file-name").textContent=f?f.name+" · "+(f.size/1048576).toFixed(2)+" MB":"No file selected";$("#upload-btn").disabled=!f});
  ["dragenter","dragover"].forEach(type=>drop.addEventListener(type,e=>{e.preventDefault();drop.style.borderColor="rgba(91,188,255,.65)"}));
  ["dragleave","drop"].forEach(type=>drop.addEventListener(type,e=>{e.preventDefault();drop.style.borderColor=""}));
  drop.addEventListener("drop",e=>{const file=e.dataTransfer.files[0];if(!file)return;const input=$("#file-input");const transfer=new DataTransfer();transfer.items.add(file);input.files=transfer.files;$("#file-name").textContent=file.name+" · "+(file.size/1048576).toFixed(2)+" MB";$("#upload-btn").disabled=false});
  $("#modal").addEventListener("click",e=>{if(e.target.id==="modal")closeModal()});
  document.addEventListener("keydown",e=>{if(e.key==="Escape")closeModal()});

  renderAll();
  const hash=location.hash.slice(1);showView(["overview","nodes","objects","repairs","integrity","rebalance","events","policies"].includes(hash)?hash:"overview");
  window.VaultFrontend={API,DATA,state,showView};
})();