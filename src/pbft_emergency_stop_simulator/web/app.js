const state={scenarios:[],suite:null,snapshot:null,logs:[],filter:""};
const $=id=>document.getElementById(id);

async function api(path,options={}){
  const response=await fetch(path,{headers:{"Content-Type":"application/json"},...options});
  if(!response.ok){const body=await response.json().catch(()=>({detail:response.statusText}));throw new Error(body.detail||response.statusText)}
  return response.json();
}
function toast(message,error=false){const el=document.createElement("div");el.className=`toast ${error?"error":""}`;el.textContent=message;document.body.appendChild(el);setTimeout(()=>el.remove(),3500)}

async function loadScenarios(){const data=await api("/api/scenarios");state.scenarios=data.scenarios;$("catalog-count").textContent=String(state.scenarios.length);renderScenarioList()}
function filteredScenarios(){const q=state.filter.trim().toLowerCase();if(!q)return state.scenarios;return state.scenarios.filter(s=>`${s.name} ${s.category} ${s.description} ${(s.tags||[]).join(" ")}`.toLowerCase().includes(q))}
function renderScenarioList(){const selected=new Set(selectedScenarios());const groups={};filteredScenarios().forEach(s=>(groups[s.category]??=[]).push(s));$("scenario-list").innerHTML=Object.entries(groups).map(([group,items])=>`<div class="scenario-group"><h4><span>${escapeHtml(group)}</span><em>${items.length}</em></h4>${items.map(s=>`<label class="scenario-item"><input type="checkbox" value="${escapeHtml(s.id)}" ${selected.has(s.id)?"checked":""}><span>${escapeHtml(s.name)}<small>${escapeHtml(s.description)}</small></span></label>`).join("")}</div>`).join("")||`<div class="empty">No scenarios match the search.</div>`;document.querySelectorAll("#scenario-list input").forEach(el=>el.addEventListener("change",updateSelectedCount));updateSelectedCount()}
function updateSelectedCount(){$("selected-count").textContent=`${selectedScenarios().length} selected`}
function selectedScenarios(){return [...document.querySelectorAll("#scenario-list input:checked")].map(el=>el.value)}
$("scenario-search").addEventListener("input",event=>{state.filter=event.target.value;renderScenarioList()});
$("select-visible").onclick=()=>{document.querySelectorAll("#scenario-list input").forEach(el=>el.checked=true);updateSelectedCount()};
$("clear-selection").onclick=()=>{document.querySelectorAll("#scenario-list input").forEach(el=>el.checked=false);updateSelectedCount()};

async function run(ids){if(!ids.length){toast("Select at least one scenario.",true);return}try{const data=await api("/api/suites",{method:"POST",body:JSON.stringify({scenario_ids:ids,repeat:Number($("repeat").value),stop_on_failure:$("stop-on-failure").checked})});toast(`Suite ${data.suite_id} started.`)}catch(err){toast(err.message,true)}}
$("run-selected").onclick=()=>run(selectedScenarios());
$("run-all").onclick=()=>run(state.scenarios.map(item=>item.id));
$("cancel").onclick=async()=>{try{await api("/api/cancel",{method:"POST"});toast("Cancellation requested.")}catch(err){toast(err.message,true)}};
$("clear-log").onclick=()=>{state.logs=[];renderLog()};

function connect(){const protocol=location.protocol==="https:"?"wss":"ws";const socket=new WebSocket(`${protocol}://${location.host}/ws`);socket.onopen=()=>{$("connection").textContent="Connected — live ROS 2 test data"};socket.onclose=()=>{$("connection").textContent="Disconnected — reconnecting…";setTimeout(connect,1000)};socket.onmessage=event=>{const message=JSON.parse(event.data);if(message.type==="suite"){state.suite=message.suite;renderSuite()}else if(message.type==="snapshot"){state.snapshot=message.snapshot;renderSnapshot()}else if(message.type==="log"){state.logs.push(message.item);state.logs=state.logs.slice(-300);renderLog()}else if(message.type==="initial"&&message.suites?.length){state.suite=message.suites[0];state.snapshot=state.suite.active_snapshot;state.logs=state.suite.live_log||[];renderSuite();renderSnapshot();renderLog()}}}

function renderSuite(){const suite=state.suite;if(!suite)return;$("suite-status").textContent=suite.status;$("active-scenario").textContent=scenarioName(suite.active_scenario);$("result-count").textContent=`${suite.results.length} tests`;const link=$("report-link");link.href=`/api/suites/${suite.suite_id}/report`;link.classList.toggle("disabled",!suite.results.length);renderResults(suite.results);if(suite.active_snapshot){state.snapshot=suite.active_snapshot;renderSnapshot()}state.logs=suite.live_log||state.logs;renderLog()}
function scenarioName(id){return state.scenarios.find(item=>item.id===id)?.name||id||"—"}

function renderSnapshot(){const s=state.snapshot;if(!s)return;$("current-view").textContent=s.current_view??0;$("primary-id").textContent=`Replica ${s.primary_id??0}`;$("safety-state").textContent=s.safety?.state||"—";$("safety-output").textContent=s.safety?.emergency_stop===true?"STOP ACTIVE":s.safety?.emergency_stop===false?"RELEASED":"—";$("elapsed").textContent=`${Number(s.elapsed_sec||0).toFixed(1)} s`;renderReplicas(s);renderPipeline(s);renderTimeline(s.timeline||[]);renderAssertions(s.assertions||[])}

function renderReplicas(s){$("replicas").innerHTML=[0,1,2,3].map(id=>{const r=s.replicas?.[String(id)]||{};const primary=s.primary_id===id;return `<div class="replica ${primary?"primary":""} ${r.is_byzantine?"byzantine":""}"><div class="replica-head"><strong>Replica ${id}</strong><span class="pill ${r.is_byzantine?"purple":primary?"green":""}">${r.is_byzantine?"FAULT INJECTED":primary?"PRIMARY":"BACKUP"}</span></div><div class="replica-grid"><div><span>Phase</span><b>${escapeHtml(r.phase||"IDLE")}</b></div><div><span>View</span><b>${r.view??0}</b></div><div><span>Prepare</span><b>${r.prepare_count??0}</b></div><div><span>Commit</span><b>${r.commit_count??0}</b></div><div><span>Prepared</span><b>${r.prepared?"YES":"NO"}</b></div><div><span>Committed</span><b>${r.committed?"YES":"NO"}</b></div></div>${r.detail?`<p class="replica-detail">${escapeHtml(r.detail)}</p>`:""}</div>`}).join("")}

function renderPipeline(s){const c=s.counts||{};const hasDecision=!!s.decision;const safety=s.safety?.state;const stages=[
 ["REQUEST",c.request>0,c.request?`${c.request} observed`:"waiting"],
 ["PRE-PREPARE",c.pre_prepare>0,c.pre_prepare?`${c.pre_prepare} published`:"waiting"],
 ["PREPARE",c.prepare>0,c.prepare?`${c.prepare} messages`:"waiting"],
 ["COMMIT",c.commit>0,c.commit?`${c.commit} messages`:"waiting"],
 ["DECISION",hasDecision,hasDecision?`view ${s.decision.view}`:"waiting"],
 ["VIEW-CHANGE",(c.view_change||0)>0,`${c.view_change||0} votes`],
 ["NEW-VIEW",(c.new_view||0)>0,`${c.new_view||0} published`],
 ["RECOVERY",(c.recovery_pre_prepare||0)>0,`${c.recovery_pre_prepare||0} PRE-PREPARE`],
 ["SAFETY",!!safety,safety||"waiting"],
 ];$("pipeline").innerHTML=stages.map(([name,done,detail])=>`<div class="stage ${done?"done":""} ${name==="VIEW-CHANGE"&&done?"fault":""} ${safety==="FAIL_SAFE_STOP"&&name==="SAFETY"?"blocked":""}"><b>${name}</b><small>${escapeHtml(String(detail))}</small></div>`).join("")}

function renderTimeline(items){$("timeline").innerHTML=items.slice(-80).reverse().map(item=>`<div class="timeline-item"><time>${Number(item.elapsed_sec).toFixed(2)}s</time><span class="timeline-type">${escapeHtml(item.type)}</span><code>${escapeHtml(compact(item.payload))}</code></div>`).join("")||`<div class="empty">Waiting for ROS 2 events.</div>`}
function compact(payload){const text=JSON.stringify(payload);return text.length>160?text.slice(0,157)+"…":text}

function renderAssertions(items){const container=$("assertions");if(!items.length){container.className="assertions empty";container.textContent="Assertions are evaluated continuously and finalized at the terminal condition.";$("assertion-summary").textContent="Waiting";return}container.className="assertions";const passed=items.filter(a=>a.passed).length;$("assertion-summary").textContent=`${passed}/${items.length} passed`;container.innerHTML=items.map(a=>`<div class="assertion ${a.passed?"pass":"fail"}"><span class="icon">${a.passed?"✓":"✕"}</span><span>${escapeHtml(a.label)}</span><code>${escapeHtml(formatValue(a.actual))}</code></div>`).join("")}
function formatValue(value){if(value===null||value===undefined)return "—";if(typeof value==="object")return JSON.stringify(value);return String(value)}

function renderLog(){const pre=$("live-log");pre.textContent=state.logs.map(item=>`[${item.time}] [${item.source}] ${item.line}`).join("\n");pre.scrollTop=pre.scrollHeight}
function renderResults(results){const el=$("results");if(!results.length){el.className="results empty";el.textContent="No suite has been executed yet.";return}el.className="results";el.innerHTML=results.map(r=>`<div class="result-row"><span class="status ${String(r.status).toLowerCase()}">${escapeHtml(r.status)}</span><strong>${escapeHtml(r.scenario_name)}</strong><span>${Number(r.duration_sec||0).toFixed(2)} s</span><span>Domain ${escapeHtml(String(r.ros_domain_id||""))}</span></div>`).join("")}
function escapeHtml(value){return String(value).replace(/[&<>'"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[c]))}

loadScenarios().catch(err=>toast(err.message,true));connect();
