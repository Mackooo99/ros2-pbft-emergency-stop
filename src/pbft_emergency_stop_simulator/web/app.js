const state={
  scenarios:[],suite:null,snapshot:null,logs:[],filter:"",
  configuration:null,validation:null,selected:new Set(),screen:"configuration"
};
const $=id=>document.getElementById(id);

async function api(path,options={}){
  const response=await fetch(path,{headers:{"Content-Type":"application/json"},...options});
  if(!response.ok){
    const body=await response.json().catch(()=>({detail:response.statusText}));
    throw new Error(body.detail||response.statusText);
  }
  return response.json();
}
function toast(message,error=false){
  const el=document.createElement("div");el.className=`toast ${error?"error":""}`;
  el.textContent=message;document.body.appendChild(el);setTimeout(()=>el.remove(),3500);
}
function escapeHtml(value){return String(value).replace(/[&<>'"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[c]))}

function readConfigurationForm(){
  return {
    system:{
      replica_count:Number($("cfg-replica-count").value),
      max_faulty:Number($("cfg-max-faulty").value),
      initial_view:Number($("cfg-initial-view").value),
      primary_policy:$("cfg-primary-policy").value
    },
    timing:{
      progress_timeout_sec:Number($("cfg-progress-timeout").value),
      decision_timeout_sec:Number($("cfg-decision-timeout").value),
      heartbeat_period_sec:Number($("cfg-heartbeat").value),
      client_publish_delay_sec:Number($("cfg-client-delay").value)
    },
    safety:{allow_confirmed_release:false},
    request:{emergency_stop:true}
  };
}
function writeConfigurationForm(config){
  $("cfg-replica-count").value=config.system.replica_count;
  $("cfg-max-faulty").value=config.system.max_faulty;
  $("cfg-initial-view").value=config.system.initial_view;
  $("cfg-primary-policy").value=config.system.primary_policy;
  $("cfg-progress-timeout").value=config.timing.progress_timeout_sec;
  $("cfg-decision-timeout").value=config.timing.decision_timeout_sec;
  $("cfg-heartbeat").value=config.timing.heartbeat_period_sec;
  $("cfg-client-delay").value=config.timing.client_publish_delay_sec;
}
async function validateConfiguration(showToast=false){
  const config=readConfigurationForm();
  try{
    const validation=await api("/api/configuration/validate",{
      method:"POST",body:JSON.stringify({configuration:config})
    });
    state.validation=validation;
    state.configuration=validation.configuration;
    renderConfigurationValidation();
    if(showToast&&validation.valid)toast("Configuration is valid.");
    return validation;
  }catch(err){
    if(showToast)toast(err.message,true);
    return {valid:false,errors:[err.message],warnings:[],derived:{}};
  }
}
function renderConfigurationValidation(){
  const validation=state.validation||{valid:false,errors:[],warnings:[],derived:{}};
  const d=validation.derived||{};
  const config=validation.configuration||readConfigurationForm();
  const n=config.system?.replica_count??0;const f=config.system?.max_faulty??0;
  $("formula-status").textContent=`${n} = 3 × ${f} + 1`;
  $("formula-status").className=validation.valid?"valid":"invalid";
  $("derived-primary").textContent=`Replica ${d.primary_id??"—"}`;
  $("derived-prepare").textContent=d.prepare_threshold??"—";
  $("derived-commit").textContent=d.commit_threshold??"—";
  $("derived-view-change").textContent=d.view_change_threshold??"—";
  const messages=[];
  (validation.errors||[]).forEach(item=>messages.push(`<div class="config-message error">✕ ${escapeHtml(item)}</div>`));
  (validation.warnings||[]).forEach(item=>messages.push(`<div class="config-message warning">! ${escapeHtml(item)}</div>`));
  if(validation.valid&&!messages.length)messages.push(`<div class="config-message success">✓ Configuration is valid and ready for scenario selection.</div>`);
  $("configuration-messages").innerHTML=messages.join("");
  $("continue-to-scenarios").disabled=!validation.valid;
}

async function initializeConfiguration(){
  const defaults=await api("/api/configuration/defaults");
  let config=defaults.configuration;
  try{
    const saved=JSON.parse(localStorage.getItem("pbftSystemConfiguration")||"null");
    if(saved)config=saved;
  }catch(_){/* ignore corrupt local state */}
  writeConfigurationForm(config);
  await validateConfiguration();
}

function setScreen(name){
  state.screen=name;
  $("configuration-step").classList.toggle("hidden",name!=="configuration");
  $("console-step").classList.toggle("hidden",name==="configuration");
  $("step-config-nav").classList.toggle("active",name==="configuration");
  $("step-scenarios-nav").classList.toggle("active",name==="scenarios");
  const running=state.suite&&["QUEUED","RUNNING"].includes(state.suite.status);
  $("step-execution-nav").classList.toggle("active",name==="console"&&running);
  $("step-results-nav").classList.toggle("active",name==="console"&&!running&&!!state.suite?.results?.length);
}
async function continueToScenarios(){
  const validation=await validateConfiguration(true);
  if(!validation.valid)return;
  localStorage.setItem("pbftSystemConfiguration",JSON.stringify(validation.configuration));
  state.selected.clear();
  await loadScenarios(validation.configuration);
  renderActiveConfiguration();
  $("step-scenarios-nav").disabled=false;
  setScreen("scenarios");
}
function suiteIsRunning(){return !!state.suite&&["QUEUED","RUNNING"].includes(state.suite.status)}
function editConfiguration(){
  if(suiteIsRunning()){toast("Stop or finish the active suite before changing configuration.",true);return}
  setScreen("configuration");
}
function renderActiveConfiguration(){
  const config=state.configuration;const d=state.validation?.derived||{};
  if(!config)return;
  $("active-config-title").textContent=`n=${config.system.replica_count} · f=${config.system.max_faulty}`;
  $("active-config-detail").textContent=`Primary ${d.primary_id} · commit quorum ${d.commit_threshold}`;
}

async function loadScenarios(configuration=state.configuration){
  const data=await api("/api/scenarios/compatible",{
    method:"POST",body:JSON.stringify({configuration})
  });
  state.validation=data.validation;state.configuration=data.validation.configuration;
  state.scenarios=data.scenarios;
  $("catalog-count").textContent=String(state.scenarios.filter(s=>s.compatibility?.compatible).length);
  renderScenarioList();
}
function filteredScenarios(){
  const q=state.filter.trim().toLowerCase();
  if(!q)return state.scenarios;
  return state.scenarios.filter(s=>`${s.name} ${s.category} ${s.description} ${(s.tags||[]).join(" ")}`.toLowerCase().includes(q));
}
function selectedScenarios(){return [...state.selected].filter(id=>state.scenarios.find(s=>s.id===id)?.compatibility?.compatible)}
function renderScenarioList(){
  const groups={};filteredScenarios().forEach(s=>(groups[s.category]??=[]).push(s));
  $("scenario-list").innerHTML=Object.entries(groups).map(([group,items])=>
    `<div class="scenario-group"><h4><span>${escapeHtml(group)}</span><em>${items.length}</em></h4>${items.map(s=>{
      const compatible=!!s.compatibility?.compatible;const checked=state.selected.has(s.id)&&compatible;
      const mode=s.execution_mode==="configurable"?"DYNAMIC":"LEGACY";
      return `<label class="scenario-item ${compatible?"":"incompatible"}" title="${escapeHtml(s.compatibility?.reason||"")}">
        <input type="checkbox" value="${escapeHtml(s.id)}" ${checked?"checked":""} ${compatible?"":"disabled"}>
        <span><span class="scenario-name-line">${escapeHtml(s.name)}<em class="mode-badge ${s.execution_mode==="configurable"?"dynamic":"legacy"}">${mode}</em></span>
        <small>${escapeHtml(s.description)}</small>${compatible?"":`<small class="incompatibility">${escapeHtml(s.compatibility?.reason||"Incompatible")}</small>`}</span>
      </label>`;
    }).join("")}</div>`
  ).join("")||`<div class="empty">No scenarios match the search.</div>`;
  document.querySelectorAll("#scenario-list input").forEach(el=>el.addEventListener("change",event=>{
    if(event.target.checked)state.selected.add(event.target.value);else state.selected.delete(event.target.value);updateSelectedCount();
  }));
  updateSelectedCount();
}
function updateSelectedCount(){$("selected-count").textContent=`${selectedScenarios().length} selected`}

async function run(ids){
  if(!ids.length){toast("Select at least one compatible scenario.",true);return}
  try{
    const data=await api("/api/suites",{method:"POST",body:JSON.stringify({
      scenario_ids:ids,repeat:Number($("repeat").value),
      stop_on_failure:$("stop-on-failure").checked,
      configuration:state.configuration
    })});
    toast(`Suite ${data.suite_id} started.`);setScreen("console");
  }catch(err){toast(err.message,true)}
}

function connect(){
  const protocol=location.protocol==="https:"?"wss":"ws";
  const socket=new WebSocket(`${protocol}://${location.host}/ws`);
  socket.onopen=()=>{$("connection").textContent="Connected — live ROS 2 test data"};
  socket.onclose=()=>{$("connection").textContent="Disconnected — reconnecting…";setTimeout(connect,1000)};
  socket.onmessage=event=>{
    const message=JSON.parse(event.data);
    if(message.type==="suite"||message.type==="suite_created"){
      state.suite=message.suite;renderSuite();
    }else if(message.type==="snapshot"){
      state.snapshot=message.snapshot;renderSnapshot();
    }else if(message.type==="log"){
      state.logs.push(message.item);state.logs=state.logs.slice(-300);renderLog();
    }else if(message.type==="initial"&&message.suites?.length){
      state.suite=message.suites[0];
      if(state.suite.configuration){state.configuration=state.suite.configuration;writeConfigurationForm(state.configuration);validateConfiguration().then(renderActiveConfiguration)}
      state.snapshot=state.suite.active_snapshot;state.logs=state.suite.live_log||[];
      renderSuite();renderSnapshot();renderLog();
    }
  };
}
function renderSuite(){
  const suite=state.suite;if(!suite)return;
  $("suite-status").textContent=suite.status;$("active-scenario").textContent=scenarioName(suite.active_scenario);
  $("result-count").textContent=`${suite.results.length} tests`;
  const link=$("report-link");link.href=`/api/suites/${suite.suite_id}/report`;link.classList.toggle("disabled",!suite.results.length);
  renderResults(suite.results);
  if(suite.configuration){state.configuration=suite.configuration;renderActiveConfiguration()}
  if(suite.active_snapshot){state.snapshot=suite.active_snapshot;renderSnapshot()}
  state.logs=suite.live_log||state.logs;renderLog();
  const running=["QUEUED","RUNNING"].includes(suite.status);
  $("edit-configuration").disabled=running;
  $("step-execution-nav").disabled=!running;
  $("step-results-nav").disabled=!suite.results.length;
  if(running)setScreen("console");
  else if(suite.results.length)setScreen("console");
}
function scenarioName(id){return state.scenarios.find(item=>item.id===id)?.name||id||"—"}
function replicaCount(){
  return Number(state.suite?.configuration?.system?.replica_count||state.configuration?.system?.replica_count||Object.keys(state.snapshot?.replicas||{}).length||4);
}
function renderSnapshot(){
  const s=state.snapshot;if(!s)return;
  $("current-view").textContent=s.current_view??0;$("primary-id").textContent=`Replica ${s.primary_id??0}`;
  $("safety-state").textContent=s.safety?.state||"—";
  $("safety-output").textContent=s.safety?.emergency_stop===true?"STOP ACTIVE":s.safety?.emergency_stop===false?"RELEASED":"—";
  $("elapsed").textContent=`${Number(s.elapsed_sec||0).toFixed(1)} s`;
  renderReplicas(s);renderPipeline(s);renderTimeline(s.timeline||[]);renderAssertions(s.assertions||[]);
}
function renderReplicas(s){
  const count=replicaCount();$("replica-summary").textContent=`${count} configured replicas`;
  $("replicas").style.setProperty("--replica-columns",String(Math.min(count,5)));
  $("replicas").innerHTML=Array.from({length:count},(_,id)=>{
    const r=s.replicas?.[String(id)]||{};const primary=s.primary_id===id;
    return `<div class="replica ${primary?"primary":""} ${r.is_byzantine?"byzantine":""}">
      <div class="replica-head"><strong>Replica ${id}</strong><span class="pill ${r.is_byzantine?"purple":primary?"green":""}">${r.is_byzantine?"FAULT INJECTED":primary?"PRIMARY":"BACKUP"}</span></div>
      <div class="replica-grid"><div><span>Phase</span><b>${escapeHtml(r.phase||"IDLE")}</b></div><div><span>View</span><b>${r.view??0}</b></div><div><span>Prepare</span><b>${r.prepare_count??0}</b></div><div><span>Commit</span><b>${r.commit_count??0}</b></div><div><span>Prepared</span><b>${r.prepared?"YES":"NO"}</b></div><div><span>Committed</span><b>${r.committed?"YES":"NO"}</b></div></div>
      ${r.detail?`<p class="replica-detail">${escapeHtml(r.detail)}</p>`:""}</div>`;
  }).join("");
}
function renderPipeline(s){
  const c=s.counts||{};const hasDecision=!!s.decision;const safety=s.safety?.state;
  const stages=[["REQUEST",c.request>0,c.request?`${c.request} observed`:"waiting"],["PRE-PREPARE",c.pre_prepare>0,c.pre_prepare?`${c.pre_prepare} published`:"waiting"],["PREPARE",c.prepare>0,c.prepare?`${c.prepare} messages`:"waiting"],["COMMIT",c.commit>0,c.commit?`${c.commit} messages`:"waiting"],["DECISION",hasDecision,hasDecision?`view ${s.decision.view}`:"waiting"],["VIEW-CHANGE",(c.view_change||0)>0,`${c.view_change||0} votes`],["NEW-VIEW",(c.new_view||0)>0,`${c.new_view||0} published`],["RECOVERY",(c.recovery_pre_prepare||0)>0,`${c.recovery_pre_prepare||0} PRE-PREPARE`],["SAFETY",!!safety,safety||"waiting"]];
  $("pipeline").innerHTML=stages.map(([name,done,detail])=>`<div class="stage ${done?"done":""} ${name==="VIEW-CHANGE"&&done?"fault":""} ${safety==="FAIL_SAFE_STOP"&&name==="SAFETY"?"blocked":""}"><b>${name}</b><small>${escapeHtml(String(detail))}</small></div>`).join("");
}
function renderTimeline(items){$("timeline").innerHTML=items.slice(-80).reverse().map(item=>`<div class="timeline-item"><time>${Number(item.elapsed_sec).toFixed(2)}s</time><span class="timeline-type">${escapeHtml(item.type)}</span><code>${escapeHtml(compact(item.payload))}</code></div>`).join("")||`<div class="empty">Waiting for ROS 2 events.</div>`}
function compact(payload){const text=JSON.stringify(payload);return text.length>160?text.slice(0,157)+"…":text}
function renderAssertions(items){
  const container=$("assertions");if(!items.length){container.className="assertions empty";container.textContent="Assertions are evaluated continuously and finalized at the terminal condition.";$("assertion-summary").textContent="Waiting";return}
  container.className="assertions";const passed=items.filter(a=>a.passed).length;$("assertion-summary").textContent=`${passed}/${items.length} passed`;
  container.innerHTML=items.map(a=>`<div class="assertion ${a.passed?"pass":"fail"}"><span class="icon">${a.passed?"✓":"✕"}</span><span>${escapeHtml(a.label)}</span><code>${escapeHtml(formatValue(a.actual))}</code></div>`).join("");
}
function formatValue(value){if(value===null||value===undefined)return "—";if(typeof value==="object")return JSON.stringify(value);return String(value)}
function renderLog(){const pre=$("live-log");pre.textContent=state.logs.map(item=>`[${item.time}] [${item.source}] ${item.line}`).join("\n");pre.scrollTop=pre.scrollHeight}
function renderResults(results){
  const el=$("results");if(!results.length){el.className="results empty";el.textContent="No suite has been executed yet.";return}
  el.className="results";el.innerHTML=results.map(r=>`<div class="result-row"><span class="status ${String(r.status).toLowerCase()}">${escapeHtml(r.status)}</span><strong>${escapeHtml(r.scenario_name)}</strong><span>${Number(r.duration_sec||0).toFixed(2)} s</span><span>Domain ${escapeHtml(String(r.ros_domain_id||""))}</span></div>`).join("");
}

let validationTimer=null;
document.querySelectorAll("#configuration-form input,#configuration-form select").forEach(el=>el.addEventListener("input",()=>{
  clearTimeout(validationTimer);validationTimer=setTimeout(()=>validateConfiguration(),180);
}));
document.querySelectorAll(".preset").forEach(button=>button.addEventListener("click",()=>{
  $("cfg-replica-count").value=button.dataset.n;$("cfg-max-faulty").value=button.dataset.f;validateConfiguration();
}));
$("reset-configuration").onclick=async()=>{localStorage.removeItem("pbftSystemConfiguration");const data=await api("/api/configuration/defaults");writeConfigurationForm(data.configuration);await validateConfiguration()};
$("continue-to-scenarios").onclick=continueToScenarios;
$("edit-configuration").onclick=editConfiguration;
$("step-config-nav").onclick=editConfiguration;
$("step-scenarios-nav").onclick=()=>{if(state.configuration)setScreen("scenarios")};
$("scenario-search").addEventListener("input",event=>{state.filter=event.target.value;renderScenarioList()});
$("select-visible").onclick=()=>{filteredScenarios().filter(s=>s.compatibility?.compatible).forEach(s=>state.selected.add(s.id));renderScenarioList()};
$("clear-selection").onclick=()=>{state.selected.clear();renderScenarioList()};
$("run-selected").onclick=()=>run(selectedScenarios());
$("run-all").onclick=()=>run(state.scenarios.filter(item=>item.compatibility?.compatible).map(item=>item.id));
$("cancel").onclick=async()=>{try{await api("/api/cancel",{method:"POST"});toast("Cancellation requested.")}catch(err){toast(err.message,true)}};
$("clear-log").onclick=()=>{state.logs=[];renderLog()};

initializeConfiguration().catch(err=>toast(err.message,true));connect();
