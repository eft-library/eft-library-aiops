'use strict';
const icons = {
 dashboard:'<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
 reports:'<path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9Z"/><path d="M14 3v6h6M8 13h8M8 17h5"/>',
 collections:'<ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v7c0 4 16 4 16 0V5M4 12v7c0 4 16 4 16 0v-7"/>',
 jobs:'<path d="M4 6h12M4 12h16M4 18h9M20 3v6M17 6h6"/>',
 arrow:'<path d="M5 12h14m-5-5 5 5-5 5"/>', calendar:'<rect x="3" y="5" width="18" height="16" rx="2"/><path d="M7 3v4m10-4v4M3 10h18"/>',
 refresh:'<path d="M20 8a8 8 0 1 0 0 8M20 3v5h-5"/>', warning:'<path d="m12 3 10 18H2Z"/><path d="M12 9v5m0 3h.01"/>',
 check:'<circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/>', clock:'<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
 spark:'<path d="m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5Z"/>', chart:'<path d="M4 3v17h17M8 15V9m5 6V5m5 10v-4"/>',
 search:'<circle cx="10" cy="10" r="6"/><path d="m15 15 5 5"/>'
};
const icon = name => `<svg viewBox="0 0 24 24" aria-hidden="true">${icons[name] || icons.reports}</svg>`;
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const num = value => Number.isFinite(Number(value)) && value !== null ? Number(value).toLocaleString('ko-KR') : '—';
const kinds = {daily:'일일',weekly:'주간',monthly:'월간'};
const labels = {WARNING:'주의',CRITICAL:'심각',OK:'정상',INFO:'참고',COMPLETED:'분석 완료',PENDING:'대기 중',PREPARED:'입력 준비',RUNNING:'분석 중',FAILED:'실패',TIMEOUT:'시간 초과',CANCELLED:'취소',collected:'수집 완료',partial:'부분 수집',disabled:'비활성',failed:'수집 실패'};
const badge = (value, custom) => `<span class="badge ${esc(String(value || 'unknown').toLowerCase())}">${esc(custom || labels[value] || value || '평가 전')}</span>`;
const stamp = (value, time = true) => {
 if (!value) return '—';
 const date = new Date(value.length === 10 ? value + 'T00:00:00+09:00' : value);
 if (Number.isNaN(date.getTime())) return '—';
 const parts=Object.fromEntries(new Intl.DateTimeFormat('ko-KR', {timeZone:'Asia/Seoul', year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(date).map(part=>[part.type,part.value]));
 return `${parts.year}.${parts.month}.${parts.day}${time?' '+parts.hour+':'+parts.minute:''}`;
};
const dayLabel = value => stamp(value,false);
const period = row => {
 if (!row?.period?.start) return '—';
 if (row.kind === 'daily') return dayLabel(row.period.start);
 const end = new Date(row.period.end + 'T00:00:00+09:00'); end.setTime(end.getTime()-86400000);
 return `${dayLabel(row.period.start)} – ${stamp(end.toISOString(),false)}`;
};
const reportName = row => `${kinds[row.kind] || '운영'} 분석 보고서`;
const reportURL = id => `#report/${encodeURIComponent(id)}`;
const qualityText = row => row.coverage_complete ? '수집 기준 충족' : '일부 데이터 누락';
const empty = (title, description, className='') => `<div class="empty ${className}">${icon('reports')}<strong>${esc(title)}</strong><p>${esc(description)}</p></div>`;
const more = (href, text='전체 보기') => `<a class="text-link" href="${esc(href)}">${esc(text)} ${icon('arrow')}</a>`;
let revision=0;
const content=document.getElementById('content');
const state = {tab:'dashboard'};

async function api(path) {
 const controller = new AbortController(); const timer=setTimeout(()=>controller.abort(),12000);
 try {
  const response=await fetch('/read/'+path, {signal:controller.signal,cache:'no-store'});
  if (!response.ok) { let code; try { code=(await response.json()).error; } catch { code='api_unavailable'; } throw new Error(code); }
  return await response.json();
 } finally { clearTimeout(timer); }
}
function connection(online) {
 const element=document.getElementById('connection');
 element.className='connection '+(online?'online':'offline'); element.innerHTML=`<i></i>${online?'API 연결됨':'연결 확인 필요'}`;
}
function head(title, subtitle, eyebrow='OPERATIONS OVERVIEW', trailing='') {
 const today = new Intl.DateTimeFormat('ko-KR',{timeZone:'Asia/Seoul',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
 return `<section class="page-head"><div><div class="eyebrow">${eyebrow}</div><h1>${esc(title)}</h1><p>${esc(subtitle)}</p></div><div class="head-actions">${trailing || `<span class="date-chip">${icon('calendar')}${esc(today)}</span>`}<button class="button" id="refresh">${icon('refresh')}새로고침</button></div></section>`;
}
function reportRows(rows) {
 return rows.map(row=>`<tr><td><div class="report-cell"><span class="type-icon">${icon('reports')}</span><div><a class="row-title" href="${reportURL(row.report_id)}">${esc(period(row))} ${esc(reportName(row))}</a><span class="row-sub">${esc(row.summary || row.report_id)}</span></div></div></td><td><span class="kind">${esc(kinds[row.kind])}</span></td><td>${badge(row.severity)}</td><td>${badge(row.status)}</td><td><span class="coverage-mini"><i class="coverage-dot ${row.coverage_complete?'complete':''}"></i>${esc(qualityText(row))}</span></td><td class="numeric small">${esc(stamp(row.completed_at || row.created_at))}</td><td><a class="text-link" href="${reportURL(row.report_id)}" aria-label="${esc(row.report_id)} 상세 보기">${icon('arrow')}</a></td></tr>`).join('');
}
function reportTable(rows) {
 return rows.length ? `<div class="table-wrap"><table class="data-table"><thead><tr><th>보고서</th><th>유형</th><th>위험도</th><th>분석 상태</th><th>데이터 품질</th><th>생성 시각 · KST</th><th></th></tr></thead><tbody>${reportRows(rows)}</tbody></table></div>` : empty('아직 보고서가 없습니다','분석이 진행되면 이곳에서 일일·주간·월간 결과를 확인할 수 있습니다.');
}
function pager(data) {
 const pages=Math.max(1,Math.ceil(data.total/data.page_size));
 return `<div class="table-foot"><span>전체 ${num(data.total)}개</span><div class="pagination"><button class="button page-button" data-page="${data.page-1}" ${data.page<=1?'disabled':''}>이전</button><span>${data.page} / ${pages}</span><button class="button page-button" data-page="${data.page+1}" ${data.page>=pages?'disabled':''}>다음</button></div></div>`;
}
function bindPages() {
 document.querySelectorAll('.page-button').forEach(button=>button.addEventListener('click',()=>{
  const [base,query='']=location.hash.slice(1).split('?'); const values=new URLSearchParams(query);
  values.set('page',button.dataset.page); location.hash=base+'?'+values;
 }));
}
function trend(rows) {
 if (!rows.length) return empty('추이를 기다리고 있습니다','일일 보고서가 쌓이면 날짜별 수집 이벤트를 확인할 수 있습니다.','chart-empty');
 const byDay=new Map(rows.map(row=>[row.period.start,row]));
 const anchor=new Date(rows[0].period.start+'T00:00:00Z');
 const days=Array.from({length:7},(_,i)=>{const date=new Date(anchor);date.setUTCDate(date.getUTCDate()-(6-i));return date.toISOString().slice(0,10);});
 const values=days.map(day=>byDay.has(day)?Number(byDay.get(day).totals?.events || 0):null);
 const max=Math.max(...values.filter(v=>v!==null),1);
 return `<div class="chart"><div class="chart-body"><div class="chart-y"><span>${num(max)}</span><span>${num(Math.round(max*2/3))}</span><span>${num(Math.round(max/3))}</span><span>0</span></div><div class="chart-plot">${days.map((day,index)=>{
  const value=values[index];const row=byDay.get(day);const title=`${day} · ${value===null?'보고서 없음':num(value)+'건 · '+qualityText(row)}`;
  return `<div class="bar-column" title="${esc(title)}"><div class="bar-slot"><div class="bar ${index===6?'latest':''} ${value===null?'missing':''}" style="height:${value===null?2:Math.max(2,value/max*100)}%" role="img" aria-label="${esc(title)}"></div></div><span class="bar-label">${day.slice(5).replace('-','.')}</span></div>`;
 }).join('')}</div></div></div>`;
}
async function dashboard(ticket) {
 const [overview, recent, daily, pending, batch]=await Promise.all([api('status'),api('reports?page_size=5'),api('reports?type=daily&status=COMPLETED&page_size=14'),api('jobs?status=PENDING&page_size=1'),api('collections?page_size=1')]);
 if(ticket!==revision)return;
 const latest=recent.items.find(r=>r.status==='COMPLETED') || daily.items[0];
 const lastDaily=daily.items[0]; const quality=lastDaily?.data_quality || {};
 const observed=quality.observed_slots; const expected=quality.expected_slots;
 const lastBatch=batch.items[0];const coverage=lastBatch?.coverage || {};
 const active=Object.values(coverage).filter(s=>s.status!=='disabled');
 const done=active.filter(s=>s.status==='collected').length;
 content.innerHTML=head('운영 대시보드','서비스의 상태와 분석 결과를 한눈에 확인하세요.')+`
 <section class="kpi-grid" aria-label="주요 현황">
 <article class="kpi"><div class="kpi-label">${icon('warning')}최근 분석 위험도</div><div class="kpi-value ${latest?.severity==='WARNING'?'warn':latest?.severity==='CRITICAL'?'danger':latest?.severity?'green':''}">${esc(labels[latest?.severity] || '평가 전')}</div><div class="kpi-foot">${latest?esc(period(latest))+' · '+esc(kinds[latest.kind])+' 분석':'완료된 분석을 기다리고 있습니다'}</div></article>
 <article class="kpi"><div class="kpi-label">${icon('collections')}일일 수집 이벤트</div><div class="kpi-value">${num(lastDaily?.totals?.events)}<small>건</small></div><div class="kpi-foot">${lastDaily?esc(dayLabel(lastDaily.period.start))+' 보고서 기준':'일일 보고서 기준'}</div><div class="kpi-deco" aria-hidden="true"><i></i><i></i><i></i><i></i><i></i></div></article>
 <article class="kpi"><div class="kpi-label">${icon('check')}정기 수집 슬롯</div><div class="kpi-value purple">${num(observed)}<small>/ ${num(expected)}</small></div><div class="kpi-foot">${lastDaily?esc(dayLabel(lastDaily.period.start))+' · '+esc(qualityText(lastDaily)):'일일 보고서 생성 후 확인 가능'}</div></article>
 <article class="kpi"><div class="kpi-label">${icon('clock')}분석 대기 작업</div><div class="kpi-value">${num(pending.total)}<small>개</small></div><div class="kpi-foot">최근 수집 ${lastBatch?esc(stamp(lastBatch.collected_at)):'기록 없음'}</div></article>
 </section>
 <div class="dashboard-grid"><section class="panel"><div class="panel-heading"><h2>일일 수집 추이<span class="sub">최근 일일 보고서의 관측 이벤트 수 · 누락일은 집계하지 않음</span></h2><span class="chart-legend"><i class="legend-dot"></i>수집 이벤트</span></div>${trend(daily.items)}</section>
 <section class="panel summary-panel"><div class="panel-heading"><h2>최근 분석 인사이트</h2>${latest?more(reportURL(latest.report_id),'보고서 보기'):''}</div>${latest?`<div class="summary-content"><div class="summary-title">${icon('spark')}AI 분석 요약 ${badge(latest.severity)}</div><p>${esc(latest.summary || '분석 요약이 없습니다.')}</p><div class="summary-meta"><span>${esc(period(latest))}</span><i class="dot"></i><span>${esc(latest.model || '기본 모델')}</span></div></div>`:empty('분석 결과를 기다리고 있습니다','완료된 보고서의 주요 내용을 이곳에 표시합니다.','chart-empty')}</section></div>
 <section class="panel table-section"><div class="panel-heading"><h2>최근 보고서<span class="sub">기간별 최신 수정본을 보여드립니다.</span></h2>${more('#reports')}</div>${lastDaily && !lastDaily.coverage_complete?`<div class="quality-note">${icon('warning')}<div><strong>수집 범위가 불완전한 보고서가 있습니다.</strong> 관측되지 않은 시간대는 정상으로 판단하지 않습니다.</div></div><div style="height:18px"></div>`:''}${reportTable(recent.items)}<div class="table-foot"><span>일일 · 주간 · 월간 분석 보고서</span><span>${lastBatch?`최근 배치: ${num(done)} / ${num(active.length)}개 소스 수집 완료`:'수집 배치 없음'}</span></div></section>`;
 bindCommon(); connection(overview.database?.status==='ready');
}
function option(value,label,current) {return `<option value="${esc(value)}" ${value===current?'selected':''}>${esc(label)}</option>`;}
async function reportsPage(query,ticket) {
 const filters=new URLSearchParams(query); const params=new URLSearchParams(filters);params.set('page_size','12');
 const data=await api('reports?'+params);if(ticket!==revision)return;
 content.innerHTML=head('분석 보고서','기간별 분석 결과와 발견 사항을 확인하세요.','ANALYSIS REPORTS')+`
 <form id="report-filters" class="filterbar"><label class="field"><span>보고서 유형</span><select name="type">${option('','전체 유형',filters.get('type')||'')}${Object.entries(kinds).map(([key,label])=>option(key,label,filters.get('type'))).join('')}</select></label>
 <label class="field"><span>위험도</span><select name="severity">${option('','전체 위험도',filters.get('severity')||'')}${['CRITICAL','WARNING','INFO','OK'].map(key=>option(key,labels[key],filters.get('severity'))).join('')}</select></label>
 <label class="field"><span>분석 상태</span><select name="status">${option('','전체 상태',filters.get('status')||'')}${['COMPLETED','RUNNING','PREPARED','FAILED','CANCELLED'].map(key=>option(key,labels[key],filters.get('status'))).join('')}</select></label>
 <label class="field"><span>분석 시작일 · 이후</span><input type="date" name="start" value="${esc(filters.get('start')||'')}"></label><label class="field"><span>분석 시작일 · 이전</span><input type="date" name="end" value="${esc(filters.get('end')||'')}"></label>
 <label class="checkbox"><input type="checkbox" name="history" value="true" ${filters.get('history')==='true'?'checked':''}>이전 수정본 포함</label><button class="button" type="submit">${icon('search')}조회</button></form>
 <p class="result-count">${num(data.total)}개의 보고서 ${filters.get('history')==='true'?'· 전체 수정본':'· 기간별 최신 수정본'}</p><section class="panel">${reportTable(data.items)}${pager(data)}</section>`;
 document.getElementById('report-filters').addEventListener('submit',event=>{
  event.preventDefault();const values=new URLSearchParams();for(const [key,value] of new FormData(event.currentTarget)){if(value)values.set(key,value);}
  if(values.get('start')&&values.get('end')&&values.get('start')>values.get('end')){showToast('시작일이 종료일보다 늦습니다.');return;}
  const target='#reports?'+values;if(location.hash===target)render();else location.hash=target;
 });bindCommon();bindPages();connection(true);
}
function metadata(entries) {return `<dl class="metadata">${entries.map(([label,value])=>`<div><dt>${esc(label)}</dt><dd>${value}</dd></div>`).join('')}</dl>`;}
function evidenceDisplay(id, data) {
 const entry=data.evidence?.[id];let body;
 if(!entry)body='원문은 이 보고서 입력에 포함되어 있지 않습니다.';
 else if(entry.message)body=(entry.service?`[${entry.service}] `:'')+(entry.timestamp?stamp(entry.timestamp)+'\n':'')+entry.message;
 else if(entry.report_id)body=entry.summary+'\n\n'+(entry.findings || []).map(finding=>typeof finding==='string'?finding:finding.issue).join('\n');
 else if(entry.samples!==undefined)body=`${entry.service || ''} · ${entry.name || entry.metric || '시스템 지표'}\n표본 ${num(entry.samples)}개 · 최소 ${num(entry.min)} · 최대 ${num(entry.max)} · 평균 ${num(entry.mean)}`;
 else body=JSON.stringify(entry,null,2);
 const link=entry?.report_id?`<a class="text-link" href="${reportURL(entry.report_id)}">하위 보고서 보기 ${icon('arrow')}</a>`:'';
 return `<details class="evidence"><summary>${esc(id)}</summary><pre>${esc(body)}</pre>${link}</details>`;
}
function specialistSection(key,title,data) {
 const section=data.result?.report?.[key];
 if(!section){
  const message=data.result?'이전 형식의 보고서입니다. 이 구역은 확장된 일일 점검 결과부터 표시됩니다.':'아직 완료된 분석 결과가 없습니다. 분석이 완료되면 이 구역에 표시됩니다.';
  return `<section class="panel specialist-panel"><div class="panel-heading"><h2>${esc(title)}</h2></div><div class="section-unavailable">${esc(message)}</div></section>`;
 }
 const status={ISSUES_OBSERVED:['warning','문제 관측'],NO_ISSUES_OBSERVED:['collected','관측된 문제 없음'],INSUFFICIENT_DATA:['unknown','판단 근거 부족']}[section.status] || ['unknown','판단 근거 부족'];
 return `<section class="panel specialist-panel"><div class="panel-heading"><h2>${esc(title)}<span class="sub">${key==='incident_analysis'?'오류 흐름 · 원인 후보 · 영향과 복구 근거':'응답 지연 · 자원 지표 · 병목 근거'}</span></h2>${badge(status[0],status[1])}</div><p class="specialist-summary">${esc(section.summary)}</p>${section.findings.map((finding,index)=>`<article class="finding"><div class="finding-head"><span class="finding-index">${String(index+1).padStart(2,'0')}</span><h3>${esc(finding.issue)}</h3></div><div class="finding-body"><h4>원인 가설</h4><p>${esc(finding.hypothesis)}</p><h4>확인 권고</h4><ul>${finding.recommended_checks.map(check=>`<li>${esc(check)}</li>`).join('')}</ul><h4>분석 근거 · 펼쳐서 보기</h4>${finding.evidence_ids.map(eid=>evidenceDisplay(eid,data)).join('')}</div></article>`).join('')}${section.limitations.length?`<div class="specialist-limitations"><h3>판단 한계</h3><ul>${section.limitations.map(line=>`<li>${esc(line)}</li>`).join('')}</ul></div>`:''}</section>`;
}
async function detailPage(id,ticket) {
 const data=await api('reports/'+encodeURIComponent(id));if(ticket!==revision)return;
 const report=data.result?.report;const quality=data.data_quality || {};
 const daily=data.kind==='daily'; const expected=daily?quality.expected_slots:quality.expected_daily_reports;
 const observed=daily?quality.observed_slots:quality.available_daily_reports;
 const ratio=expected?Math.min(100,Math.max(0,Number(observed || 0)/Number(expected)*100)):0;
 const childIds=Object.values(data.lineage?.daily_report_ids || {});
 content.innerHTML=`<a class="back-link" href="#reports">${icon('arrow')}보고서 목록으로</a>`+head(`${kinds[data.kind] || ''} 분석 보고서`,`${period(data)} · ${data.report_id}`,'REPORT DETAILS',`<span class="date-chip">${badge(data.status)}</span>`)+`
 <div class="detail-layout"><div class="detail-main"><section class="panel detail-summary"><div class="detail-period">${esc(period(data))} <span class="muted">· Asia/Seoul</span></div><h2>분석 요약</h2>${badge(report?.severity)}<p>${esc(report?.summary || (data.status==='FAILED'?'분석을 완료하지 못했습니다. 작업 현황에서 오류를 확인하세요.':'아직 완료된 분석 결과가 없습니다.'))}</p></section>
 <section class="panel"><div class="panel-heading"><h2>주요 발견 사항<span class="sub">${num(report?.findings?.length || 0)}개의 항목 · 관측 사실과 가설을 구분해 확인하세요.</span></h2></div>${report?.findings?.length?report.findings.map((finding,index)=>`<article class="finding"><div class="finding-head"><span class="finding-index">${String(index+1).padStart(2,'0')}</span><h3>${esc(finding.issue)}</h3></div><div class="finding-body"><h4>원인 가설</h4><p>${esc(finding.hypothesis)}</p><h4>확인 권고</h4><ul>${finding.recommended_checks.map(check=>`<li>${esc(check)}</li>`).join('')}</ul><h4>분석 근거 · 펼쳐서 보기</h4>${finding.evidence_ids.map(eid=>evidenceDisplay(eid,data)).join('')}</div></article>`).join(''):empty('표시할 발견 사항이 없습니다','분석 결과가 생성되면 근거와 함께 확인할 수 있습니다.')}</section>
 ${daily?specialistSection('incident_analysis','장애 분석',data)+specialistSection('performance_analysis','성능 분석',data):''}
 <section class="panel limitations"><h2>분석 한계 및 참고 사항</h2><ul>${(report?.limitations?.length?report.limitations:data.input_limitations || []).map(line=>`<li>${esc(line)}</li>`).join('') || '<li>등록된 참고 사항이 없습니다.</li>'}</ul></section></div>
 <aside class="detail-side"><section class="panel"><h3>보고서 정보</h3>${metadata([['분석 유형',esc(kinds[data.kind])],['수정 버전',`v${String(data.revision).padStart(4,'0')}`],['생성 시각',esc(stamp(data.created_at))],['완료 시각',esc(stamp(data.completed_at))],['분석 모델',esc(data.result?.model || '—')],['자동 변경',data.result?'수행하지 않음':'—'],['관측 이벤트',num(data.totals?.events)+'건'],...(data.error?[['오류',`<span class="error-code">${esc(data.error)}</span>`]]:[])])}</section>
 <section class="panel"><h3>데이터 수집 품질</h3>${badge(data.coverage_complete?'collected':'partial',data.coverage_complete?'수집 기준 충족':'불완전한 데이터')}<div class="progress-track"><div class="progress-fill" style="width:${ratio}%"></div></div><div class="coverage-label"><span>${daily?'관측 슬롯':'완료된 일일 보고서'}</span><span>${num(observed)} / ${num(expected)}</span></div><div class="quality-grid"><div class="quality-metric"><strong>${num(daily?data.source_batch_count:quality.missing_dates?.length)}</strong>${daily?'입력 배치':'누락 날짜'}</div><div class="quality-metric"><strong>${num(daily?quality.unclassified_level_events:quality.incomplete_daily_reports?.length)}</strong>${daily?'미분류 이벤트':'불완전 일일'}</div><div class="quality-metric"><strong>${num(daily?quality.encoding_error_events:quality.outdated_report_ids?.length)}</strong>${daily?'인코딩 오류':'오래된 일일'}</div></div>${!data.coverage_complete?'<p class="service-message">누락된 시간대나 보고서는 정상으로 간주하지 않습니다.</p>':''}</section>
 ${childIds.length?`<section class="panel"><h3>입력 일일 보고서</h3><div class="history-list">${childIds.map(child=>`<a href="${reportURL(child)}">${esc(child)}</a>`).join('')}</div></section>`:''}
 </aside></div>`;bindCommon();connection(true);
}
async function collectionPage(query,ticket) {
 const params=new URLSearchParams(query);params.set('page_size','12');
 const [data,latest]=await Promise.all([api('collections?'+params),api('collections?page_size=1')]);if(ticket!==revision)return;
 const batch=latest.items[0];const services=Object.entries(batch?.coverage || {});
 content.innerHTML=head('수집 현황','최근 배치의 서비스별 수집 범위와 수집 이력을 확인하세요.','COLLECTION STATUS')+`
 ${batch?`<div class="result-count">최근 수집 ${esc(stamp(batch.collected_at))} · 배치 ${esc(batch.batch_id)}</div><section class="service-grid">${services.map(([name,service])=>`<article class="service-card"><div class="service-card-head"><span>${esc(name)}</span>${badge(service.status)}</div><div class="service-card-stats"><span><strong>${num(service.events)}</strong>이벤트</span><span><strong>${num(service.files)}</strong>파일</span><span><strong>${num(Math.round((service.bytes_read||0)/1024))}</strong>KB 읽음</span></div>${service.warnings?.length?`<div class="service-warnings">${esc(service.warnings.join(' · '))}</div>`:`<div class="service-message">${service.status==='disabled'?'수집 대상에서 비활성화되어 있습니다.':'이 배치에 등록된 수집 경고가 없습니다.'}</div>`}</article>`).join('')}</section>`:empty('아직 수집 배치가 없습니다','정기 수집이 시작되면 서비스별 현황이 표시됩니다.')}
 <section class="panel table-section"><div class="panel-heading"><h2>수집 배치 이력<span class="sub">DB에 등록된 배치 기준 · 서비스 정상 여부와는 별개입니다.</span></h2></div>${data.items.length?`<div class="table-wrap"><table class="data-table"><thead><tr><th>배치 ID</th><th>수집 시각 · KST</th><th>이벤트</th><th>읽은 용량</th><th>수집 범위</th></tr></thead><tbody>${data.items.map(row=>{const sources=Object.values(row.coverage).filter(s=>s.status!=='disabled');const complete=sources.filter(s=>s.status==='collected').length;return `<tr><td class="batch-name">${esc(row.batch_id)}</td><td class="numeric">${esc(stamp(row.collected_at))}</td><td class="numeric">${num(row.totals?.events)}</td><td class="numeric">${num(Math.round((row.totals?.bytes_read||0)/1024))} KB</td><td>${badge(complete===sources.length?'collected':'partial',`${complete}/${sources.length}개 소스 완료`)}</td></tr>`;}).join('')}</tbody></table></div>`:empty('등록된 이력이 없습니다','다음 수집 이후 등록 상태를 확인해 주세요.')}${pager(data)}</section>`;bindCommon();bindPages();connection(true);
}
async function jobsPage(query,ticket) {
 const params=new URLSearchParams(query);params.set('page_size','12');const data=await api('jobs?'+params);if(ticket!==revision)return;
 content.innerHTML=head('분석 작업 현황','작업의 진행 상태와 처리 결과를 확인하세요.','ANALYSIS ACTIVITY')+`
 <form id="job-filters" class="filterbar"><label class="field"><span>작업 상태</span><select name="status">${option('','전체 상태',params.get('status')||'')}${['PENDING','RUNNING','COMPLETED','FAILED','TIMEOUT','CANCELLED'].map(key=>option(key,labels[key],params.get('status'))).join('')}</select></label><button class="button" type="submit">${icon('search')}조회</button></form>
 <section class="panel">${data.items.length?`<div class="table-wrap"><table class="data-table"><thead><tr><th>대상 보고서</th><th>작업 상태</th><th>시도</th><th>접수 시각 · KST</th><th>완료 시각 · KST</th><th>오류</th></tr></thead><tbody>${data.items.map(row=>`<tr><td><a class="row-title" href="${reportURL(row.report_id)}">${esc(row.report_id)}</a><span class="row-sub">${esc(row.job_id)}</span></td><td>${badge(row.status)}</td><td class="numeric">${num(row.attempt_count)} / ${num(row.max_attempts)}</td><td class="numeric small">${esc(stamp(row.requested_at))}</td><td class="numeric small">${esc(stamp(row.finished_at))}</td><td>${row.error?`<span class="error-code">${esc(row.error)}</span>`:'<span class="muted">—</span>'}</td></tr>`).join('')}</tbody></table></div>`:empty('해당하는 작업이 없습니다','등록된 분석 작업은 상태별로 확인할 수 있습니다.')}${pager(data)}</section>`;
 document.getElementById('job-filters').addEventListener('submit',event=>{event.preventDefault();const values=new URLSearchParams();const value=new FormData(event.currentTarget).get('status');if(value)values.set('status',value);const target='#jobs?'+values;if(location.hash===target)render();else location.hash=target;});bindCommon();bindPages();connection(true);
}
function bindCommon(){document.getElementById('refresh')?.addEventListener('click',()=>render());}
function showToast(text){const toast=document.getElementById('toast');toast.textContent=text;toast.hidden=false;setTimeout(()=>toast.hidden=true,3500);}
async function render(){
 const ticket=++revision;const [route,query='']=(location.hash.slice(1)||'dashboard').split('?');const tab=route.startsWith('report/')?'reports':route;
 state.tab=tab;
 const nav=[['dashboard','대시보드'],['reports','분석 보고서'],['collections','수집 현황'],['jobs','작업 현황']];
 document.getElementById('navigation').innerHTML=nav.map(([key,label])=>`<a href="#${key}" title="${label}" class="${tab===key?'active':''}" ${tab===key?'aria-current="page"':''}>${icon(key)}<span>${label}</span>${tab===key?'<span class="nav-arrow">›</span>':''}</a>`).join('');
 document.getElementById('breadcrumb').textContent=nav.find(([key])=>key===tab)?.[1] || '대시보드';
 content.innerHTML='<div class="loading"><span class="spinner"></span>운영 데이터를 불러오는 중입니다.</div>';
 try{
  if(route.startsWith('report/'))await detailPage(decodeURIComponent(route.slice(7)),ticket);
  else if(tab==='reports')await reportsPage(query,ticket);
  else if(tab==='collections')await collectionPage(query,ticket);
  else if(tab==='jobs')await jobsPage(query,ticket);
  else await dashboard(ticket);
  if(ticket!==revision)return;
  document.getElementById('updated').textContent='마지막 조회 '+stamp(new Date().toISOString())+' KST';
 }catch(error){
  if(ticket!==revision)return;
  const messages={api_authentication:'웹 서버와 API의 인증 설정을 확인해 주세요.',not_found:'요청한 보고서를 찾을 수 없습니다.',invalid_filter:'조회 조건을 확인해 주세요.',busy:'다른 작업이 진행 중입니다. 잠시 후 다시 조회해 주세요.'};
  content.innerHTML=`<section class="error-panel">${icon('warning')}<h2>${error.message==='not_found'?'보고서가 없습니다':'데이터를 불러오지 못했습니다'}</h2><p>${esc(messages[error.message] || 'API 연결 상태를 확인하고 잠시 후 다시 조회해 주세요.')}</p><button class="button" id="refresh">${icon('refresh')}다시 조회</button></section>`;
  connection(false);bindCommon();
 }
}
window.addEventListener('hashchange',()=>{window.scrollTo(0,0);render();});
render();
