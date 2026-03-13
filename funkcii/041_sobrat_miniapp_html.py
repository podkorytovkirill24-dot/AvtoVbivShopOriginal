def build_miniapp_html() -> str:
    return """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover" />
  <title>Личный кабинет</title>
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&display=swap');
    :root{
      --bg:#0a1320; --bg2:#12233a; --card:#142941; --line:#2c4b6f;
      --text:#e8f1ff; --muted:#9ab4d1; --ok:#30e28d; --warn:#ffd166; --bad:#ff6b6b; --acc:#52c8ff;
    }
    *{box-sizing:border-box}
    body{
      margin:0; color:var(--text); font-family:'Space Grotesk',sans-serif; min-height:100vh; padding:14px;
      background:
        radial-gradient(1200px 500px at 5% -10%, #1f3f66 0%, rgba(31,63,102,0) 70%),
        radial-gradient(900px 420px at 110% 0%, #153454 0%, rgba(21,52,84,0) 70%),
        linear-gradient(180deg,var(--bg),var(--bg2));
    }
    .wrap{max-width:960px;margin:0 auto}
    .hero{border:1px solid var(--line);border-radius:20px;padding:16px;background:linear-gradient(135deg,rgba(82,200,255,.16),rgba(44,75,111,.2))}
    .title{font-size:24px;font-weight:700}
    .sub{color:var(--muted);margin-top:4px}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;margin-top:12px}
    .card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px}
    .label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.5px}
    .value{font-size:24px;font-weight:700;margin-top:6px}
    .mini{margin-top:6px;color:var(--muted);font-size:13px}
    .tabs{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
    .tab{padding:9px 12px;border-radius:10px;border:1px solid var(--line);background:#1a3654;color:var(--text);cursor:pointer;font-weight:600}
    .tab.active{background:#234b73;border-color:#6bb9ff}
    .panel{margin-top:10px;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px}
    .row{display:flex;justify-content:space-between;gap:10px;margin-top:8px}
    .list{margin-top:8px;display:grid;gap:8px}
    .item{padding:10px;border:1px solid var(--line);border-radius:10px;background:rgba(10,19,32,.55)}
    .item .top{display:flex;justify-content:space-between;gap:8px}
    .item .meta{color:var(--muted);font-size:12px;margin-top:4px}
    .ok{color:var(--ok)} .warn{color:var(--warn)} .bad{color:var(--bad)}
    .ref{margin-top:10px;word-break:break-all;background:rgba(82,200,255,.1);border:1px dashed rgba(82,200,255,.55);padding:10px;border-radius:10px}
    .btn{margin-top:8px;padding:9px 12px;border:1px solid var(--line);background:#1c3652;color:var(--text);border-radius:10px;cursor:pointer}
    .field{margin-top:10px}
    .field label{display:block;color:var(--muted);font-size:12px;margin-bottom:6px}
    .input,.select,.textarea{
      width:100%;background:#0f2033;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:10px;font-family:inherit
    }
    .textarea{min-height:120px;resize:vertical}
    .submit-result{margin-top:10px;font-size:13px;color:var(--muted)}
    .hidden{display:none!important}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="title">Личный кабинет</div>
      <div class="sub" id="hello">Загрузка данных...</div>
      <div class="grid" id="stats"></div>
      <div class="tabs">
        <button class="tab active" data-tab="overview">Обзор</button>
        <button class="tab" data-tab="submit">Сдать номера</button>
        <button class="tab" data-tab="numbers">Номера</button>
        <button class="tab" data-tab="withdrawals">Выводы</button>
        <button class="tab" data-tab="payouts">Выплаты</button>
        <button class="tab hidden" id="tabAdmin" data-tab="admin">Админ</button>
      </div>
    </div>
    <div class="panel" id="panel"></div>
  </div>
  <script>
    const tg = window.Telegram.WebApp;
    tg.ready(); tg.expand();
    let APP = null;
    let activeTab = 'overview';
    function esc(v){ return String(v ?? '').replace(/[&<>\"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[m])); }
    function card(label, value, mini=''){
      return `<div class="card"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div><div class="mini">${esc(mini)}</div></div>`;
    }
    function money(v){ return '$' + Number(v||0).toFixed(2); }
    function renderOverview(data){
      const p = data.profile, f = data.finance, q = data.queue, r = data.referrals;
      const refHtml = r.ref_link
        ? `<div class="ref">${esc(r.ref_link)}<br><button class="btn" id="copyRef">Скопировать реф-ссылку</button></div>`
        : `<div class="ref">Укажите BOT_USERNAME в .env, чтобы показывалась полная ссылка.</div>`;
      return `
        <div class="label">Финансы</div>
        <div class="row"><span>Доступно</span><span>${money(f.balance)}</span></div>
        <div class="row"><span>Запросы на вывод</span><span>${esc(f.withdrawals_total)}</span></div>
        <div class="row"><span class="ok">Оплачено</span><span class="ok">${esc(f.withdrawals_paid)} / ${money(f.withdrawals_paid_sum)}</span></div>
        <div class="row"><span class="warn">Ожидает</span><span class="warn">${esc(f.withdrawals_pending)}</span></div>
        <div class="field">
          <label>Запросить вывод ($)</label>
          <input class="input" id="withdrawAmount" placeholder="10.00" />
          <button class="btn" id="withdrawBtn">💵 Запросить вывод</button>
          <div class="submit-result" id="withdrawResult"></div>
        </div>
        <div class="row"><span>Рефералы</span><span>${esc(r.invited)}</span></div>
        <div class="mini">Регистрация: ${esc(p.created_at)} • Активность: ${esc(p.last_seen)}</div>
        ${refHtml}
      `;
    }
    function renderList(items, builder){
      if(!items || !items.length){
        return `<div class="mini">Пока пусто.</div>`;
      }
      return `<div class="list">${items.map(builder).join('')}</div>`;
    }
    function renderNumbers(data){
      return renderList(data.activity.numbers, n => `
        <div class="item">
          <div class="top"><b>${esc(n.phone)}</b><span>${esc(n.status)}</span></div>
          <div class="meta">Создан: ${esc(n.created_at)} • Завершен: ${esc(n.completed_at)}</div>
        </div>`);
    }
    function renderWithdrawals(data){
      return renderList(data.activity.withdrawals, w => `
        <div class="item">
          <div class="top"><b>${money(w.amount)}</b><span>${esc(w.status)}</span></div>
          <div class="meta">Создан: ${esc(w.created_at)} • Обновлен: ${esc(w.updated_at)}</div>
        </div>`);
    }
    function renderPayouts(data){
      return renderList(data.activity.payouts, p => `
        <div class="item">
          <div class="top"><b>${money(p.amount)}</b><span>выплата</span></div>
          <div class="meta">${esc(p.created_at)}${p.note ? ' • ' + esc(p.note) : ''}</div>
        </div>`);
    }
    function optionHtml(arr, valueKey, textBuilder){
      return (arr || []).map(x => `<option value="${esc(x[valueKey])}">${esc(textBuilder(x))}</option>`).join('');
    }
    function renderSubmit(data){
      const tariffs = data.submit_options.tariffs || [];
      if(!tariffs.length){
        return `<div class="mini">Нет активных тарифов. Обратитесь к администратору.</div>`;
      }
      return `
        <div class="label">Сдать номера в очередь</div>
        <div class="field">
          <label>Тариф</label>
          <select class="select" id="submitTariff">
            ${optionHtml(tariffs, 'id', t => `${t.name} | ${t.duration_min} мин | $${t.price}`)}
          </select>
        </div>
        <div class="field">
          <label>Номера (каждый с новой строки)</label>
          <textarea class="textarea" id="submitNumbers" placeholder="77071234567\n77771234567"></textarea>
        </div>
        <button class="btn" id="submitBtn">🚀 Отправить в очередь</button>
        <div class="submit-result" id="submitResult"></div>
      `;
    }
    async function submitNumbers(){
      const tariffSel = document.getElementById('submitTariff');
      const numbersEl = document.getElementById('submitNumbers');
      const out = document.getElementById('submitResult');
      if(!tariffSel || !numbersEl || !out) return;
      if(!numbersEl.value.trim()){
        out.textContent = 'Введите номера.';
        return;
      }
      out.textContent = 'Отправка...';
      const res = await fetch('/miniapp/api/submit', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({
          init_data: tg.initData || '',
          tariff_id: Number(tariffSel.value),
          numbers_text: numbersEl.value
        })
      });
      if(!res.ok){
        out.textContent = 'Ошибка отправки: ' + res.status;
        return;
      }
      const data = await res.json();
      if(!data.ok){
        out.textContent = data.error || 'Ошибка';
        return;
      }
      out.textContent = `Готово: ${data.accepted_count}. В очереди теперь: ${data.queue_after}. Пропущено: ${data.skipped_count || 0}.`;
      numbersEl.value = '';
      await load();
      setTab('numbers');
    }
    async function requestWithdraw(){
      const amountEl = document.getElementById('withdrawAmount');
      const out = document.getElementById('withdrawResult');
      if(!amountEl || !out) return;
      const val = amountEl.value.trim();
      if(!val){
        out.textContent = 'Введите сумму.';
        return;
      }
      out.textContent = 'Отправка запроса...';
      const res = await fetch('/miniapp/api/withdraw', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ init_data: tg.initData || '', amount: val })
      });
      if(!res.ok){
        out.textContent = 'Ошибка: ' + res.status;
        return;
      }
      const data = await res.json();
      if(!data.ok){
        out.textContent = data.error || 'Ошибка';
        return;
      }
      out.textContent = `Запрос #${data.request_id} на $${Number(data.amount).toFixed(2)} отправлен.`;
      amountEl.value = '';
      await load();
    }
    function renderAdmin(data){
      if(!data.admin || !data.admin.enabled){
        return `<div class="mini">Нет доступа.</div>`;
      }
      return `
        <div class="label">Админ-панель мини-аппа</div>
        <div class="row"><span>Запросов вывода (pending)</span><span>${esc(data.admin.pending_withdrawals)}</span></div>
        <div class="field">
          <label>Выдать выплату пользователю (@username или ID)</label>
          <input class="input" id="adminPayoutTarget" placeholder="@username или 123456789" />
        </div>
        <div class="field">
          <label>Сумма выплаты ($)</label>
          <input class="input" id="adminPayoutAmount" placeholder="8.00" />
        </div>
        <div class="field">
          <label>Комментарий (необязательно)</label>
          <input class="input" id="adminPayoutNote" placeholder="Отстой по номеру..." />
        </div>
        <button class="btn" id="adminPayoutBtn">➕ Выдать выплату</button>
        <div class="submit-result" id="adminPayoutResult"></div>
        <div class="label" style="margin-top:14px">Номера (последние 250)</div>
        ${renderList(data.admin.numbers, n => `
          <div class="item">
            <div class="top"><b>${esc(n.phone)}</b><span>${esc(n.status)}</span></div>
            <div class="meta">
              Тариф: ${esc(n.tariff_name)} (${esc(n.duration_min)} мин / $${esc(n.price)})<br>
              Кто сдал: ${esc(n.submitter_username ? '@' + n.submitter_username : 'ID ' + n.submitter_id)}<br>
              Отстоял: ${esc(n.stood_min)} мин | Зачет по тарифу: ${n.eligible_paid ? 'да' : 'нет'}<br>
              Создан: ${esc(n.created_at)} | Взят: ${esc(n.assigned_at)} | Завершен: ${esc(n.completed_at)}
            </div>
          </div>
        `)}
      `;
    }
    async function adminPayout(){
      const targetEl = document.getElementById('adminPayoutTarget');
      const amountEl = document.getElementById('adminPayoutAmount');
      const noteEl = document.getElementById('adminPayoutNote');
      const out = document.getElementById('adminPayoutResult');
      if(!targetEl || !amountEl || !out) return;
      const target = targetEl.value.trim();
      const amount = amountEl.value.trim();
      if(!target || !amount){
        out.textContent = 'Заполни пользователя и сумму.';
        return;
      }
      out.textContent = 'Выдача выплаты...';
      const res = await fetch('/miniapp/api/admin/payout', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          init_data: tg.initData || '',
          target,
          amount,
          note: noteEl ? noteEl.value.trim() : ''
        })
      });
      if(!res.ok){
        out.textContent = 'Ошибка: ' + res.status;
        return;
      }
      const data = await res.json();
      if(!data.ok){
        out.textContent = data.error || 'Ошибка';
        return;
      }
      out.textContent = `Готово: выдано $${Number(data.amount).toFixed(2)} пользователю ID ${data.target_user_id}`;
      amountEl.value = '';
      if(noteEl) noteEl.value = '';
      await load();
    }
    function renderTab(){
      if(!APP) return;
      const panel = document.getElementById('panel');
      if(activeTab === 'overview') panel.innerHTML = renderOverview(APP);
      if(activeTab === 'submit') panel.innerHTML = renderSubmit(APP);
      if(activeTab === 'numbers') panel.innerHTML = `<div class="label">Последние номера</div>${renderNumbers(APP)}`;
      if(activeTab === 'withdrawals') panel.innerHTML = `<div class="label">История выводов</div>${renderWithdrawals(APP)}`;
      if(activeTab === 'payouts') panel.innerHTML = `<div class="label">История выплат</div>${renderPayouts(APP)}`;
      if(activeTab === 'admin') panel.innerHTML = renderAdmin(APP);
      const copyBtn = document.getElementById('copyRef');
      if(copyBtn){
        copyBtn.onclick = async () => {
          try{ await navigator.clipboard.writeText(APP.referrals.ref_link || ''); copyBtn.textContent = 'Скопировано'; }catch(_){}
        };
      }
      const withdrawBtn = document.getElementById('withdrawBtn');
      if(withdrawBtn){
        withdrawBtn.onclick = requestWithdraw;
      }
      const adminPayoutBtn = document.getElementById('adminPayoutBtn');
      if(adminPayoutBtn){
        adminPayoutBtn.onclick = adminPayout;
      }
      if(activeTab === 'submit'){
        const submitBtn = document.getElementById('submitBtn');
        if(submitBtn){
          submitBtn.onclick = submitNumbers;
        }
      }
    }
    function setTab(tab){
      activeTab = tab;
      document.querySelectorAll('.tab').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tab));
      renderTab();
    }
    async function load(){
      const initData = tg.initData || '';
      if(!initData){
        document.getElementById('hello').textContent = 'Не удалось авторизоваться в WebApp.';
        return;
      }
      const res = await fetch('/miniapp/api/me', {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({init_data: initData})
      });
      if(!res.ok){
        document.getElementById('hello').textContent = 'Ошибка загрузки: ' + res.status;
        return;
      }
      APP = await res.json();
      const p = APP.profile, f = APP.finance, q = APP.queue, r = APP.referrals;
      const who = p.username ? '@' + p.username : (p.full_name || ('ID ' + p.user_id));
      document.getElementById('hello').textContent = `${who}`;
      document.getElementById('stats').innerHTML = [
        card('Баланс', money(f.balance), `Выплаты: ${f.payouts_count} / ${money(f.payouts_total)}`),
        card('Сдано', q.submitted, `Встал: ${q.success} • Слет: ${q.slip} • Ошибка: ${q.error}`),
        card('Отстояло', q.stood_count, `Начислено: ${money(q.stood_amount)}`),
        card('Success rate', q.success_rate, `Отменено: ${q.canceled}`),
        card('Рефералы', r.invited, `Код: ${r.ref_code}`)
      ].join('');
      const tabAdmin = document.getElementById('tabAdmin');
      if(tabAdmin){
        const isAdmin = !!(APP.admin && APP.admin.enabled);
        tabAdmin.classList.toggle('hidden', !isAdmin);
        if(!isAdmin && activeTab === 'admin'){
          activeTab = 'overview';
        }
      }
      renderTab();
    }
    document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => setTab(btn.dataset.tab)));
    load();
  </script>
</body>
</html>"""
