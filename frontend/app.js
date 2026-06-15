const BASE = '/codedoc';
const { createApp, ref, reactive, onMounted, nextTick, watch } = Vue;

createApp({
  setup() {
    const token = ref(localStorage.getItem('cw_token') || '');
    const username = ref(localStorage.getItem('cw_username') || '');
    const sidebarOpen = ref(false);
    const authMode = ref('login');
    const authForm = reactive({ username: '', password: '' });
    const authLoading = ref(false);
    const authError = ref('');

    const toasts = ref([]);
    function toast(msg, type='info') {
      const t = { msg, type };
      toasts.value.push(t);
      setTimeout(() => { const i = toasts.value.indexOf(t); if (i>=0) toasts.value.splice(i,1); }, 3500);
    }

    async function api(path, opts={}) {
      const headers = { 'Content-Type': 'application/json', ...(opts.headers||{}) };
      if (token.value) headers['Authorization'] = 'Bearer ' + token.value;
      const res = await fetch(BASE + path, { ...opts, headers });
      if (res.status === 401) {
        token.value = ''; localStorage.removeItem('cw_token');
        toast('会话已过期，请重新登录', 'error');
        throw new Error('unauthorized');
      }
      let data;
      try { data = await res.json(); } catch { data = {}; }
      if (!res.ok) { const msg = data.detail || data.error || '请求失败'; toast(msg, 'error'); throw new Error(msg); }
      return data;
    }

    async function doAuth() {
      authError.value = '';
      if (!authForm.username || !authForm.password) { authError.value = '请填写用户名和密码'; return; }
      authLoading.value = true;
      try {
        const endpoint = authMode.value === 'login' ? '/api/v1/auth/login' : '/api/v1/auth/register';
        const data = await api(endpoint, { method: 'POST', body: JSON.stringify(authForm) });
        token.value = data.token;
        username.value = data.username;
        localStorage.setItem('cw_token', data.token);
        localStorage.setItem('cw_username', data.username);
        toast(authMode.value === 'login' ? '欢迎回来 · ' + data.username : '注册成功 · 欢迎使用', 'success');
        await loadRepos();
      } catch(e) { authError.value = e.message; }
      authLoading.value = false;
    }
    function logout() {
      token.value = '';
      username.value = '';
      localStorage.removeItem('cw_token');
      localStorage.removeItem('cw_username');
      repos.value = [];
      currentRepo.value = null;
    }

    // Repos
    const repos = ref([]);
    const reposLoading = ref(false);
    const showAddModal = ref(false);
    const addTab = ref('github');
    const addUrl = ref('');
    const addLoading = ref(false);
    // Upload state
    const uploadName = ref('');
    const uploadFile = ref(null);
    const uploadLoading = ref(false);
    const dragOver = ref(false);
    const fileInput = ref(null);
    let pollTimer = null;

    function closeAddModal() {
      showAddModal.value = false;
      // Reset transient state when closed (keep tab choice for UX)
      addUrl.value = '';
      uploadFile.value = null;
      uploadName.value = '';
      dragOver.value = false;
    }
    function onFilePicked(e) {
      const f = e.target.files && e.target.files[0];
      if (f) acceptFile(f);
      e.target.value = '';
    }
    function onFileDropped(e) {
      dragOver.value = false;
      const f = e.dataTransfer.files && e.dataTransfer.files[0];
      if (f) acceptFile(f);
    }
    function acceptFile(f) {
      if (!/\.zip$/i.test(f.name)) { toast('请选择 .zip 文件', 'error'); return; }
      if (f.size > 50 * 1024 * 1024) { toast('文件大小不能超过 50MB', 'error'); return; }
      uploadFile.value = f;
      if (!uploadName.value.trim()) {
        const base = f.name.replace(/\.zip$/i, '');
        uploadName.value = base;
      }
    }
    function humanSize(n) {
      if (n < 1024) return n + ' B';
      if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
      return (n / 1024 / 1024).toFixed(2) + ' MB';
    }

    async function doUploadRepo() {
      if (!uploadFile.value) { toast('请先选择 ZIP 文件', 'error'); return; }
      const rawName = uploadName.value.trim();
      if (!rawName) { toast('请输入仓库名称', 'error'); return; }
      uploadLoading.value = true;
      try {
        const fd = new FormData();
        fd.append('file', uploadFile.value);
        fd.append('name', rawName);
        const headers = {};
        if (token.value) headers['Authorization'] = 'Bearer ' + token.value;
        const res = await fetch(BASE + '/api/v1/repos/upload', { method: 'POST', body: fd, headers });
        let data; try { data = await res.json(); } catch { data = {}; }
        if (!res.ok) { throw new Error(data.detail || data.error || '上传失败'); }
        toast('已开始索引 ' + (data.name || rawName), 'success');
        closeAddModal();
        await loadRepos();
      } catch(e) {
        toast(e.message || '上传失败', 'error');
      }
      uploadLoading.value = false;
    }

    async function loadRepos() {
      reposLoading.value = true;
      try {
        const data = await api('/api/v1/repos');
        repos.value = data.repos || [];
        // refresh currentRepo from list
        if (currentRepo.value) {
          const fresh = repos.value.find(r => r.name === currentRepo.value.name);
          if (fresh) currentRepo.value = fresh;
        }
        checkPolling();
      } catch(e) {}
      reposLoading.value = false;
    }

    function checkPolling() {
      const needsPoll = repos.value.some(r => r.status === 'cloning' || r.status === 'indexing');
      if (needsPoll && !pollTimer) {
        pollTimer = setInterval(async () => {
          try {
            const data = await api('/api/v1/repos');
            repos.value = data.repos || [];
            if (currentRepo.value) {
              const fresh = repos.value.find(r => r.name === currentRepo.value.name);
              if (fresh) currentRepo.value = fresh;
            }
            const still = repos.value.some(r => r.status === 'cloning' || r.status === 'indexing');
            if (!still) { clearInterval(pollTimer); pollTimer = null; }
          } catch(e) { clearInterval(pollTimer); pollTimer = null; }
        }, 3000);
      } else if (!needsPoll && pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    async function doAddRepo() {
      if (!addUrl.value.trim()) { toast('请输入 GitHub 地址', 'error'); return; }
      addLoading.value = true;
      try {
        const data = await api('/api/v1/repos', { method: 'POST', body: JSON.stringify({ url: addUrl.value.trim() }) });
        toast('已开始导入 ' + data.name, 'success');
        showAddModal.value = false;
        addUrl.value = '';
        await loadRepos();
      } catch(e) {}
      addLoading.value = false;
    }

    async function deleteRepo(r) {
      if (!confirm('确定删除仓库 ' + r.name + '？')) return;
      try {
        await api('/api/v1/repos/' + r.name, { method: 'DELETE' });
        toast('已删除 ' + r.name, 'success');
        if (currentRepo.value && currentRepo.value.name === r.name) {
          currentRepo.value = null;
          currentTab.value = 'overview';
        }
        await loadRepos();
      } catch(e) {}
    }

    function statusText(s) {
      return ({ pending:'等待中', cloning:'克隆中', indexing:'索引中', ready:'已就绪', error:'失败' })[s] || s;
    }
    function shortRepoName(name){
      const parts = (name||'').split('/');
      return parts.length===2 ? parts[1] : name;
    }
    function formatDate(s) {
      if (!s) return '';
      try {
        const d = new Date(s.replace(' ', 'T') + 'Z');
        const now = new Date();
        const diff = now - d;
        if (diff < 60000) return '刚刚';
        if (diff < 3600000) return Math.floor(diff/60000) + ' 分钟前';
        if (diff < 86400000) return Math.floor(diff/3600000) + ' 小时前';
        return d.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' });
      } catch { return s; }
    }
    function fmtTime(d=new Date()){
      const hh = String(d.getHours()).padStart(2,'0');
      const mm = String(d.getMinutes()).padStart(2,'0');
      return hh+':'+mm;
    }

    // Selection
    const currentRepo = ref(null);
    const currentTab = ref('overview');

    function selectRepo(r, tab) {
      currentRepo.value = r;
      currentTab.value = tab || 'overview';
      // Auto-close mobile drawer after selection
      if (window.innerWidth <= 768) sidebarOpen.value = false;
      if (tab === 'qa') {
        if (chatMessages.value.length === 0) {
          chatMessages.value.push({
            role: 'bot',
            content: '你好！我是代码文档助手，可以基于这个仓库的知识图谱回答你的问题。\n\n试着问我：\n  · 这个项目的核心模块有哪些？\n  · 路由是怎么注册的？\n  · X 函数的调用关系是什么？',
            time: fmtTime()
          });
        }
        nextTick(() => { if (chatBox.value) chatBox.value.scrollTop = chatBox.value.scrollHeight; });
      }
      if (tab === 'doc') {
        loadUserDocs();
        if (!Object.keys(docTemplates.value).length) loadDocTemplates();
        // Re-attach to a running docgen for this repo (if any), or auto-load
        // the most recent completed doc — so user sees progress / cached doc
        // even after navigating away and back.
        var running = docgenTasks.value.find(function(t){
          return t.repo === r.name && (t.status === 'queued' || t.status === 'running');
        });
        if (running) {
          docTaskId.value = running.task_id;
          docLoading.value = true;
          docContent.value = '';
          docStage.value = DOC_STAGE_LABELS[running.stage] || running.stage || '处理中';
          docProgress.value = running.progress || '';
          pollDocgenTask(running.task_id);
        } else {
          var lastDone = docgenTasks.value.find(function(t){
            return t.repo === r.name && t.status === 'done';
          });
          if (lastDone && (!docContent.value || docTaskId.value !== lastDone.task_id)) {
            docTaskId.value = lastDone.task_id;
            renderDocFromTask(lastDone.task_id);
          }
        }
      }
    }

    // Navigate to a non-repo top-level page (e.g. 'tasks').
    function goPage(page) {
      currentTab.value = page;
      currentRepo.value = null;
      if (window.innerWidth <= 768) sidebarOpen.value = false;
      if (page === 'tasks') loadDocgenTasks();
    }

    // Doc
    const docContent = ref('');
    const docLoading = ref(false);
    const docStage = ref('');      // human-readable stage label, e.g. "生成项目概览"
    const docProgress = ref('');   // secondary detail line, e.g. "分析模块 2/5: foo.bar"
    const docTaskId = ref('');     // active async docgen task id
    const docTemplates = ref({});
    const sectionLabels = ref({});
    const docTemplate = ref('default');
    const customSections = ref([]);

    async function loadDocTemplates(){
      if (!token.value) return;
      try {
        const data = await api('/api/v1/docgen/templates');
        docTemplates.value = data.templates || {};
        sectionLabels.value = data.sections || {};
        if (docTemplates.value[docTemplate.value] && Array.isArray(docTemplates.value[docTemplate.value].sections)) {
          customSections.value = [...docTemplates.value[docTemplate.value].sections];
        }
      } catch(e) { /* silent */ }
    }

    function onTemplateChange(){
      var t = docTemplates.value[docTemplate.value];
      if (t && Array.isArray(t.sections)) {
        customSections.value = [...t.sections];
      } else if (docTemplate.value === 'custom' && customSections.value.length === 0) {
        customSections.value = Object.keys(sectionLabels.value);
      }
    }

    // ---- 我的任务 (per-user docgen task list) ----------------------------
    const docgenTasks = ref([]);
    let _taskPollInterval = null;

    const TASK_STATUS_LABELS = {
      queued: '排队中', running: '生成中', done: '已完成', error: '失败',
    };

    function runningTaskCount() {
      return docgenTasks.value.filter(function(t){
        return t.status === 'queued' || t.status === 'running';
      }).length;
    }

    function fmtTaskTime(ts) {
      if (!ts) return '';
      try {
        var d = new Date(ts * 1000);
        var now = new Date();
        var diff = now - d;
        if (diff < 60000) return '刚刚';
        if (diff < 3600000) return Math.floor(diff/60000) + ' 分钟前';
        if (diff < 86400000) return Math.floor(diff/3600000) + ' 小时前';
        return d.toLocaleString('zh-CN', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      } catch(e) { return ''; }
    }

    function taskTemplateLabel(t) {
      var tmpl = docTemplates.value[t.template];
      return (tmpl && tmpl.name) || t.template || '';
    }

    async function loadDocgenTasks() {
      if (!token.value) return;
      try {
        var r = await api('/api/v1/docgen/tasks');
        docgenTasks.value = r.tasks || [];
        // Re-attach pollers for any tasks still in flight
        docgenTasks.value.forEach(function(t){
          if (t.status === 'queued' || t.status === 'running') {
            pollDocgenTask(t.task_id);
          }
        });
      } catch(e) { /* ignore */ }
    }

    function startTaskAutoReload() {
      if (_taskPollInterval) clearInterval(_taskPollInterval);
      _taskPollInterval = setInterval(function(){
        if (!token.value) return;
        var hasPending = docgenTasks.value.some(function(t){
          return t.status === 'queued' || t.status === 'running';
        });
        if (hasPending || currentTab.value === 'tasks') {
          loadDocgenTasks();
        }
      }, 5000);
    }

    async function viewTaskDocument(task) {
      var repo = repos.value.find(function(r){ return r.name === task.repo; });
      docTaskId.value = task.task_id;
      docContent.value = '';
      docLoading.value = true;
      docStage.value = '加载文档...';
      docProgress.value = '';
      if (repo) {
        currentRepo.value = repo;
      }
      currentTab.value = 'doc';
      if (window.innerWidth <= 768) sidebarOpen.value = false;
      await renderDocFromTask(task.task_id);
    }

    // User-uploaded reference docs
    const userDocs = ref([]);
    const userDocUploading = ref(false);
    const docDragOver = ref(false);

    async function loadUserDocs(){
      if (!currentRepo.value) { userDocs.value = []; return; }
      try {
        var data = await api('/api/v1/repos/' + currentRepo.value.name + '/docs');
        userDocs.value = data.docs || [];
      } catch(e) { userDocs.value = []; }
    }

    async function uploadUserDocFile(file){
      if (!currentRepo.value || !file) return;
      var fd = new FormData();
      fd.append('file', file);
      userDocUploading.value = true;
      try {
        var r = await fetch((window.__API_BASE__||'') + '/api/v1/repos/' + currentRepo.value.name + '/docs/upload', {
          method: 'POST',
          headers: { 'Authorization': 'Bearer ' + token.value },
          body: fd,
        });
        var data = await r.json();
        if (!r.ok) { toast(data.detail || '上传失败', 'error'); }
        else { toast('已上传 ' + data.filename, 'success'); await loadUserDocs(); }
      } catch(e) { toast('上传失败: ' + e.message, 'error'); }
      userDocUploading.value = false;
    }

    function onPickUserDoc(ev){
      var f = ev.target.files && ev.target.files[0];
      if (f) uploadUserDocFile(f);
      ev.target.value = '';
    }

    function onDropUserDoc(ev){
      docDragOver.value = false;
      var f = ev.dataTransfer && ev.dataTransfer.files && ev.dataTransfer.files[0];
      if (f) uploadUserDocFile(f);
    }

    async function deleteUserDoc(d){
      if (!confirm('删除文档 ' + d.filename + '？')) return;
      try {
        await api('/api/v1/repos/' + currentRepo.value.name + '/docs/' + d.id, { method: 'DELETE' });
        toast('已删除', 'success');
        await loadUserDocs();
      } catch(e) { toast('删除失败: ' + e.message, 'error'); }
    }

    function renderDoc(raw) {
      if (!raw) return '';
      let html = raw;
      // If looks like HTML already, just keep; otherwise process markdown lite
      if (!/<h[1-6]|<p|<ul|<table/i.test(html)) {
        // basic markdown to html
        html = html
          .replace(/```([\s\S]*?)```/g, (m, code) => '<pre><code>' + code.replace(/</g,'&lt;') + '</code></pre>')
          .replace(/^### (.*)$/gm, '<h3>$1</h3>')
          .replace(/^## (.*)$/gm, '<h2>$1</h2>')
          .replace(/^# (.*)$/gm, '<h2>$1</h2>')
          .replace(/^\* (.*)$/gm, '<li>$1</li>')
          .replace(/^- (.*)$/gm, '<li>$1</li>')
          .replace(/(<li>.*<\/li>\n?)+/g, m => '<ul>' + m + '</ul>')
          .replace(/`([^`]+)`/g, '<code>$1</code>')
          .replace(/\n\n/g, '</p><p>')
          .replace(/^(?!<)(.*)$/gm, '<p>$1</p>')
          .replace(/<p><\/p>/g, '')
          .replace(/<p>(<h[1-6])/g,'$1').replace(/(<\/h[1-6]>)<\/p>/g,'$1')
          .replace(/<p>(<ul)/g,'$1').replace(/(<\/ul>)<\/p>/g,'$1')
          .replace(/<p>(<pre)/g,'$1').replace(/(<\/pre>)<\/p>/g,'$1');
      }
      return html;
    }

    // Human-readable labels for backend stage codes
    const DOC_STAGE_LABELS = {
      queued: '排队中',
      loading: '加载仓库索引',
      overview: '生成项目概览',
      arch_diagram: '绘制系统架构图',
      class_uml: '绘制核心类 UML',
      call_flow: '绘制调用关系图',
      modules: '分析关键模块',
      routes: '整理接口文档',
      recommendations: '生成推荐问答',
      finalizing: '整理文档',
      done: '完成',
      error: '出错',
    };

    function renderDocMarkdown(md) {
      // Replace ASK markers with clickable buttons
      md = md.replace(/- ASK::(.+)/g, function(_, q){
        var safe = q.replace(/"/g, '&quot;').trim();
        return '<div class="ask-item"><button class="ask-btn" data-q="' + safe + '">问这个 →</button> ' + safe + '</div>';
      });
      if (window.marked && marked.parse) return marked.parse(md);
      return renderDoc(md);
    }

    // Fire-and-forget: submit task and immediately return control. A non-blocking
    // background poller updates docgenTasks state and (if user stays on this doc
    // tab for this task) updates docContent when done.
    async function doGenDoc() {
      if (!currentRepo.value) return;
      try {
        var payload = { repo: currentRepo.value.name, template: docTemplate.value };
        if (docTemplate.value === 'custom') {
          payload.sections = customSections.value;
        }
        const submit = await api('/api/v1/docgen', { method: 'POST', body: JSON.stringify(payload) });
        if (submit.error) {
          toast('生成失败: ' + submit.error, 'error');
          return;
        }
        var tName = (docTemplates.value[docTemplate.value] && docTemplates.value[docTemplate.value].name) || '完整文档';
        toast('任务已提交（' + tName + '），可继续操作其他功能', 'success');

        // Optimistically attach to this task on the doc tab so the user sees
        // progress immediately — but the polling is NON-BLOCKING.
        docTaskId.value = submit.task_id;
        docLoading.value = true;
        docContent.value = '';
        docStage.value = '排队中（第 ' + (submit.position || 1) + ' 位）';
        docProgress.value = '';

        await loadDocgenTasks();          // surface the new task in 我的任务
        pollDocgenTask(submit.task_id);   // fire-and-forget; does not await
      } catch(e) {
        toast('提交失败: ' + e.message, 'error');
      }
    }

    // Background poller — never blocks the UI. Updates docgenTasks state and,
    // if the user is currently viewing this task on the doc tab, updates the
    // doc content + stage labels too. Exits when status reaches done/error,
    // when the user logs out, or when polling errors out.
    const docgenPollers = ref({});  // task_id -> true while a poller is active
    async function pollDocgenTask(taskId) {
      if (docgenPollers.value[taskId]) return;  // already polling
      docgenPollers.value[taskId] = true;
      try {
        while (true) {
          await new Promise(function(r){ setTimeout(r, 3000); });
          if (!token.value) return;
          var s;
          try {
            s = await api('/api/v1/docgen/' + taskId + '/status');
          } catch(e) {
            return;
          }
          // Update the matching row in docgenTasks (if loaded)
          var idx = docgenTasks.value.findIndex(function(t){ return t.task_id === taskId; });
          if (idx >= 0) {
            docgenTasks.value[idx].status = s.status;
            docgenTasks.value[idx].stage = s.stage;
            docgenTasks.value[idx].progress = s.progress;
            docgenTasks.value[idx].position = s.position || 0;
            docgenTasks.value[idx].error = s.error || '';
          }
          // If the user is currently looking at this task's doc tab, update the live progress display
          if (docTaskId.value === taskId) {
            if (s.status === 'queued') {
              docStage.value = '排队中（第 ' + s.position + ' 位 / 共 ' + s.queue_total + ' 个任务）';
              docProgress.value = '等待前面的任务完成...';
            } else if (s.status === 'running') {
              docStage.value = DOC_STAGE_LABELS[s.stage] || s.stage || '处理中';
              docProgress.value = s.progress || '';
            }
          }
          if (s.status === 'done') {
            toast(((docgenTasks.value[idx] && docgenTasks.value[idx].repo) || '') + ' 文档已生成', 'success');
            if (docTaskId.value === taskId) {
              await renderDocFromTask(taskId);
            }
            return;
          }
          if (s.status === 'error') {
            toast('生成失败: ' + (s.error || '未知错误'), 'error');
            if (docTaskId.value === taskId) {
              docContent.value = '<p style="color:#ff3b30">生成失败: ' + (s.error || '未知错误') + '</p>';
              docStage.value = '';
              docProgress.value = '';
              docLoading.value = false;
            }
            return;
          }
        }
      } finally {
        delete docgenPollers.value[taskId];
      }
    }

    // Load the cached document for a task and render it into the doc pane.
    async function renderDocFromTask(taskId) {
      try {
        var r = await api('/api/v1/docgen/' + taskId + '/document');
        if (r.status !== 'done' || !r.document) {
          // still in progress — show stage if attached
          if (docTaskId.value === taskId) {
            docStage.value = DOC_STAGE_LABELS[r.stage] || r.stage || '处理中';
            docProgress.value = r.progress || '';
          }
          return;
        }
        docContent.value = renderDocMarkdown(r.document);
        docLoading.value = false;
        docStage.value = '';
        docProgress.value = '';
        await nextTick();
        document.querySelectorAll('.doc-body .ask-btn').forEach(function(btn){
          btn.onclick = function(){
            var q = btn.dataset.q || '';
            currentTab.value = 'qa';
            chatInput.value = q;
            nextTick().then(function(){ doAsk(); });
          };
        });
      } catch(e) {
        toast('加载文档失败: ' + e.message, 'error');
      }
    }

    // Chat
    const chatMessages = ref([]);
    const expandedInvalid = ref({});
    const chatInput = ref('');
    const chatLoading = ref(false);
    const chatBox = ref(null);

    async function doAsk() {
      const q = chatInput.value.trim();
      if (!q || !currentRepo.value || chatLoading.value) return;
      chatMessages.value.push({ role: 'me', content: q, time: fmtTime() });
      chatInput.value = '';
      chatLoading.value = true;
      await nextTick();
      if (chatBox.value) chatBox.value.scrollTop = chatBox.value.scrollHeight;
      try {
        const data = await api('/api/v1/ask', { method: 'POST', body: JSON.stringify({ question: q, repo: currentRepo.value.name }) });
        chatMessages.value.push({
          role: 'bot',
          content: data.answer || '暂无回答',
          time: fmtTime(),
          groundedness: data.groundedness,
          total_refs: data.total_refs,
          valid_refs_count: data.valid_refs_count,
          invalid_refs: data.invalid_refs || [],
        });
      } catch(e) {
        chatMessages.value.push({ role: 'bot', content: '请求失败: ' + e.message, time: fmtTime() });
      }
      chatLoading.value = false;
      await nextTick();
      if (chatBox.value) chatBox.value.scrollTop = chatBox.value.scrollHeight;
    }

    // Reset chat when switching repo. Doc state is intentionally NOT reset
    // here — selectRepo() takes care of re-attaching to a running task or
    // auto-loading the most recent completed doc for the new repo.
    watch(currentRepo, (val, old) => {
      if (!val || !old || val.name !== old.name) {
        chatMessages.value = [];
        userDocs.value = [];
        // Clear the doc pane only if there's no in-flight task for the new repo
        // and no cached doc to show; selectRepo() will repopulate when known.
        var hasRunning = val && docgenTasks.value.some(function(t){
          return t.repo === val.name && (t.status === 'queued' || t.status === 'running');
        });
        var hasDone = val && docgenTasks.value.some(function(t){
          return t.repo === val.name && t.status === 'done';
        });
        if (!hasRunning && !hasDone) {
          docContent.value = '';
          docStage.value = '';
          docProgress.value = '';
          docTaskId.value = '';
          docLoading.value = false;
        }
        if (val) loadUserDocs();
      }
    });

    onMounted(() => {
      if (token.value) {
        loadRepos();
        loadDocTemplates();
        loadDocgenTasks();
        startTaskAutoReload();
      }
    });

    // Whenever the user logs in/out, refresh tasks + auto-poll state.
    watch(token, function(v){
      if (v) {
        loadDocgenTasks();
        startTaskAutoReload();
      } else if (_taskPollInterval) {
        clearInterval(_taskPollInterval);
        _taskPollInterval = null;
        docgenTasks.value = [];
      }
    });
    function groundednessClass(g) {
      if (g >= 0.85) return 'gnd-high';
      if (g >= 0.6) return 'gnd-mid';
      return 'gnd-low';
    }
    function groundednessTitle(m) {
      const v = m.valid_refs_count || 0;
      const t = m.total_refs || 0;
      const missing = (m.invalid_refs || []).length;
      return `共 ${t} 个代码引用，其中 ${v} 个已在知识图谱中验证` + (missing ? `，${missing} 个未匹配` : '');
    }
    function toggleInvalidRefs(i) {
      expandedInvalid.value = { ...expandedInvalid.value, [i]: !expandedInvalid.value[i] };
    }



    return {
      token, username, sidebarOpen, authMode, authForm, authLoading, authError, doAuth, logout,
      toasts,
      repos, reposLoading, showAddModal, addTab, addUrl, addLoading,
      uploadName, uploadFile, uploadLoading, dragOver, fileInput,
      closeAddModal, onFilePicked, onFileDropped, doUploadRepo, humanSize,
      doAddRepo, deleteRepo, statusText, shortRepoName, formatDate,
      currentRepo, currentTab, selectRepo, goPage,
      docContent, docLoading, docStage, docProgress, doGenDoc,
      docTemplates, sectionLabels, docTemplate, customSections, onTemplateChange,
      userDocs, userDocUploading, docDragOver,
      onPickUserDoc, onDropUserDoc, deleteUserDoc,
      chatMessages, expandedInvalid, groundednessClass, groundednessTitle, toggleInvalidRefs, chatInput, chatLoading, chatBox, doAsk,
      docgenTasks, runningTaskCount, viewTaskDocument,
      TASK_STATUS_LABELS, DOC_STAGE_LABELS, fmtTaskTime, taskTemplateLabel,
    };
  }
}).mount('#app');
