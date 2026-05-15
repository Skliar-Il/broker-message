package admin

const uiHTML = `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<title>broker-message admin</title>
<style>
body{font-family:system-ui,sans-serif;margin:2rem;background:#0f1419;color:#e6edf3}
h1{color:#58a6ff} section{margin:1.5rem 0;padding:1rem;border:1px solid #30363d;border-radius:8px}
button{background:#238636;color:#fff;border:none;padding:.5rem 1rem;border-radius:6px;cursor:pointer;margin:.25rem}
button.danger{background:#da3633} pre{background:#161b22;padding:1rem;overflow:auto;max-height:300px}
input{padding:.4rem;margin:.25rem}
#login{display:block} #app{display:none}
</style>
</head>
<body>
<div id="login">
<h2>Login</h2>
<input id="user" placeholder="user" value="admin"/>
<input id="pass" type="password" placeholder="password" value="admin"/>
<button onclick="doLogin()">Sign in</button>
</div>
<div id="app">
<h1>broker-message</h1>
<section><h3>State</h3><pre id="state"></pre><button onclick="refresh()">Refresh</button></section>
<section><h3>Clients</h3><pre id="clients"></pre></section>
<section><h3>Topics</h3><pre id="topics"></pre></section>
<section><h3>DB</h3><pre id="db"></pre></section>
<section class="danger"><h3>Danger zone</h3>
<button class="danger" onclick="restart()">Graceful restart MQTT</button>
</section>
</div>
<script>
let csrf='';
async function doLogin(){
  const u=document.getElementById('user').value;
  const p=document.getElementById('pass').value;
  const r=await fetch('/api/login',{method:'POST',headers:{'Authorization':'Basic '+btoa(u+':'+p)}});
  const j=await r.json(); csrf=j.csrf;
  document.getElementById('login').style.display='none';
  document.getElementById('app').style.display='block';
  refresh();
}
async function api(path,opts={}){
  opts.headers=opts.headers||{};
  if(csrf) opts.headers['X-CSRF-Token']=csrf;
  const r=await fetch(path,opts);
  return r.json();
}
async function refresh(){
  document.getElementById('state').textContent=JSON.stringify(await api('/api/state'),null,2);
  document.getElementById('clients').textContent=JSON.stringify(await api('/api/clients'),null,2);
  document.getElementById('topics').textContent=JSON.stringify(await api('/api/topics'),null,2);
  document.getElementById('db').textContent=JSON.stringify(await api('/api/db'),null,2);
}
async function restart(){ await api('/api/admin/restart',{method:'POST'}); alert('restart requested'); }
</script>
</body>
</html>`