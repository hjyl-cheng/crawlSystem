function h(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function loginPage({ returnTo, error = "", loggedOut = false }) {
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>登录 - QY 控制台</title>
  <style>
    :root { color-scheme:light; --bg:#e9eeeb; --panel:#fff; --line:#dbe6df; --text:#101713; --muted:#6f7d74; --primary:#127849; --dark:#0b4d32; --red:#b52d2d; }
    * { box-sizing:border-box; }
    body { margin:0; min-height:100vh; display:grid; place-items:center; padding:24px; background:var(--bg); color:var(--text); font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; }
    main { width:min(420px,100%); padding:32px; background:var(--panel); border:1px solid var(--line); border-radius:8px; box-shadow:0 18px 42px rgba(20,35,28,.10); }
    .brand { display:flex; align-items:center; gap:12px; margin-bottom:28px; }
    .mark { width:42px; height:42px; display:grid; place-items:center; border:3px solid var(--primary); border-radius:50%; color:var(--primary); font-size:11px; font-weight:850; }
    h1 { margin:0; font-size:24px; letter-spacing:0; }
    .sub { margin-top:4px; color:var(--muted); }
    form { display:grid; gap:18px; }
    label { display:grid; gap:7px; color:var(--muted); font-size:12px; font-weight:800; }
    input { width:100%; min-height:44px; padding:10px 12px; border:1px solid var(--line); border-radius:6px; background:#fff; color:var(--text); font:inherit; outline:none; }
    input:focus { border-color:var(--primary); box-shadow:0 0 0 3px rgba(18,120,73,.12); }
    button { min-height:44px; border:1px solid var(--primary); border-radius:6px; background:var(--dark); color:#fff; font:inherit; font-weight:800; cursor:pointer; }
    button:hover { background:var(--primary); }
    .message { margin-bottom:18px; padding:10px 12px; border-radius:6px; background:#edf7f1; color:var(--dark); }
    .error { background:#fff0f0; color:var(--red); }
  </style>
</head>
<body>
  <main>
    <div class="brand"><div class="mark">QY</div><div><h1>QY 控制台</h1><div class="sub">管理员登录</div></div></div>
    ${loggedOut ? '<div class="message">已退出登录</div>' : ""}
    ${error ? `<div class="message error">${h(error)}</div>` : ""}
    <form method="post" action="/auth/login" autocomplete="on">
      <input type="hidden" name="return_to" value="${h(returnTo)}">
      <label>账号<input name="username" autocomplete="username" required maxlength="128" autofocus></label>
      <label>密码<input name="password" type="password" autocomplete="current-password" required maxlength="1024"></label>
      <button type="submit">登录</button>
    </form>
  </main>
</body>
</html>`;
}
