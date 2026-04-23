pub fn index_html() -> &'static str {
    r#"<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>perp-arb admin</title>
    <style>
      :root {
        --bg: #f4efe4;
        --panel: #fffaf0;
        --ink: #242019;
        --muted: #6d6252;
        --line: #d8ccb7;
        --accent: #0f766e;
        --accent-dark: #134e4a;
        --danger: #b42318;
        --code: #102a43;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        color: var(--ink);
        font-family: Georgia, "Times New Roman", serif;
        background:
          radial-gradient(circle at 15% 15%, rgba(15, 118, 110, 0.16), transparent 30rem),
          linear-gradient(135deg, #f8f0df 0%, var(--bg) 45%, #e7dcc7 100%);
      }

      main {
        width: min(1180px, calc(100% - 32px));
        margin: 0 auto;
        padding: 36px 0 56px;
      }

      h1 {
        margin: 0 0 8px;
        font-size: clamp(32px, 6vw, 64px);
        letter-spacing: -0.05em;
      }

      h2 {
        margin: 0 0 14px;
        font-size: 22px;
      }

      p {
        color: var(--muted);
      }

      .hero {
        display: grid;
        gap: 12px;
        margin-bottom: 24px;
      }

      .grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 18px;
      }

      .panel {
        border: 1px solid var(--line);
        border-radius: 22px;
        background: rgba(255, 250, 240, 0.88);
        box-shadow: 0 22px 80px rgba(36, 32, 25, 0.1);
        padding: 18px;
      }

      label {
        display: grid;
        gap: 6px;
        margin: 10px 0;
        color: var(--muted);
        font-size: 14px;
      }

      input,
      select,
      textarea,
      button {
        font: inherit;
      }

      input,
      select,
      textarea {
        width: 100%;
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px 12px;
        color: var(--ink);
        background: #fffdf7;
      }

      textarea {
        min-height: 260px;
        resize: vertical;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 13px;
      }

      button {
        border: 0;
        border-radius: 999px;
        padding: 10px 16px;
        color: white;
        background: var(--accent);
        cursor: pointer;
      }

      button.secondary {
        color: var(--accent-dark);
        background: #d7f2ec;
      }

      button:hover {
        background: var(--accent-dark);
      }

      .actions {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 14px;
      }

      pre {
        overflow: auto;
        max-height: 420px;
        border-radius: 16px;
        padding: 14px;
        color: #e7f9f5;
        background: var(--code);
      }

      .status {
        min-height: 24px;
        color: var(--accent-dark);
      }

      .status.error {
        color: var(--danger);
      }

      @media (max-width: 820px) {
        .grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <h1>perp-arb admin</h1>
        <p>任务 1 管理面：提交账户、提交策略、查看持久化结果和运行态分片规划。</p>
        <div id="status" class="status"></div>
      </section>

      <section class="grid">
        <article class="panel">
          <h2>账户配置</h2>
          <label>
            账户 ID
            <input id="account-id" value="binance-main" />
          </label>
          <label>
            名称
            <input id="account-name" value="Binance Main" />
          </label>
          <label>
            交易所
            <select id="account-exchange">
              <option value="binance_usd_m">binance_usd_m</option>
              <option value="bybit_linear">bybit_linear</option>
            </select>
          </label>
          <label>
            API Key
            <input id="account-api-key" value="demo-api-key" />
          </label>
          <label>
            API Secret
            <input id="account-api-secret" type="password" value="demo-api-secret" />
          </label>
          <label>
            Passphrase（可选）
            <input id="account-passphrase" />
          </label>
          <label>
            <span><input id="account-enabled" type="checkbox" checked /> 启用账户</span>
          </label>
          <div class="actions">
            <button id="save-account">保存账户</button>
            <button id="seed-bybit" class="secondary">填入 Bybit 示例</button>
          </div>
        </article>

        <article class="panel">
          <h2>策略配置 JSON</h2>
          <textarea id="strategy-json"></textarea>
          <div class="actions">
            <button id="save-strategy">保存策略</button>
            <button id="toggle-strategy" class="secondary">启用/停用策略</button>
          </div>
        </article>

        <article class="panel">
          <h2>策略列表</h2>
          <pre id="strategies">loading...</pre>
        </article>

        <article class="panel">
          <h2>账户列表</h2>
          <pre id="accounts">loading...</pre>
        </article>

        <article class="panel">
          <h2>运行态规划</h2>
          <pre id="plan">loading...</pre>
        </article>

        <article class="panel">
          <h2>查询入口</h2>
          <div class="actions">
            <button data-query="/api/positions">持仓</button>
            <button data-query="/api/balances">余额</button>
            <button data-query="/api/orders">活动订单</button>
            <button data-query="/api/runtime/status">运行状态</button>
          </div>
          <pre id="query-result">[]</pre>
        </article>
      </section>
    </main>

    <script>
      const $ = (id) => document.getElementById(id);
      const status = $("status");

      const defaultStrategy = {
        id: "btc-binance-bybit",
        name: "BTC Binance/Bybit",
        enabled: true,
        long_leg: {
          exchange: "binance_usd_m",
          symbol: "BTCUSDT",
          account_id: "binance-main"
        },
        short_leg: {
          exchange: "bybit_linear",
          symbol: "BTCUSDT",
          account_id: "bybit-main"
        },
        open_levels: [
          { spread_pct: 1.0, notional_usd: 100 },
          { spread_pct: 2.0, notional_usd: 100 }
        ],
        close_levels: [
          { spread_pct: 0.5, notional_usd: 100 }
        ],
        max_total_notional: 500,
        max_open_orders: 4,
        stale_order_query_ms: 3000
      };

      $("strategy-json").value = JSON.stringify(defaultStrategy, null, 2);

      function showStatus(message, isError = false) {
        status.textContent = message;
        status.classList.toggle("error", isError);
      }

      async function requestJson(url, options = {}) {
        const response = await fetch(url, {
          headers: { "content-type": "application/json" },
          ...options
        });
        const text = await response.text();
        const data = text ? JSON.parse(text) : null;
        if (!response.ok) {
          throw new Error(data?.error || response.statusText);
        }
        return data;
      }

      async function refresh() {
        const [strategies, accounts, plan] = await Promise.all([
          requestJson("/api/strategies"),
          requestJson("/api/accounts"),
          requestJson("/api/runtime/plan")
        ]);
        $("strategies").textContent = JSON.stringify(strategies, null, 2);
        $("accounts").textContent = JSON.stringify(accounts, null, 2);
        $("plan").textContent = plan
          ? `${plan.total_enabled_strategies} enabled strategies\n${JSON.stringify(plan, null, 2)}`
          : "{}";
      }

      $("seed-bybit").addEventListener("click", () => {
        $("account-id").value = "bybit-main";
        $("account-name").value = "Bybit Main";
        $("account-exchange").value = "bybit_linear";
        $("account-api-key").value = "demo-bybit-api-key";
        $("account-api-secret").value = "demo-bybit-api-secret";
      });

      $("save-account").addEventListener("click", async () => {
        try {
          const payload = {
            id: $("account-id").value,
            name: $("account-name").value,
            exchange: $("account-exchange").value,
            enabled: $("account-enabled").checked,
            api_key: $("account-api-key").value,
            api_secret: $("account-api-secret").value,
            passphrase: $("account-passphrase").value || null
          };
          await requestJson("/api/accounts", {
            method: "POST",
            body: JSON.stringify(payload)
          });
          showStatus("账户已保存，后端已持久化并重建运行态规划。");
          await refresh();
        } catch (error) {
          showStatus(error.message, true);
        }
      });

      $("save-strategy").addEventListener("click", async () => {
        try {
          const payload = JSON.parse($("strategy-json").value);
          await requestJson("/api/strategies", {
            method: "POST",
            body: JSON.stringify(payload)
          });
          showStatus("策略已保存，后端已持久化并重建运行态规划。");
          await refresh();
        } catch (error) {
          showStatus(error.message, true);
        }
      });

      $("toggle-strategy").addEventListener("click", async () => {
        try {
          const strategy = JSON.parse($("strategy-json").value);
          await requestJson(`/api/strategies/${encodeURIComponent(strategy.id)}/enabled`, {
            method: "POST",
            body: JSON.stringify({ enabled: !strategy.enabled })
          });
          strategy.enabled = !strategy.enabled;
          $("strategy-json").value = JSON.stringify(strategy, null, 2);
          showStatus(`策略已${strategy.enabled ? "启用" : "停用"}。`);
          await refresh();
        } catch (error) {
          showStatus(error.message, true);
        }
      });

      document.querySelectorAll("[data-query]").forEach((button) => {
        button.addEventListener("click", async () => {
          try {
            const data = await requestJson(button.dataset.query);
            $("query-result").textContent = JSON.stringify(data, null, 2);
          } catch (error) {
            showStatus(error.message, true);
          }
        });
      });

      refresh().catch((error) => showStatus(error.message, true));
    </script>
  </body>
</html>"#
}
