// worker.js  — Cloudflare Workers (Free) + KV による同期API
// ルーム状態を KV に保存し、クライアントはポーリングで同期します。

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (url.pathname === "/state") {
      const room = url.searchParams.get("room") || "";
      const key  = url.searchParams.get("key")  || "";
      if (!room || !key) return json({ error: "room/key required" }, 400);

      const kvKey = `room:${room}|${key}`;
      if (req.method === "GET") {
        // クライアントが知っている vTick を受け取り、最新が大きい時だけ返す
        const vt = parseInt(url.searchParams.get("vt") || "0", 10);
        const raw = await env.STATE_KV.get(kvKey, "json");
        if (!raw || typeof raw !== "object" || (raw.state?.vTick ?? 0) <= vt) {
          // 何も更新がなければ 204
          return new Response(null, { status: 204 });
        }
        // 最新状態を返す
        return json({ v: raw.v, state: raw.state });
      }

      if (req.method === "PUT") {
        const body = await safeJSON(req);
        if (!body || typeof body !== "object" || typeof body.state !== "object") {
          return json({ error: "invalid payload" }, 400);
        }
        // 保存する。軽い競合対策でサーバ側の連番を持つ
        const prev = (await env.STATE_KV.get(kvKey, "json")) || { v: 0 };
        const next = { v: (prev.v || 0) + 1, state: body.state };
        await env.STATE_KV.put(kvKey, JSON.stringify(next), { expirationTtl: 60 * 60 * 24 * 7 }); // 7日
        return json({ ok: true, v: next.v });
      }

      return json({ error: "method not allowed" }, 405);
    }

    return new Response("OK", { status: 200 });
  }
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}

async function safeJSON(req) {
  try { return await req.json(); } catch { return null; }
}
