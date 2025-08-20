// worker.js — 高度な競合解決機能付き同期API
// 各操作にタイムスタンプを付けて、サーバー側でインテリジェントマージ

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, PUT, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Accept",
};

export default {
  async fetch(req, env) {
    if (req.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(req.url);
    // Routing: /state (GET/PUT), /export (POST), /import (POST)
    if (url.pathname === "/state") {
      const room = url.searchParams.get("room") || "";
      const key = url.searchParams.get("key") || "";

      if (!room || !key) {
        return json({ error: "room/key required" }, 400);
      }

  const kvKey = `room:${room}|${key}`;
  const usersKey = `users:room:${room}|${key}`;
  // prefer a dedicated USERS_KV if bound; fall back to STATE_KV for compatibility
  const usersStore = env.USERS_KV || env.STATE_KV;

      // --- GET: 最新状態を返す ---
      if (req.method === "GET") {
        const clientLastSync = parseInt(url.searchParams.get("lastSync") || "0", 10);

        try {
          const raw = await env.STATE_KV.get(kvKey, "json");
          // if a separate users KV exists, merge its authoritative users list
          try {
            const usersRaw = await (usersStore && usersStore.get ? usersStore.get(usersKey, 'json') : null);
            if (usersRaw && usersRaw.users && raw && raw.state) {
              raw.state.users = usersRaw.users;
            }
          } catch (e) {
            // ignore users store read errors, continue with raw
          }

          if (!raw || typeof raw !== "object") {
            return new Response(null, { status: 204, headers: corsHeaders });
          }

          const serverLastUpdate = raw.lastUpdate || 0;

          // クライアントの最終同期時刻より新しい更新があるかチェック
          if (serverLastUpdate <= clientLastSync) {
            return new Response(null, { status: 204, headers: corsHeaders });
          }

          return json({
            state: raw.state,
            lastUpdate: raw.lastUpdate,
            processedOps: raw.processedOps || [],
            conflicts: raw.conflicts || [],
            notify: raw.notify || { newUsers: [], newTxs: [] }
          });

        } catch (error) {
          console.error("GET error:", error);
          return json({ error: "fetch failed" }, 500);
        }
      }

      // --- PUT: 操作をマージして状態を更新 ---
      if (req.method === "PUT") {
        try {
          const body = await safeJSON(req);

          if (!body || !Array.isArray(body.operations)) {
            return json({ error: "operations array required" }, 400);
          }

          // 現在の状態を取得
          const current = await env.STATE_KV.get(kvKey, "json") || {
            state: { users: [], txs: [], listings: [], rights: [], notifications: [], vTick: 0 },
            lastUpdate: 0,
            processedOps: [],
            conflicts: []
          };

          // 新しい操作をマージ
          const mergeResult = await mergeOperations(current, body.operations);

          // 更新時刻を設定
          mergeResult.lastUpdate = Date.now();

          // KVに保存
          const toSave = {
            state: mergeResult.state,
            lastUpdate: mergeResult.lastUpdate,
            processedOps: mergeResult.processedOps || [],
            conflicts: mergeResult.conflicts || [],
            notify: {
              newUsers: mergeResult.newUsers || [],
              newTxs: mergeResult.newTxs || [],
              newRights: mergeResult.newRights || [],
              updatedRights: mergeResult.updatedRights || [],
              updatedListings: mergeResult.updatedListings || [],
              deletedListings: mergeResult.deletedListings || [],
              notifications: mergeResult.state.notifications || [],
            }
          };

          await env.STATE_KV.put(
            kvKey,
            JSON.stringify(toSave),
            { expirationTtl: 60 * 60 * 24 * 7 }
          );
          // persist users separately if possible to avoid truncation/loss in single large state
          try {
            if (usersStore && usersStore.put) {
              await usersStore.put(usersKey, JSON.stringify({ users: mergeResult.state.users || [], lastUpdate: mergeResult.lastUpdate }), { expirationTtl: 60 * 60 * 24 * 7 });
            }
          } catch (e) {
            // non-fatal: continue
            console.error('usersStore put failed', e);
          }

          // レスポンスに更新後の完全な state と processedOps を返す
          return json({
            ok: true,
            newState: toSave.state,
            processedOps: (toSave.processedOps || []).map(p => p.id || p),
            conflicts: mergeResult.newConflicts || [],
            notify: toSave.notify,
            lastUpdate: toSave.lastUpdate
          });

        } catch (error) {
          console.error("PUT error:", error);
          return json({ error: "merge failed: " + error.message }, 500);
        }
      }

      return json({ error: "method not allowed" }, 405);
    }

    if (url.pathname === "/export") {
      return await handleExportEndpoint(req, env, url, usersStore);
    }

    if (url.pathname === "/import") {
      return await handleImportEndpoint(req, env, url, usersStore);
    }

    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }
};

// 操作をマージして競合を解決
async function mergeOperations(current, newOperations) {
  const result = {
    state: JSON.parse(JSON.stringify(current.state)), // deep copy
    processedOps: [...(current.processedOps || [])],
    conflicts: [...(current.conflicts || [])],
    newConflicts: [],
    newUsers: [],
    newTxs: [],
    newRights: [],
    updatedListings: [],
    deletedListings: []
  };

  const unprocessedOps = newOperations.filter(op =>
    !result.processedOps.some(processed => processed.id === op.id)
  );

  unprocessedOps.sort((a, b) => a.timestamp - b.timestamp);

  for (const op of unprocessedOps) {
    try {
      const conflictResult = await applyOperation(result.state, op);

      if (conflictResult.conflict) {
        result.newConflicts.push({
          operationId: op.id,
          type: conflictResult.conflictType,
          message: conflictResult.message,
          timestamp: op.timestamp,
          userId: op.userId
        });
      }

      if (conflictResult.createdUser) result.newUsers.push(conflictResult.createdUser);
      if (Array.isArray(conflictResult.createdTxs) && conflictResult.createdTxs.length) result.newTxs.push(...conflictResult.createdTxs);
      if (conflictResult.createdRight) result.newRights.push(conflictResult.createdRight);
      if (conflictResult.updatedListing) result.updatedListings.push(conflictResult.updatedListing);
      if (conflictResult.deletedListingId) result.deletedListings.push(conflictResult.deletedListingId);

      result.processedOps.push({
        id: op.id,
        timestamp: op.timestamp,
        type: op.type,
        userId: op.userId
      });

    } catch (error) {
      result.newConflicts.push({
        operationId: op.id,
        type: "error",
        message: error.message,
        timestamp: op.timestamp,
        userId: op.userId
      });
    }
  }

  const cutoff = Date.now() - (1000 * 60 * 60);
  result.processedOps = result.processedOps.filter(op => op.timestamp > cutoff);
  result.conflicts = result.conflicts.filter(c => c.timestamp > cutoff);
  // Clear old notifications after 1 minute
  const notifyCutoff = Date.now() - (1000 * 60);
  if(result.state.notifications) {
      result.state.notifications = result.state.notifications.filter(n => n.timestamp > notifyCutoff);
  }


  return result;
}

// 個別の操作を適用（競合チェック付き）
async function applyOperation(state, operation) {
  const { type, data, timestamp, userId, meta } = operation;

  switch (type) {
    case "transfer":
      return applyTransfer(state, data, timestamp, userId, meta);
    case "buy_listing":
      return applyBuyListing(state, data, timestamp, userId, meta);
    case "create_listing":
      return applyCreateListing(state, data, timestamp, userId, meta);
    case "toggle_listing":
      return applyToggleListing(state, data, timestamp, userId, meta);
    case "delete_listing":
      return applyDeleteListing(state, data, timestamp, userId, meta);
    case "register_user":
      return applyRegisterUser(state, data, timestamp, meta);
    case "morning_claim":
      return applyMorningClaim(state, data, timestamp, userId, meta);
    case "roulette":
      return applyRoulette(state, data, timestamp, userId, meta);
    case "buyer_request":
      return applyBuyerRequest(state, data, timestamp, userId, meta);
    case "seller_respond":
      return applySellerRespond(state, data, timestamp, userId, meta);
    case "report_execution":
      return applyReportExecution(state, data, timestamp, userId, meta);
    case "buyer_confirm":
      return applyBuyerConfirm(state, data, timestamp, userId, meta);
    case "buyer_reject":
      return applyBuyerReject(state, data, timestamp, userId, meta);
    case "seller_refund":
      return applySellerRefund(state, data, timestamp, userId, meta);
    case "buyer_finalize":
      return applyBuyerFinalize(state, data, timestamp, userId, meta);
    case "send_message":
      return applySendMessage(state, data, timestamp);
    default:
      return { conflict: true, conflictType: "unknown_operation", message: "Unknown operation type: " + type };
  }
}

// メッセージ送信操作の適用
function applySendMessage(state, data, timestamp) {
    const { recipientId, content, type, isHtml } = data;
    if (!recipientId || !content || !type) {
        return { conflict: true, conflictType: 'bad_request', message: 'recipientId, content, and type are required for send_message.' };
    }
    if (!state.notifications) {
        state.notifications = [];
    }
    state.notifications.push({
        id: generateId(),
        timestamp,
        recipientId,
        content,
        type,
        isHtml: isHtml || false
    });
    return { conflict: false };
}


// 送金操作の適用
function applyTransfer(state, data, timestamp, userId, meta) {
  const { toId, amount, memo } = data;
  const from = state.users.find(u => u.id === userId);
  const to = state.users.find(u => u.id === toId);

  if (!from || !to) return { conflict: true, conflictType: "user_not_found", message: "送信者または受信者が見つかりません" };
  if (from.balance < amount) return { conflict: true, conflictType: "insufficient_balance", message: "残高不足" };

  from.balance -= amount;
  to.balance += amount;

  const tx = { id: generateId(), ts: timestamp, type: 'transfer', from: userId, to: toId, amount, memo: memo || '' };
  if (!meta?.silent) state.txs.unshift(tx);

  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, createdTxs: !meta?.silent ? [tx] : [] };
}

// 出品購入の適用
function applyBuyListing(state, data, timestamp, userId, meta) {
  const { listingId } = data;
  const listing = state.listings.find(l => l.id === listingId && l.active);
  const buyer = state.users.find(u => u.id === userId);

  if (!listing) return { conflict: true, conflictType: "listing_not_found", message: "出品が見つからないか既に停止中" };
  if (!buyer) return { conflict: true, conflictType: "buyer_not_found", message: "購入者が見つかりません" };
  if ((listing.qty || 1) - (listing.sold || 0) <= 0) return { conflict: true, conflictType: "sold_out", message: "在庫切れ" };
  if (buyer.balance < listing.price) return { conflict: true, conflictType: "insufficient_balance", message: "残高不足" };
  if (listing.sellerId === userId) return { conflict: true, conflictType: "self_purchase", message: "自分の出品は購入できません" };

  const seller = state.users.find(u => u.id === listing.sellerId);
  const jackpot = state.users.find(u => u.name === 'ジャックポット');
  const fee = Math.floor(listing.price * 0.10);
  const sellerReceive = listing.price - fee;

  buyer.balance -= listing.price;
  if (seller) seller.balance += sellerReceive;
  if (jackpot) jackpot.balance += fee;

  listing.sold = (listing.sold || 0) + 1;
  if (listing.sold >= listing.qty) listing.active = false;

  const createdRight = { id: generateId(), listingId: listing.id, title: listing.title, buyerId: userId, sellerId: listing.sellerId, ts: timestamp, status: 'owned', executed: false };
  state.rights.unshift(createdRight);

  const tx = { id: generateId(), ts: timestamp, type: 'purchase', from: userId, to: listing.sellerId, amount: listing.price, listingId: listing.id, title: listing.title, memo: '権利購入（手数料10%）' };
  if (!meta?.silent) state.txs.unshift(tx);

  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, createdRight, createdTxs: !meta?.silent ? [tx] : [], updatedListing: listing };
}

// 出品作成の適用
function applyCreateListing(state, data, timestamp, userId, meta) {
  const { title, price, desc, qty } = data;
  const newListing = { id: generateId(), title, price, desc, sellerId: userId, active: true, ts: timestamp, qty: qty || 1, sold: 0 };
  state.listings.unshift(newListing);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, updatedListing: newListing };
}

// 出品切り替えの適用
function applyToggleListing(state, data, timestamp, userId, meta) {
  const { listingId } = data;
  const listing = state.listings.find(l => l.id === listingId);
  if (!listing) return { conflict: true, conflictType: "listing_not_found", message: "出品が見つかりません" };
  if (listing.sellerId !== userId) return { conflict: true, conflictType: "not_owner", message: "自分の出品ではありません" };
  listing.active = !listing.active;
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false };
}

// ユーザー登録の適用
function applyRegisterUser(state, data, timestamp, meta) {
  const { name, passHash } = data;
  if (state.users.some(u => u.name === name)) return { conflict: true, conflictType: "name_taken", message: "そのユーザー名は既に使用されています" };
  const newUser = { id: generateId(), name, passHash, balance: 10000, streakCount: 0, lastMorningDate: '' };
  state.users.push(newUser);
  const mintTx = { id: generateId(), ts: timestamp, type: 'mint', to: newUser.id, amount: 10000, memo: '初期発行' };
  if (!meta?.silent) state.txs.unshift(mintTx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, createdUser: newUser, createdTxs: !meta?.silent ? [mintTx] : [] };
}

// 朝活の適用
function applyMorningClaim(state, data, timestamp, userId, meta) {
  const user = state.users.find(u => u.id === userId);
  if (!user) return { conflict: true, conflictType: "user_not_found", message: "ユーザーが見つかりません" };
  const today = new Date(timestamp).toISOString().split('T')[0];
  if (user.lastMorningDate === today) return { conflict: true, conflictType: "already_claimed", message: "本日は既に受取済み" };

  if (user.lastMorningDate) {
    const diffDays = Math.round((new Date(today) - new Date(user.lastMorningDate)) / 86400000);
    user.streakCount = (diffDays === 1) ? Math.min((user.streakCount || 0) + 1, 30) : 1;
  } else {
    user.streakCount = 1;
  }
  user.lastMorningDate = today;
  const bonus = 1000 + 100 * (user.streakCount || 0);
  user.balance += bonus;
  const tx = { id: generateId(), ts: timestamp, type: 'mint', to: userId, amount: bonus, memo: `朝活（連続${user.streakCount}日）` };
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, createdTxs: !meta?.silent ? [tx] : [] };
}

// ルーレットの適用
function applyRoulette(state, data, timestamp, userId, meta) {
  const { roll } = data;
  const user = state.users.find(u => u.id === userId);
  const jackpot = state.users.find(u => u.name === 'ジャックポット');
  if (!user || !jackpot) return { conflict: true, conflictType: "user_not_found", message: "ユーザーが見つかりません" };
  if (user.balance < 100) return { conflict: true, conflictType: "insufficient_balance", message: "残高不足" };

  user.balance -= 100;
  jackpot.balance += 100;
  const createdTxs = [];
  const feeTx = { id: generateId(), ts: timestamp, type: 'jpot-fee', from: userId, to: jackpot.id, amount: 100, memo: 'ルーレット参加費' };
  if (!meta?.silent) { state.txs.unshift(feeTx); createdTxs.push(feeTx); }

  let payout = 0;
  if (roll < 0.01) {
    payout = jackpot.balance;
    if (payout > 0) {
      user.balance += payout;
      jackpot.balance = 0;
      const jackpotTx = { id: generateId(), ts: timestamp, type: 'jackpot', from: jackpot.id, to: userId, amount: payout, memo: '🎉 ジャックポット当選' };
      if (!meta?.silent) { state.txs.unshift(jackpotTx); createdTxs.push(jackpotTx); }
    }
  } else {
    const prizes = [{p: 0.40, v: 0}, {p: 0.25, v: 50}, {p: 0.18, v: 100}, {p: 0.09, v: 150}, {p: 0.05, v: 200}, {p: 0.02, v: 400}, {p: 0.0099, v: 700}];
    let acc = 0;
    for (const prize of prizes) {
      acc += prize.p;
      if (roll < acc) {
        payout = Math.min(jackpot.balance, prize.v);
        break;
      }
    }
    if (payout > 0) {
      user.balance += payout;
      jackpot.balance -= payout;
      const payoutTx = { id: generateId(), ts: timestamp, type: 'roulette', from: jackpot.id, to: userId, amount: payout, memo: 'ルーレット払い出し' };
      if (!meta?.silent) { state.txs.unshift(payoutTx); createdTxs.push(payoutTx); }
    }
  }
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, payout, createdTxs };
}

function generateId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { "content-type": "application/json; charset=utf-8", ...corsHeaders } });
}

async function safeJSON(req) {
  try {
    return await req.json();
  } catch (error) {
    return null;
  }
}

async function handleExportEndpoint(req, env, url, usersStore) {
  if (req.method !== 'POST') return json({ error: 'POST required' }, 405);
  const room = url.searchParams.get('room') || '';
  const key = url.searchParams.get('key') || '';
  if (!room || !key) return json({ error: 'room/key required' }, 400);
  const kvKey = `room:${room}|${key}`;
  const usersKey = `users:room:${room}|${key}`;
  try {
    const raw = await env.STATE_KV.get(kvKey, 'json');
    if (!raw || !raw.state) return json({ error: 'No data found' }, 404);
    try {
      const usersRaw = usersStore && usersStore.get ? await usersStore.get(usersKey, 'json') : null;
      if (usersRaw && usersRaw.users) raw.state.users = usersRaw.users;
    } catch (e) {}
    return json({ room, exportedAt: new Date().toISOString(), data: raw.state, metadata: { version: 'v14', lastUpdate: raw.lastUpdate } });
  } catch (e) {
    return json({ error: 'Export failed' }, 500);
  }
}

async function handleImportEndpoint(req, env, url, usersStore) {
  if (req.method !== 'POST') return json({ error: 'POST required' }, 405);
  const room = url.searchParams.get('room') || '';
  const key = url.searchParams.get('key') || '';
  if (!room || !key) return json({ error: 'room/key required' }, 400);
  const kvKey = `room:${room}|${key}`;
  const usersKey = `users:room:${room}|${key}`;
  try {
    const body = await safeJSON(req);
    if (!body || !body.data) return json({ error: 'No data to import' }, 400);
    const overwrite = body.overwrite === true;
    if (!overwrite) {
      const existing = await env.STATE_KV.get(kvKey, 'json');
      if (existing) return json({ error: 'Data exists, use overwrite:true' }, 409);
    }
    const newState = { state: body.data, lastUpdate: Date.now(), processedOps: [], conflicts: [] };
    await env.STATE_KV.put(kvKey, JSON.stringify(newState));
    try {
      if (usersStore && usersStore.put) {
        await usersStore.put(usersKey, JSON.stringify({ users: body.data.users || [], lastUpdate: Date.now() }));
      }
    } catch (e) {}
    const stats = { users: body.data.users?.length || 0, txs: body.data.txs?.length || 0, listings: body.data.listings?.length || 0, rights: body.data.rights?.length || 0 };
    return json({ ok: true, message: 'Data imported successfully', recordsImported: stats });
  } catch (e) {
    return json({ error: 'Import failed: ' + e.message }, 500);
  }
}

function applyDeleteListing(state, data, timestamp, userId, meta) {
  const { listingId } = data;
  const idx = state.listings.findIndex(l => l.id === listingId);
  if (idx === -1) return { conflict: true, conflictType: "listing_not_found", message: "出品が見つかりません" };
  const listing = state.listings[idx];
  if (listing.sellerId !== userId) return { conflict: true, conflictType: "not_owner", message: "自分の出品ではありません" };
  if ((listing.sold || 0) > 0) return { conflict: true, conflictType: "has_sales", message: "購入履歴があるため削除できません" };
  state.listings.splice(idx, 1);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, deletedListingId: listingId };
}

function applyBuyerRequest(state, data, timestamp, userId, meta) {
  const { rightId } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.buyerId !== userId) return { conflict: true, conflictType: 'not_buyer', message: '購入者のみ申請可' };
  if (r.status !== 'owned') return { conflict: true, conflictType: 'invalid_state', message: '実行申請不可の状態' };
  r.status = 'request';
  const tx = { id: generateId(), ts: timestamp, type: 'execute-request', from: r.buyerId, to: r.sellerId, listingId: r.listingId, title: r.title, memo: '購入者が実行を申請' };
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, updatedRight: r, createdTxs: !meta?.silent ? [tx] : [] };
}

function applySellerRespond(state, data, timestamp, userId, meta) {
  const { rightId, action } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.sellerId !== userId) return { conflict: true, conflictType: 'not_seller', message: '出品者のみ応答可' };
  if (r.status !== 'request') return { conflict: true, conflictType: 'invalid_state', message: '応答不可の状態' };

  let tx;
  if (action === 'exec') {
    r.status = 'seller_executed';
    tx = { id: generateId(), ts: timestamp, type: 'execute-done', from: r.sellerId, to: r.buyerId, listingId: r.listingId, title: r.title, memo: '販売者が実行' };
  } else {
    r.status = 'seller_cancel_requested';
    tx = { id: generateId(), ts: timestamp, type: 'execute-cancel-ask', from: r.sellerId, to: r.buyerId, listingId: r.listingId, title: r.title, memo: '販売者がキャンセル申請' };
  }
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, updatedRight: r, createdTxs: !meta?.silent ? [tx] : [] };
}

function applyBuyerFinalize(state, data, timestamp, userId, meta) {
  const { rightId } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.buyerId !== userId) return { conflict: true, conflictType: 'not_buyer', message: '購入者のみ確定可' };

  let tx;
  if (r.status === 'seller_executed') {
    r.status = 'finalized';
    r.executed = true;
    tx = { id: generateId(), ts: timestamp, type: 'execute-finalize', from: r.buyerId, to: r.sellerId, listingId: r.listingId, title: r.title, memo: '購入者が実行確認（確定）' };
    if (!meta?.silent) state.txs.unshift(tx);
    state.vTick = (state.vTick || 0) + 1;
    return { conflict: false, updatedRight: r, createdTxs: !meta?.silent ? [tx] : [] };
  } else if (r.status === 'seller_cancel_requested') {
    const l = state.listings.find(x => x.id === r.listingId);
    const seller = state.users.find(u => u.id === r.sellerId);
    const buyer = state.users.find(u => u.id === r.buyerId);
    if (!l || !seller || !buyer) return { conflict: true, conflictType: 'missing_entities', message: '関連データが不足' };
    const refund = Math.floor(l.price * 0.90);
    if (seller.balance < refund) return { conflict: true, conflictType: 'seller_insufficient', message: '販売者残高不足' };
    seller.balance -= refund;
    buyer.balance += refund;
    l.sold = Math.max(0, (l.sold || 0) - 1);
    if (l.sold < l.qty) l.active = true;
    state.rights = state.rights.filter(x => x.id !== rightId);
    tx = { id: generateId(), ts: timestamp, type: 'execute-cancel-ok', from: r.buyerId, to: r.sellerId, listingId: l.id, title: r.title, amount: refund, memo: 'キャンセル返金（全額）確定' };
    if (!meta?.silent) state.txs.unshift(tx);
    state.vTick = (state.vTick || 0) + 1;
    return { conflict: false, updatedListing: l, createdTxs: !meta?.silent ? [tx] : [] };
  }
  return { conflict: true, conflictType: 'invalid_state', message: '確定不可の状態' };
}

function applyReportExecution(state, data, timestamp, userId, meta) {
  const { rightId } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.sellerId !== userId) return { conflict: true, conflictType: 'not_seller', message: '出品者のみ報告可' };
  if (r.status !== 'request') return { conflict: true, conflictType: 'invalid_state', message: '報告不可の状態' };
  r.status = 'seller_reported';
  r.rejectionCount = 0;
  const tx = { id: generateId(), ts: timestamp, type: 'execute-reported', from: r.sellerId, to: r.buyerId, listingId: r.listingId, title: r.title, memo: '販売者が実行を報告' };
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, updatedRight: r, createdTxs: !meta?.silent ? [tx] : [] };
}

function applyBuyerConfirm(state, data, timestamp, userId, meta) {
  const { rightId } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.buyerId !== userId) return { conflict: true, conflictType: 'not_buyer', message: '購入者のみ確認可' };
  if (r.status !== 'seller_reported') return { conflict: true, conflictType: 'invalid_state', message: '確認不可の状態' };
  r.status = 'finalized'; r.executed = true;
  delete r.rejectionCount;
  const tx = { id: generateId(), ts: timestamp, type: 'execute-confirm', from: r.buyerId, to: r.sellerId, listingId: r.listingId, title: r.title, memo: '購入者が実行を確認（承諾）' };
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, updatedRight: r, createdTxs: !meta?.silent ? [tx] : [] };
}

function applyBuyerReject(state, data, timestamp, userId, meta) {
  const { rightId } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.buyerId !== userId) return { conflict: true, conflictType: 'not_buyer', message: '購入者のみ拒否可' };
  if (r.status !== 'seller_reported') return { conflict: true, conflictType: 'invalid_state', message: '拒否不可の状態' };
  if (typeof r.rejectionCount === 'number' && r.rejectionCount >= 1) return { conflict: true, conflictType: 'already_rejected', message: '既に拒否済みです' };
  r.rejectionCount = (r.rejectionCount || 0) + 1;
  r.status = 'buyer_rejected';
  const tx = { id: generateId(), ts: timestamp, type: 'execute-reject', from: r.buyerId, to: r.sellerId, listingId: r.listingId, title: r.title, memo: '購入者が実行を拒否' };
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, updatedRight: r, createdTxs: !meta?.silent ? [tx] : [] };
}

function applySellerRefund(state, data, timestamp, userId, meta) {
  const { rightId, amount } = data;
  const r = state.rights.find(x => x.id === rightId);
  if (!r) return { conflict: true, conflictType: 'right_not_found', message: '権利が見つかりません' };
  if (r.sellerId !== userId) return { conflict: true, conflictType: 'not_seller', message: '出品者のみ返金可' };
  if (!['buyer_rejected', 'seller_cancel_requested', 'seller_reported'].includes(r.status)) return { conflict: true, conflictType: 'invalid_state', message: '返金不可の状態' };

  const listing = state.listings.find(l => l.id === r.listingId);
  const seller = state.users.find(u => u.id === r.sellerId);
  const buyer = state.users.find(u => u.id === r.buyerId);
  if (!listing || !seller || !buyer) return { conflict: true, conflictType: 'missing_entities', message: '関連データが不足' };

  const fullAmount = listing.price || 0;
  const refundAmount = typeof amount === 'number' && amount > 0 ? Math.min(amount, fullAmount) : fullAmount;
  if (seller.balance < refundAmount) return { conflict: true, conflictType: 'seller_insufficient', message: '販売者残高不足' };

  seller.balance -= refundAmount;
  buyer.balance += refundAmount;
  listing.sold = Math.max(0, (listing.sold || 0) - 1);
  if (listing.sold < listing.qty) listing.active = true;
  state.rights = state.rights.filter(x => x.id !== rightId);

  const tx = { id: generateId(), ts: timestamp, type: 'seller_refund', from: seller.id, to: buyer.id, listingId: listing.id, title: r.title, amount: refundAmount, memo: `返金: ¥${refundAmount}` };
  if (!meta?.silent) state.txs.unshift(tx);
  state.vTick = (state.vTick || 0) + 1;
  return { conflict: false, deletedListingId: listing.id, createdTxs: !meta?.silent ? [tx] : [], updatedListing: listing };
}
