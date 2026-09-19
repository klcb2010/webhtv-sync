// Merged single-file Cloudflare Worker
// Official source: fish2018/webhtv main branch
//   serverless/webhtv-remote-cloudflare/src/{index.js,relay.js,playback-sync.js}
// Paste this entire file into Cloudflare Dashboard -> Workers -> Edit code
// Required Durable Object bindings (wrangler / dashboard):
//   RELAY_DO    -> WebHTVRemoteRelayDO
//   PLAYBACK_DO -> WebHTVPlaybackSyncDO
// Migrations:
//   v1: new_sqlite_classes = ["WebHTVRemoteRelayDO"]
//   v2: new_sqlite_classes = ["WebHTVPlaybackSyncDO"]

// ========== relay.js ==========
const SERVER_MODE = 'cloudflare';
const BIND_TTL_MS = 10 * 60 * 1000;
const COMMAND_TTL_MS = 60 * 60 * 1000;
const SYNC_TTL_MS = 2 * 60 * 60 * 1000;
const MAX_SYNC_PART_BYTES = 25 * 1024 * 1024;
const PARTS = new Set(['backup', 'syncFiles', 'loginStateFiles', 'manifest']);

const CAPABILITIES = {
  configManage: true,
  remoteSync: true,
  pushAction: true,
  recentLog: true,
  deviceBackup: false,
  fileManage: false,
  webHomeManage: false,
  shellProxyManage: false,
  siteInjectManage: false,
  webHomeExtensionManage: false,
  playbackSync: false,
  playbackPersistentStorage: false,
  multiDeviceBatch: false,
  webSocket: false,
  persistentStorage: false,
  externalObjectStorage: false,
  deviceRevoke: false
};

function createRelayState(snapshot = null) {
  return {
    devices: new Map(snapshot?.devices || []),
    bindCodes: new Map(snapshot?.bindCodes || []),
    groupDevices: new Map((snapshot?.groupDevices || []).map(([groupId, devices]) => [groupId, new Set(devices || [])])),
    commands: new Map(snapshot?.commands || []),
    queues: new Map((snapshot?.queues || []).map(([deviceId, queue]) => [deviceId, Array.isArray(queue) ? queue : []])),
    syncs: new Map(snapshot?.syncs || []),
    parts: new Map(),
    lastCleanup: Number(snapshot?.lastCleanup || 0)
  };
}

function snapshotRelayState(input = state) {
  return {
    devices: [...input.devices],
    bindCodes: [...input.bindCodes],
    groupDevices: [...input.groupDevices].map(([groupId, devices]) => [groupId, [...devices]]),
    commands: [...input.commands],
    queues: [...input.queues].map(([deviceId, queue]) => [deviceId, [...queue]]),
    syncs: [...input.syncs],
    lastCleanup: input.lastCleanup
  };
}

const defaultState = globalThis.__WEBHTV_REMOTE_RELAY_STATE || (globalThis.__WEBHTV_REMOTE_RELAY_STATE = createRelayState());

let state = defaultState;

function withStateAsync(nextState, run) {
  const previous = state;
  state = nextState || defaultState;
  return Promise.resolve()
    .then(run)
    .finally(() => {
      state = previous;
    });
}

async function handleRelayRequest(request, options = {}) {
  return withStateAsync(options.state, async () => {
    if (request.method === 'OPTIONS') return cors(new Response(null, { status: 204 }));
    try {
      cleanup();
      return cors(await route(request, options));
    } catch (e) {
      return cors(json({ ok: false, error: e && e.message ? e.message : String(e) }, e && e.status ? e.status : 500));
    }
  });
}

async function route(request, options) {
  const url = new URL(request.url);
  const path = url.pathname.replace(/\/+$/, '') || '/';
  const method = request.method.toUpperCase();

  if (method === 'GET' && path === '/api/health') return json({ ok: true, time: Date.now() });
  if (method === 'GET' && path === '/api/server/capabilities') return json(capabilities(options));

  if (method === 'POST' && path === '/api/device/register') return registerDevice(request, options);
  if (method === 'POST' && path === '/api/device/bind-code') return createBindCode(request, options);
  if (method === 'POST' && path === '/api/device/poll') return pollDevice(request, options);

  if (method === 'POST' && (path === '/api/groups/claim' || path === '/api/family/claim')) return claimDevice(request, options);
  if (method === 'GET' && path === '/api/devices') return listDevices(request, options);

  if (method === 'POST' && path === '/api/commands') return createCommand(request, options);
  {
    const m = path.match(/^\/api\/commands\/([^/]+)$/);
    if (m && method === 'GET') return getCommand(request, options, m[1]);
  }
  {
    const m = path.match(/^\/api\/commands\/([^/]+)\/result$/);
    if (m && method === 'POST') return commandResult(request, m[1]);
  }

  if (method === 'POST' && path === '/api/sync/create') return createSync(request, options);
  {
    const m = path.match(/^\/api\/sync\/([^/]+)$/);
    if (m && method === 'GET') return getSync(request, m[1]);
  }
  {
    const m = path.match(/^\/api\/sync\/([^/]+)\/part\/([^/]+)$/);
    if (m && method === 'POST') return uploadSyncPart(request, m[1], m[2]);
    if (m && method === 'GET') return downloadSyncPart(request, m[1], m[2]);
  }
  {
    const m = path.match(/^\/api\/sync\/([^/]+)\/export-complete$/);
    if (m && method === 'POST') return exportComplete(request, options, m[1]);
  }
  {
    const m = path.match(/^\/api\/sync\/([^/]+)\/restore-complete$/);
    if (m && method === 'POST') return restoreComplete(request, m[1]);
  }

  return json({ ok: false, error: 'Not found' }, 404);
}

function capabilities(options) {
  const persistentStorage = options.persistentStorage === true;
  return {
    ok: true,
    serverMode: SERVER_MODE,
    serverName: options.serverName || 'WebHTV Remote Relay',
    relayMode: options.relayMode || 'origin-token-memory',
    time: Date.now(),
    maxSyncPartBytes: MAX_SYNC_PART_BYTES,
    capabilities: {
      ...CAPABILITIES,
      playbackSync: options.playbackSync === true,
      playbackPersistentStorage: options.playbackPersistentStorage === true,
      persistentStorage
    }
  };
}

async function registerDevice(request, options) {
  const body = await readJson(request);
  const origin = serverOrigin(request);
  let deviceToken = readDeviceToken(request, body);
  if (!deviceToken) deviceToken = randomCapability('dtk');
  const deviceId = await requireDerivedId('dev', origin, deviceToken, body.deviceId, 'Invalid device token');
  const existing = state.devices.get(deviceId);
  const now = Date.now();
  const groupIds = new Set(deviceGroupIds(existing));
  const groupTokens = readGroupTokens(request, body);
  for (const token of groupTokens) {
    const group = await groupFromToken(request, token);
    groupIds.add(group.groupId);
    linkDevice(group.groupId, deviceId);
  }
  const device = {
    deviceId,
    groupId: [...groupIds][0] || null,
    groupIds: [...groupIds],
    name: String(body.name || existing?.name || 'WebHTV'),
    alias: String(body.alias || existing?.alias || ''),
    role: String(body.role || existing?.role || 'app'),
    type: body.type ?? existing?.type ?? 0,
    appVersion: String(body.appVersion || existing?.appVersion || ''),
    capabilities: body.capabilities || existing?.capabilities || {},
    lastSeen: now,
    createdAt: existing?.createdAt || now,
    updatedAt: now
  };
  state.devices.set(deviceId, device);
  for (const id of device.groupIds) linkDevice(id, deviceId);
  return json({ ok: true, deviceId, deviceToken, deviceSecret: deviceToken, groupIds: device.groupIds, server: capabilities(options) });
}

async function createBindCode(request, options) {
  const body = await readJson(request);
  const { device } = await requireDevice(request, body);
  const origin = serverOrigin(request);
  const bindGrantToken = String(body.bindGrantToken || '').trim();
  if (!bindGrantToken) throw httpError(400, 'Missing bindGrantToken');
  const grantId = await requireDerivedId('bnd', origin, bindGrantToken, body.grantId, 'Invalid bind grant token');
  let code = '';
  for (let i = 0; i < 8; i++) {
    code = String(Math.floor(100000 + Math.random() * 900000));
    if (!state.bindCodes.has(code)) break;
  }
  state.bindCodes.set(code, { code, deviceId: device.deviceId, grantId, bindGrantToken, expiresAt: Date.now() + BIND_TTL_MS });
  return json({ ok: true, code, grantId, expiresIn: Math.floor(BIND_TTL_MS / 1000), server: capabilities(options) });
}

async function claimDevice(request, options) {
  const body = await readJson(request);
  const { device: requester } = await requireDevice(request, body);
  const code = String(body.code || '').trim();
  const bind = state.bindCodes.get(code);
  if (!bind || bind.expiresAt < Date.now()) throw httpError(404, 'Bind code expired');
  if (requester.deviceId === bind.deviceId) throw httpError(400, 'Cannot bind local device');

  const device = state.devices.get(bind.deviceId);
  if (!device) throw httpError(404, 'Device not found');
  const group = await groupFromToken(request, readGroupToken(request, body) || randomCapability('gtk'));
  addGroupToDevice(device, group.groupId);
  device.alias = String(body.alias || device.alias || device.name || '');
  device.updatedAt = Date.now();
  state.devices.set(device.deviceId, device);
  linkDevice(group.groupId, device.deviceId);
  state.bindCodes.delete(code);
  const command = enqueueCommand(group.groupId, device.deviceId, 'remote.profile.addGroup', {
    groupId: group.groupId,
    groupToken: group.groupToken,
    groupTokenHash: group.groupTokenHash,
    grantId: bind.grantId,
    bindGrantToken: bind.bindGrantToken,
    alias: String(body.alias || '')
  }, group.groupTokenHash);
  return json({
    ok: true,
    deviceId: device.deviceId,
    groupId: group.groupId,
    groupToken: group.groupToken,
    familyToken: group.groupToken,
    groupTokenHash: group.groupTokenHash,
    grantId: bind.grantId,
    bindGrantToken: bind.bindGrantToken,
    commandId: command.id,
    device: publicDevice(device),
    server: capabilities(options)
  });
}

async function listDevices(request, options) {
  const { groupId } = await requireGroup(request);
  const devices = [...(state.groupDevices.get(groupId) || [])]
    .map((deviceId) => state.devices.get(deviceId))
    .filter(Boolean)
    .map(publicDevice);
  devices.sort((a, b) => String(a.name).localeCompare(String(b.name)));
  return json({ ok: true, devices, server: capabilities(options) });
}

async function pollDevice(request, options) {
  const body = await readJson(request);
  const { device } = await requireDevice(request, body);
  const groupTokens = readGroupTokens(request, body);
  const groupIds = new Set(deviceGroupIds(device));
  for (const token of groupTokens) {
    const group = await groupFromToken(request, token);
    groupIds.add(group.groupId);
    linkDevice(group.groupId, device.deviceId);
  }
  device.groupId = [...groupIds][0] || null;
  device.groupIds = [...groupIds];
  device.lastSeen = Date.now();
  device.updatedAt = Date.now();
  state.devices.set(device.deviceId, device);

  const queue = state.queues.get(device.deviceId) || [];
  while (queue.length) {
    const commandId = queue.shift();
    const command = state.commands.get(commandId);
    if (!command || isExpired(command.createdAt, COMMAND_TTL_MS)) continue;
    command.status = 'delivered';
    command.deliveredAt = Date.now();
    state.commands.set(command.id, command);
    return json({ ok: true, command, server: capabilities(options) });
  }
  state.queues.set(device.deviceId, queue);
  return json({ ok: true, command: null, server: capabilities(options) });
}

async function createCommand(request) {
  const body = await readJson(request);
  const group = await requireGroup(request, body);
  const targetDeviceId = cleanId(body.targetDeviceId);
  const type = String(body.type || '');
  const payload = body.payload || {};
  if (type === 'remote.profile.addGroup') {
    if (!payload.bindGrantToken || !payload.groupToken) throw httpError(400, 'Missing bootstrap payload');
    const bootstrapGroup = await groupFromToken(request, payload.groupToken);
    if (bootstrapGroup.groupId !== group.groupId) throw httpError(400, 'Bootstrap group token mismatch');
    payload.groupId = bootstrapGroup.groupId;
    payload.groupTokenHash = bootstrapGroup.groupTokenHash;
  } else {
    requireTargetIfKnown(group.groupId, targetDeviceId);
    payload.groupId = group.groupId;
    payload.groupTokenHash = group.groupTokenHash;
  }
  const command = enqueueCommand(group.groupId, targetDeviceId, type, payload, group.groupTokenHash);
  return json({ ok: true, commandId: command.id, command });
}

async function getCommand(request, options, commandId) {
  const { groupId } = await requireGroup(request);
  const command = state.commands.get(cleanId(commandId));
  if (!command || command.groupId !== groupId) throw httpError(404, 'Command not found');
  return json({ ok: true, command, server: capabilities(options) });
}

async function commandResult(request, commandId) {
  const body = await readJson(request);
  const { device } = await requireDevice(request, body);
  const command = state.commands.get(cleanId(commandId));
  if (!command) throw httpError(404, 'Command not found');
  if (command.targetDeviceId !== device.deviceId) throw httpError(403, 'Command target mismatch');
  command.status = body.ok === false ? 'failed' : 'done';
  command.result = body;
  command.finishedAt = Date.now();
  state.commands.set(command.id, command);
  return json({ ok: true });
}

async function createSync(request, options) {
  const body = await readJson(request);
  const { groupId, groupTokenHash } = await requireGroup(request, body);
  const sourceDeviceId = cleanId(body.sourceDeviceId);
  const targetDeviceId = cleanId(body.targetDeviceId);
  requireTargetIfKnown(groupId, sourceDeviceId);
  requireTargetIfKnown(groupId, targetDeviceId);
  if (sourceDeviceId === targetDeviceId) throw httpError(400, 'Source and target device must be different');

  const syncId = `sync_${randomId(20)}`;
  const origin = new URL(request.url).origin;
  const sync = {
    id: syncId,
    groupId,
    groupTokenHash,
    sourceDeviceId,
    targetDeviceId,
    options: normalizeSyncOptions(body.options || {}),
    status: 'created',
    parts: {},
    createdAt: Date.now(),
    updatedAt: Date.now()
  };
  state.syncs.set(syncId, sync);

  const command = enqueueCommand(groupId, sourceDeviceId, 'remoteSync.export', {
    syncId,
    targetDeviceId,
    options: sync.options,
    uploadBase: `${origin}/api/sync/${syncId}/part`,
    completeUrl: `${origin}/api/sync/${syncId}/export-complete`,
    groupId,
    groupTokenHash
  }, groupTokenHash);
  sync.exportCommandId = command.id;
  return json({ ok: true, sync, exportCommandId: command.id, server: capabilities(options) });
}

async function getSync(request, syncId) {
  const { groupId } = await requireGroup(request);
  const sync = state.syncs.get(cleanId(syncId));
  if (!sync || sync.groupId !== groupId) throw httpError(404, 'Sync not found');
  return json({ ok: true, sync });
}

async function uploadSyncPart(request, syncId, part) {
  part = normalizePart(part);
  const { device } = await requireDevice(request);
  const sync = getSyncForDevice(cleanId(syncId), device.deviceId);
  if (sync.sourceDeviceId !== device.deviceId) throw httpError(403, 'Only source device can upload sync parts');
  const contentLength = Number(request.headers.get('content-length') || 0);
  if (contentLength > MAX_SYNC_PART_BYTES) throw httpError(413, 'Sync part is too large for online relay');

  const bytes = await request.arrayBuffer();
  if (bytes.byteLength > MAX_SYNC_PART_BYTES) throw httpError(413, 'Sync part is too large for online relay');

  const key = partKey(sync.id, part);
  const contentType = request.headers.get('content-type') || contentTypeForPart(part);
  state.parts.set(key, { bytes, contentType, size: bytes.byteLength, uploadedAt: Date.now() });
  sync.parts[part] = { key, size: bytes.byteLength, contentType, uploadedAt: Date.now() };
  sync.status = 'exporting';
  sync.updatedAt = Date.now();
  state.syncs.set(sync.id, sync);
  return json({ ok: true, part, size: bytes.byteLength });
}

async function downloadSyncPart(request, syncId, part) {
  part = normalizePart(part);
  const { device } = await requireDevice(request);
  const sync = getSyncForDevice(cleanId(syncId), device.deviceId);
  const info = sync.parts && sync.parts[part];
  if (!info) throw httpError(404, 'Sync part not found');
  const stored = state.parts.get(info.key);
  if (!stored) throw httpError(404, 'Sync part expired');
  return new Response(stored.bytes.slice(0), {
    headers: {
      'content-type': stored.contentType || contentTypeForPart(part),
      'content-length': String(stored.size || 0),
      'cache-control': 'no-store'
    }
  });
}

async function exportComplete(request, options, syncId) {
  const body = await readJson(request);
  const { device } = await requireDevice(request, body);
  const sync = getSyncForDevice(cleanId(syncId), device.deviceId);
  if (sync.sourceDeviceId !== device.deviceId) throw httpError(403, 'Only source device can finish export');
  sync.status = 'exported';
  sync.exportResult = body;
  sync.updatedAt = Date.now();

  const origin = new URL(request.url).origin;
  const downloads = {};
  for (const part of Object.keys(sync.parts || {})) downloads[part] = `${origin}/api/sync/${sync.id}/part/${part}`;
  const command = enqueueCommand(sync.groupId, sync.targetDeviceId, 'remoteSync.restore', {
    syncId: sync.id,
    sourceDeviceId: sync.sourceDeviceId,
    options: sync.options,
    parts: sync.parts,
    downloads,
    completeUrl: `${origin}/api/sync/${sync.id}/restore-complete`,
    groupId: sync.groupId,
    groupTokenHash: sync.groupTokenHash
  }, sync.groupTokenHash);
  sync.restoreCommandId = command.id;
  state.syncs.set(sync.id, sync);
  return json({ ok: true, restoreCommandId: command.id, server: capabilities(options) });
}

async function restoreComplete(request, syncId) {
  const body = await readJson(request);
  const { device } = await requireDevice(request, body);
  const sync = getSyncForDevice(cleanId(syncId), device.deviceId);
  if (sync.targetDeviceId !== device.deviceId) throw httpError(403, 'Only target device can finish restore');
  sync.status = body.ok === false ? 'restore_failed' : 'done';
  sync.restoreResult = body;
  sync.updatedAt = Date.now();
  state.syncs.set(sync.id, sync);
  if (body.ok !== false) deleteSyncParts(sync);
  return json({ ok: true });
}

function enqueueCommand(groupId, targetDeviceId, type, payload, groupTokenHash) {
  if (!type) throw httpError(400, 'Missing command type');
  const id = `cmd_${randomId(20)}`;
  const command = { id, groupId, groupTokenHash, targetDeviceId, type, payload, status: 'queued', createdAt: Date.now() };
  state.commands.set(id, command);
  const queue = state.queues.get(targetDeviceId) || [];
  queue.push(id);
  state.queues.set(targetDeviceId, queue);
  return command;
}

function requireTargetIfKnown(groupId, deviceId) {
  if (!deviceId) throw httpError(400, 'Missing deviceId');
  const device = state.devices.get(deviceId);
  if (device && !deviceInGroup(device, groupId)) throw httpError(404, 'Device is not bound to this group');
  return device;
}

function getSyncForDevice(syncId, deviceId) {
  const sync = state.syncs.get(syncId);
  if (!sync) throw httpError(404, 'Sync not found');
  if (sync.sourceDeviceId !== deviceId && sync.targetDeviceId !== deviceId) throw httpError(403, 'Device is not part of this sync');
  return sync;
}

async function requireDevice(request, body = {}) {
  const origin = serverOrigin(request);
  const deviceToken = readDeviceToken(request, body);
  if (!deviceToken) throw httpError(401, 'Missing device credentials');
  const deviceId = await requireDerivedId('dev', origin, deviceToken, body.deviceId || request.headers.get('x-device-id'), 'Invalid device token');
  let device = state.devices.get(deviceId);
  if (!device) {
    const now = Date.now();
    device = {
      deviceId,
      groupId: null,
      groupIds: [],
      name: String(body.name || 'WebHTV'),
      alias: String(body.alias || ''),
      role: String(body.role || 'app'),
      type: body.type ?? 0,
      appVersion: String(body.appVersion || ''),
      capabilities: body.capabilities || {},
      lastSeen: now,
      createdAt: now,
      updatedAt: now
    };
    state.devices.set(deviceId, device);
  }
  return { device };
}

async function requireGroup(request, body = {}) {
  const groupToken = readGroupToken(request, body);
  if (!groupToken) throw httpError(401, 'Missing group token');
  return groupFromToken(request, groupToken);
}

function readGroupToken(request, body = {}) {
  return String(body.groupToken || body.familyToken || request?.headers.get('x-group-token') || request?.headers.get('x-family-token') || bearer(request) || '').trim();
}

function readDeviceToken(request, body = {}) {
  return String(body.deviceToken || body.deviceSecret || request?.headers.get('x-device-token') || bearer(request) || '').trim();
}

function readGroupTokens(request, body = {}) {
  const result = [];
  const direct = readGroupToken(null, body);
  if (direct) result.push(direct);
  if (Array.isArray(body.groups)) {
    for (const item of body.groups) {
      const token = typeof item === 'string' ? item : item && (item.groupToken || item.familyToken);
      if (token) result.push(String(token).trim());
    }
  }
  const seen = new Set();
  return result.filter((token) => token && !seen.has(token) && seen.add(token));
}

async function groupFromToken(request, groupToken) {
  groupToken = String(groupToken || '').trim();
  if (!groupToken) throw httpError(401, 'Missing group token');
  const origin = serverOrigin(request);
  const groupId = await deriveId('grp', origin, groupToken);
  const groupTokenHash = await hashText(`${origin}:${groupToken}`);
  return { groupId, groupToken, familyToken: groupToken, groupTokenHash };
}

async function requireDerivedId(prefix, origin, token, id, message) {
  const expected = await deriveId(prefix, origin, token);
  const actual = cleanId(id);
  if (actual && actual !== expected) throw httpError(401, message);
  return expected;
}

function linkDevice(groupId, deviceId) {
  const devices = state.groupDevices.get(groupId) || new Set();
  devices.add(deviceId);
  state.groupDevices.set(groupId, devices);
}

function addGroupToDevice(device, groupId) {
  const groupIds = new Set(deviceGroupIds(device));
  if (groupId) groupIds.add(groupId);
  device.groupIds = [...groupIds];
  device.groupId = device.groupIds[0] || null;
  return device;
}

function deviceInGroup(device, groupId) {
  return deviceGroupIds(device).includes(groupId);
}

function deviceGroupIds(device) {
  if (!device) return [];
  const groupIds = new Set();
  if (Array.isArray(device.groupIds)) for (const id of device.groupIds) if (id) groupIds.add(id);
  if (device.groupId) groupIds.add(device.groupId);
  if (device.ownerId) groupIds.add(device.ownerId);
  return [...groupIds];
}

function normalizeSyncOptions(options) {
  return {
    config: options.config !== false,
    loginState: options.loginState !== false,
    spider: options.spider !== false,
    webHome: options.webHome !== false,
    search: options.search !== false,
    keep: options.keep !== false,
    history: options.history !== false,
    settings: options.settings === true,
    paths: typeof options.paths === 'string' ? options.paths : undefined
  };
}

function normalizePart(part) {
  part = String(part || '').replace(/\.zip$|\.json$/g, '');
  if (!PARTS.has(part)) throw httpError(400, 'Invalid sync part');
  return part;
}

function deleteSyncParts(sync) {
  for (const info of Object.values(sync.parts || {})) if (info.key) state.parts.delete(info.key);
}

function publicDevice(device) {
  return {
    deviceId: device.deviceId,
    name: device.alias || device.name,
    rawName: device.name,
    role: device.role,
    type: device.type,
    appVersion: device.appVersion,
    lastSeen: device.lastSeen,
    online: Date.now() - Number(device.lastSeen || 0) < 45_000,
    capabilities: device.capabilities || {}
  };
}

async function readJson(request) {
  const text = await request.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (e) {
    throw httpError(400, 'Invalid JSON body');
  }
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store'
    }
  });
}

function cors(response) {
  const headers = new Headers(response.headers);
  headers.set('access-control-allow-origin', '*');
  headers.set('access-control-allow-methods', 'GET,POST,OPTIONS');
  headers.set('access-control-allow-headers', 'authorization,content-type,x-webhtv-origin,x-device-id,x-device-token,x-group-token,x-family-token');
  headers.set('access-control-max-age', '86400');
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function cleanup() {
  const now = Date.now();
  if (now - state.lastCleanup < 30_000) return;
  state.lastCleanup = now;

  for (const [code, item] of state.bindCodes) if (item.expiresAt < now) state.bindCodes.delete(code);
  for (const [id, command] of state.commands) if (isExpired(command.createdAt, COMMAND_TTL_MS)) state.commands.delete(id);
  for (const [id, sync] of state.syncs) {
    if (!isExpired(sync.createdAt, SYNC_TTL_MS)) continue;
    deleteSyncParts(sync);
    state.syncs.delete(id);
  }
  for (const [deviceId, queue] of state.queues) {
    state.queues.set(deviceId, queue.filter((id) => state.commands.has(id)));
  }
}

function isExpired(time, ttl) {
  return Date.now() - Number(time || 0) > ttl;
}

function httpError(status, message) {
  const error = new Error(message);
  error.status = status;
  throw error;
}

function bearer(request) {
  const value = request?.headers.get('authorization') || '';
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : '';
}

function cleanId(value) {
  return String(value || '').replace(/[^A-Za-z0-9_.:-]/g, '').slice(0, 80);
}

function partKey(syncId, part) {
  return `sync:${syncId}:${part}`;
}

function contentTypeForPart(part) {
  return part === 'backup' || part === 'manifest' ? 'application/json; charset=utf-8' : 'application/zip';
}

function randomId(bytes = 16) {
  const array = new Uint8Array(bytes);
  crypto.getRandomValues(array);
  return [...array].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function randomToken(bytes = 32) {
  return randomId(bytes);
}

function randomCapability(prefix) {
  return `${prefix}_${randomToken(32)}`;
}

function serverOrigin(request) {
  const declared = normalizeOrigin(request.headers.get('x-webhtv-origin'));
  if (declared) return declared;
  return new URL(request.url).origin;
}

function normalizeOrigin(value) {
  value = String(value || '').trim();
  if (!value) return '';
  try {
    const url = new URL(value);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return '';
    return url.origin.toLowerCase();
  } catch {
    return '';
  }
}

async function deriveId(prefix, origin, token) {
  return `${prefix}_${(await hashText(`${origin}:${token}`)).slice(0, 24)}`;
}

async function hashText(text) {
  const data = new TextEncoder().encode(text);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return [...new Uint8Array(hash)].map((b) => b.toString(16).padStart(2, '0')).join('');
}


// ========== playback-sync.js ==========
const PLAYBACK_SYNC_PATHS = new Set(['/api/playback/sync', '/playback/sync']);
const TOMBSTONE_RETENTION_MS = 90 * 24 * 60 * 60 * 1000;
const CLEANUP_INTERVAL_MS = 24 * 60 * 60 * 1000;
const MAX_BODY_BYTES = 128 * 1024;
const MAX_BATCH_ITEMS = 100;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const PLAYBACK_SCHEMA = 'webhtv.playback.v1';

function isPlaybackSyncPath(pathname) {
  const path = normalizePath(pathname);
  if (PLAYBACK_SYNC_PATHS.has(path)) return true;
  for (const base of PLAYBACK_SYNC_PATHS) if (path === `${base}/status`) return true;
  return false;
}

async function handlePlaybackSyncGateway(request, env) {
  if (request.method === 'OPTIONS') return playbackCors(new Response(null, { status: 204 }));
  if (!env || !env.PLAYBACK_DO) return playbackError(503, 'PLAYBACK_DO is not configured');

  const token = playbackToken(request);
  if (!token) return playbackError(401, 'Missing X-WebHTV-Token');
  if (token.length > 512) return playbackError(400, 'X-WebHTV-Token is too long');

  const namespace = `user-${await sha256(token)}`;
  return env.PLAYBACK_DO.getByName(namespace).fetch(request);
}

export class WebHTVPlaybackSyncDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sql = state.storage.sql;
    this.ready = state.blockConcurrencyWhile(async () => this.migrate());
  }

  async fetch(request) {
    await this.ready;
    if (request.method === 'OPTIONS') return playbackCors(new Response(null, { status: 204 }));
    try {
      this.cleanup();
      const url = new URL(request.url);
      const path = normalizePath(url.pathname);
      const status = [...PLAYBACK_SYNC_PATHS].some((base) => path === `${base}/status`);
      if (status) {
        if (request.method === 'GET') return playbackCors(this.status(request, url));
        return playbackError(405, 'Method not allowed');
      }
      if (!PLAYBACK_SYNC_PATHS.has(path)) return playbackError(404, 'Not found');
      if (request.method === 'GET') return playbackCors(this.pull(request, url));
      if (request.method === 'POST') return playbackCors(await this.ingest(request));
      return playbackError(405, 'Method not allowed');
    } catch (error) {
      const status = Number(error && error.status) || 500;
      if (status >= 400 && status < 500) return playbackError(status, error && error.message ? error.message : 'Invalid request');
      console.error('Playback sync request failed', error && error.stack ? error.stack : error);
      return playbackError(500, 'Internal server error');
    }
  }

  migrate() {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS playback_meta (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
      );
      INSERT OR IGNORE INTO playback_meta (key, value) VALUES ('sequence', 0);
      INSERT OR IGNORE INTO playback_meta (key, value) VALUES ('last_cleanup', 0);

      CREATE TABLE IF NOT EXISTS playback_items (
        config_key TEXT NOT NULL,
        item_key TEXT NOT NULL,
        history_key TEXT NOT NULL,
        site_key TEXT NOT NULL,
        vod_id TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (config_key, item_key)
      );
      CREATE INDEX IF NOT EXISTS idx_playback_items_config_seq
        ON playback_items (config_key, seq);

      CREATE TABLE IF NOT EXISTS playback_tombstones (
        config_key TEXT NOT NULL,
        marker_key TEXT NOT NULL,
        scope TEXT NOT NULL,
        history_key TEXT NOT NULL,
        site_key TEXT NOT NULL,
        vod_id TEXT NOT NULL,
        deleted_at INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (config_key, marker_key)
      );
      CREATE INDEX IF NOT EXISTS idx_playback_tombstones_config_seq
        ON playback_tombstones (config_key, seq);
      CREATE INDEX IF NOT EXISTS idx_playback_tombstones_deleted_at
        ON playback_tombstones (deleted_at);

      CREATE TABLE IF NOT EXISTS playback_events (
        config_key TEXT NOT NULL,
        event_id TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        PRIMARY KEY (config_key, event_id)
      );
      CREATE INDEX IF NOT EXISTS idx_playback_events_received_at
        ON playback_events (received_at);
    `);
  }

  async ingest(request) {
    const body = await readPlaybackJson(request);
    const configKey = requireConfigKey(request, body);
    const rawEvents = extractPlaybackEvents(body);
    if (!rawEvents.length) throw playbackHttpError(400, 'Playback event is empty');
    if (rawEvents.length > MAX_BATCH_ITEMS) throw playbackHttpError(413, `Too many playback events; maximum is ${MAX_BATCH_ITEMS}`);

    const sharedEventId = rawEvents.length === 1
      ? cleanString(request.headers.get('x-webhtv-webhook-id') || request.headers.get('idempotency-key'), 160)
      : '';
    const now = Date.now();
    // Validate the entire batch before applying any item so a malformed item cannot
    // leave earlier records committed while the request itself returns an error.
    const events = rawEvents.map((raw) => normalizePlaybackEvent(raw, configKey, now, sharedEventId));
    const results = events.map((event) => event.kind === 'delete' ? this.applyDelete(event, now) : this.applyUpsert(event, now));
    return playbackJson({
      ok: true,
      received: results.length,
      applied: results.filter((item) => item.action === 'created' || item.action === 'updated' || item.action === 'deleted').length,
      skipped: results.filter((item) => item.action === 'skipped' || item.action === 'duplicate').length,
      results
    });
  }

  pull(request, url) {
    const configKey = requireConfigKey(request);
    const since = parseCursor(request.headers.get('x-webhtv-since') || url.searchParams.get('since'));
    const limit = parseLimit(request.headers.get('x-webhtv-limit') || url.searchParams.get('limit'));
    const cutoff = Date.now() - TOMBSTONE_RETENTION_MS;
    const rows = this.sql.exec(`
      SELECT seq, kind, payload FROM (
        SELECT seq, 'upsert' AS kind, payload
          FROM playback_items
         WHERE config_key = ? AND seq > ?
        UNION ALL
        SELECT seq, 'delete' AS kind, payload
          FROM playback_tombstones
         WHERE config_key = ? AND deleted_at >= ? AND seq > ?
      )
      ORDER BY seq ASC
      LIMIT ?
    `, configKey, since, configKey, cutoff, since, limit + 1).toArray();

    const hasMore = rows.length > limit;
    const selected = hasMore ? rows.slice(0, limit) : rows;
    const changes = [];
    for (const row of selected) {
      try {
        changes.push(JSON.parse(row.payload));
      } catch {
        // Ignore an individually corrupted row without breaking all other records.
      }
    }
    const nextSince = selected.length ? String(selected[selected.length - 1].seq) : String(since);
    return playbackJson({ changes, nextSince, hasMore });
  }

  status(request, url) {
    const configKey = requireConfigKey(request);
    const cutoff = Date.now() - TOMBSTONE_RETENTION_MS;
    const items = this.sql.exec('SELECT COUNT(*) AS count FROM playback_items WHERE config_key = ?', configKey).one();
    const tombstones = this.sql.exec('SELECT COUNT(*) AS count FROM playback_tombstones WHERE config_key = ? AND deleted_at >= ?', configKey, cutoff).one();
    const latest = this.sql.exec(`
      SELECT COALESCE(MAX(seq), 0) AS seq FROM (
        SELECT seq FROM playback_items WHERE config_key = ?
        UNION ALL
        SELECT seq FROM playback_tombstones WHERE config_key = ? AND deleted_at >= ?
      )
    `, configKey, configKey, cutoff).one();
    return playbackJson({
      ok: true,
      configKey,
      items: Number(items.count || 0),
      tombstones: Number(tombstones.count || 0),
      nextSince: String(latest.seq || 0),
      retentionDays: 90,
      endpoint: `${url.origin}${basePlaybackPath(url.pathname)}`
    });
  }

  applyUpsert(event, receivedAt) {
    return this.state.storage.transactionSync(() => {
      if (event.eventId && this.hasEvent(event.configKey, event.eventId)) {
        return resultFor(event, 'duplicate', 0, 'Event already processed');
      }

      const current = firstRow(this.sql.exec(
        'SELECT updated_at, seq FROM playback_items WHERE config_key = ? AND item_key = ?',
        event.configKey,
        event.itemKey
      ));
      const tombstone = firstRow(this.sql.exec(`
        SELECT MAX(deleted_at) AS deleted_at, MAX(seq) AS seq
          FROM playback_tombstones
         WHERE config_key = ? AND (
           scope = 'all'
           OR (scope = 'site' AND site_key = ?)
           OR (scope = 'item' AND ((site_key = ? AND vod_id = ?) OR (history_key <> '' AND history_key = ?)))
         )
      `, event.configKey, event.siteKey, event.siteKey, event.vodId, event.historyKey));
      const deletedAt = Number(tombstone?.deleted_at || 0);
      if (deletedAt > 0 && event.updatedAt <= deletedAt) {
        this.recordEvent(event.configKey, event.eventId, receivedAt);
        return resultFor(event, 'skipped', Number(tombstone?.seq || 0), 'A newer deletion exists');
      }
      if (current && event.updatedAt <= Number(current.updated_at || 0)) {
        this.recordEvent(event.configKey, event.eventId, receivedAt);
        return resultFor(event, 'skipped', Number(current.seq || 0), 'A newer progress record exists');
      }

      const seq = this.nextSequence();
      const payload = JSON.stringify(event.payload);
      this.sql.exec(`
        INSERT INTO playback_items
          (config_key, item_key, history_key, site_key, vod_id, updated_at, seq, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(config_key, item_key) DO UPDATE SET
          history_key = excluded.history_key,
          site_key = excluded.site_key,
          vod_id = excluded.vod_id,
          updated_at = excluded.updated_at,
          seq = excluded.seq,
          payload = excluded.payload
      `, event.configKey, event.itemKey, event.historyKey, event.siteKey, event.vodId, event.updatedAt, seq, payload);
      this.recordEvent(event.configKey, event.eventId, receivedAt);
      return resultFor(event, current ? 'updated' : 'created', seq, '');
    });
  }

  applyDelete(event, receivedAt) {
    return this.state.storage.transactionSync(() => {
      if (event.eventId && this.hasEvent(event.configKey, event.eventId)) {
        return resultFor(event, 'duplicate', 0, 'Event already processed');
      }

      const current = firstRow(this.sql.exec(
        'SELECT deleted_at, seq FROM playback_tombstones WHERE config_key = ? AND marker_key = ?',
        event.configKey,
        event.markerKey
      ));
      if (current && event.deletedAt <= Number(current.deleted_at || 0)) {
        this.recordEvent(event.configKey, event.eventId, receivedAt);
        return resultFor(event, 'skipped', Number(current.seq || 0), 'A newer deletion exists');
      }

      const seq = this.nextSequence();
      const payload = JSON.stringify(event.payload);
      this.sql.exec(`
        INSERT INTO playback_tombstones
          (config_key, marker_key, scope, history_key, site_key, vod_id, deleted_at, seq, payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(config_key, marker_key) DO UPDATE SET
          scope = excluded.scope,
          history_key = excluded.history_key,
          site_key = excluded.site_key,
          vod_id = excluded.vod_id,
          deleted_at = excluded.deleted_at,
          seq = excluded.seq,
          payload = excluded.payload
      `, event.configKey, event.markerKey, event.scope, event.historyKey, event.siteKey, event.vodId, event.deletedAt, seq, payload);

      let deletedRows = 0;
      if (event.scope === 'all') {
        deletedRows = this.sql.exec(
          'DELETE FROM playback_items WHERE config_key = ? AND updated_at <= ?',
          event.configKey,
          event.deletedAt
        ).rowsWritten;
      } else if (event.scope === 'site') {
        deletedRows = this.sql.exec(
          'DELETE FROM playback_items WHERE config_key = ? AND site_key = ? AND updated_at <= ?',
          event.configKey,
          event.siteKey,
          event.deletedAt
        ).rowsWritten;
      } else {
        deletedRows = this.sql.exec(`
          DELETE FROM playback_items
           WHERE config_key = ? AND updated_at <= ?
             AND (item_key = ? OR (history_key <> '' AND history_key = ?))
        `, event.configKey, event.deletedAt, event.itemKey, event.historyKey).rowsWritten;
      }
      this.recordEvent(event.configKey, event.eventId, receivedAt);
      return { ...resultFor(event, 'deleted', seq, ''), affected: Number(deletedRows || 0) };
    });
  }

  nextSequence() {
    this.sql.exec("UPDATE playback_meta SET value = value + 1 WHERE key = 'sequence'");
    return Number(this.sql.exec("SELECT value FROM playback_meta WHERE key = 'sequence'").one().value || 0);
  }

  hasEvent(configKey, eventId) {
    if (!eventId) return false;
    return Boolean(firstRow(this.sql.exec(
      'SELECT 1 AS found FROM playback_events WHERE config_key = ? AND event_id = ? LIMIT 1',
      configKey,
      eventId
    )));
  }

  recordEvent(configKey, eventId, receivedAt) {
    if (!eventId) return;
    this.sql.exec(
      'INSERT OR IGNORE INTO playback_events (config_key, event_id, received_at) VALUES (?, ?, ?)',
      configKey,
      eventId,
      receivedAt
    );
  }

  cleanup() {
    const now = Date.now();
    const last = Number(this.sql.exec("SELECT value FROM playback_meta WHERE key = 'last_cleanup'").one().value || 0);
    if (now - last < CLEANUP_INTERVAL_MS) return;
    const cutoff = now - TOMBSTONE_RETENTION_MS;
    this.state.storage.transactionSync(() => {
      this.sql.exec('DELETE FROM playback_tombstones WHERE deleted_at < ?', cutoff);
      this.sql.exec('DELETE FROM playback_events WHERE received_at < ?', cutoff);
      this.sql.exec("UPDATE playback_meta SET value = ? WHERE key = 'last_cleanup'", now);
    });
  }
}

function normalizePlaybackEvent(input, configKey, now = Date.now(), fallbackEventId = '') {
  const raw = unwrapPlaybackEvent(input);
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw playbackHttpError(400, 'Invalid playback event');
  configKey = validatedConfigKey(configKey, 'Missing X-WebHTV-Config-Key');
  const bodyConfigKey = normalizeConfigKey(raw.configKey || raw.config_key);
  if (bodyConfigKey.length > 256) throw playbackHttpError(400, 'configKey is too long');
  if (bodyConfigKey && bodyConfigKey !== configKey) throw playbackHttpError(400, 'configKey does not match X-WebHTV-Config-Key');

  const eventName = cleanString(raw.event, 80).toLowerCase();
  const action = cleanString(raw.action || raw.op || raw.operation, 32).toLowerCase();
  const deletion = booleanValue(raw.deleted) || eventName === 'playback.deleted'
    || action === 'delete' || action === 'deleted' || action === 'remove' || action === 'removed';
  const eventId = cleanString(raw.eventId || raw.event_id || fallbackEventId, 160);
  let historyKey = cleanString(raw.historyKey || raw.key, 4096);
  const parts = historyParts(historyKey);
  let siteKey = cleanString(raw.siteKey || raw.site || raw.site_key || parts.siteKey, 1024);
  let vodId = cleanString(raw.vodId || raw.vod_id || raw.videoId || raw.itemId || parts.vodId, 8192);

  if (deletion) {
    const requestedScope = cleanString(raw.scope, 16).toLowerCase();
    if (requestedScope && !['all', 'site', 'item'].includes(requestedScope)) {
      throw playbackHttpError(400, 'scope must be item, site, or all');
    }
    const scope = normalizeScope(raw.scope, historyKey, siteKey, vodId);
    if (!scope) throw playbackHttpError(400, 'scope=all must be explicit when no item or site identity is provided');
    if (scope === 'site' && !siteKey) throw playbackHttpError(400, 'siteKey is required for a site deletion');
    if (scope === 'item' && !historyKey && (!siteKey || !vodId)) throw playbackHttpError(400, 'historyKey or siteKey + vodId is required for an item deletion');
    if (scope === 'all') {
      historyKey = '';
      siteKey = '';
      vodId = '';
    } else if (scope === 'site') {
      historyKey = '';
      vodId = '';
    }
    const deletedAt = positiveTimestamp(raw.deletedAt || raw.deleted_at || raw.timestamp || raw.updatedAt, 0);
    if (!deletedAt) throw playbackHttpError(400, 'deletedAt or timestamp is required for a deletion');
    const itemKey = portableItemKey(historyKey, siteKey, vodId);
    const markerKey = scope === 'all' ? 'all' : scope === 'site' ? `site\n${siteKey}` : `item\n${itemKey}`;
    const payload = compactObject({
      schema: PLAYBACK_SCHEMA,
      action: 'delete',
      event: 'playback.deleted',
      eventId,
      configKey,
      historyKey,
      siteKey,
      vodId,
      scope,
      deletedAt
    });
    return { kind: 'delete', configKey, eventId, historyKey, siteKey, vodId, scope, deletedAt, itemKey, markerKey, payload };
  }

  if (!siteKey) throw playbackHttpError(400, 'siteKey is required');
  if (!vodId) throw playbackHttpError(400, 'vodId is required');
  const vodName = cleanString(raw.vodName || raw.vod_name || raw.name || raw.title, 2048);
  const episodeName = cleanString(raw.episodeName || raw.episode || raw.episodeTitle || raw.vodRemarks || raw.remarks, 2048);
  const positionMs = positiveNumber(raw.positionMs || raw.position || raw.position_ms || raw.pos);
  const durationMs = positiveNumber(raw.durationMs || raw.duration || raw.duration_ms);
  if (!vodName) throw playbackHttpError(400, 'vodName is required');
  if (!episodeName) throw playbackHttpError(400, 'episodeName is required');
  if (positionMs <= 0) throw playbackHttpError(400, 'positionMs must be greater than 0');
  if (durationMs <= 0) throw playbackHttpError(400, 'durationMs must be greater than 0');
  const updatedAt = positiveTimestamp(raw.updatedAt || raw.updated_at || raw.timestamp || raw.updateTime, now);
  const completed = eventName === 'playback.ended' || booleanValue(raw.completed);
  const suppliedProgress = boundedNumber(raw.progress, 0, 1);
  const payload = compactObject({
    schema: PLAYBACK_SCHEMA,
    action: 'upsert',
    event: eventName || undefined,
    eventId,
    configKey,
    configName: cleanString(raw.configName || raw.config_name, 2048),
    historyKey,
    siteKey,
    siteName: cleanString(raw.siteName, 2048),
    vodId,
    vodName,
    vodPic: cleanString(raw.vodPic || raw.vod_pic || raw.pic || raw.poster, 8192),
    flag: cleanString(raw.flag || raw.vodFlag || raw.line || raw.source, 2048),
    episodeName,
    episodeUrl: cleanString(raw.episodeUrl || raw.episode_url || raw.url || raw.playUrl, 8192),
    positionMs: Math.min(positionMs, durationMs),
    durationMs,
    progress: suppliedProgress > 0 ? suppliedProgress : Math.min(positionMs, durationMs) / durationMs,
    speed: positiveNumber(raw.speed) || 1,
    completed,
    updatedAt,
    clientKey: cleanString(raw.clientKey, 256)
  });
  return {
    kind: 'upsert',
    configKey,
    eventId,
    historyKey,
    siteKey,
    vodId,
    itemKey: portableItemKey(historyKey, siteKey, vodId),
    updatedAt,
    payload
  };
}

function parseCursor(value) {
  if (value == null || String(value).trim() === '') return 0;
  const parsed = Number(String(value).trim());
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw playbackHttpError(400, 'Invalid X-WebHTV-Since cursor');
  return parsed;
}

function parseLimit(value) {
  if (value == null || String(value).trim() === '') return DEFAULT_LIMIT;
  const text = String(value).trim();
  if (!/^\d+$/.test(text)) return DEFAULT_LIMIT;
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return DEFAULT_LIMIT;
  return Math.min(MAX_LIMIT, parsed);
}

function extractPlaybackEvents(body) {
  if (Array.isArray(body)) return body;
  if (!body || typeof body !== 'object') return [];
  const changes = firstArray(body, 'changes', 'operations');
  if (changes) return changes.map((item) => inheritPlaybackFields(item, body));

  const result = [];
  const deletions = firstArray(body, 'deleted', 'deletions', 'tombstones', 'removed', 'deletedItems');
  if (deletions) {
    for (const item of deletions) {
      const value = typeof item === 'string' ? { historyKey: item } : item;
      result.push(inheritPlaybackFields({ ...value, action: 'delete' }, body));
    }
  }
  const items = firstArray(body, 'items', 'records', 'upserts', 'list');
  if (items) for (const item of items) result.push(inheritPlaybackFields(item, body));
  if (result.length) return result;
  if (Array.isArray(body.data)) return body.data.map((item) => inheritPlaybackFields(item, body));
  return [body];
}

function unwrapPlaybackEvent(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return input;
  if (!input.data || typeof input.data !== 'object' || Array.isArray(input.data)) return input;
  return inheritPlaybackFields(input.data, input);
}

function inheritPlaybackFields(input, parent) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return input;
  const output = { ...input };
  for (const key of ['action', 'op', 'operation', 'event', 'eventId', 'deleted', 'scope', 'deletedAt', 'timestamp', 'updatedAt', 'configKey', 'configName']) {
    if (output[key] == null && parent && parent[key] != null && typeof parent[key] !== 'object') output[key] = parent[key];
  }
  return output;
}

async function readPlaybackJson(request) {
  const declared = Number(request.headers.get('content-length') || 0);
  if (declared > MAX_BODY_BYTES) throw playbackHttpError(413, 'Playback payload is too large');
  const text = await request.text();
  if (new TextEncoder().encode(text).byteLength > MAX_BODY_BYTES) throw playbackHttpError(413, 'Playback payload is too large');
  if (!text.trim()) throw playbackHttpError(400, 'Playback payload is empty');
  try {
    return JSON.parse(text);
  } catch {
    throw playbackHttpError(400, 'Invalid JSON body');
  }
}

function requireConfigKey(request, body = null) {
  const header = validatedConfigKey(request.headers.get('x-webhtv-config-key'));
  const bodyKey = body && !Array.isArray(body) ? validatedConfigKey(body.configKey || body.config_key) : '';
  if (header && bodyKey && header !== bodyKey) throw playbackHttpError(400, 'configKey does not match X-WebHTV-Config-Key');
  const configKey = header || bodyKey;
  if (!configKey) throw playbackHttpError(400, 'Missing X-WebHTV-Config-Key');
  return configKey;
}

function normalizeConfigKey(value) {
  return String(value == null ? '' : value).trim().toLowerCase();
}

function validatedConfigKey(value, missingMessage = '') {
  const configKey = normalizeConfigKey(value);
  if (configKey.length > 256) throw playbackHttpError(400, 'configKey is too long');
  if (!configKey && missingMessage) throw playbackHttpError(400, missingMessage);
  return configKey;
}

function normalizeScope(value, historyKey, siteKey, vodId) {
  const scope = cleanString(value, 16).toLowerCase();
  if (scope === 'all' || scope === 'site' || scope === 'item') return scope;
  if (historyKey || (siteKey && vodId)) return 'item';
  if (siteKey) return 'site';
  return '';
}

function portableItemKey(historyKey, siteKey, vodId) {
  if (siteKey && vodId) return `${siteKey}\n${vodId}`;
  return `history\n${historyKey}`;
}

function historyParts(historyKey) {
  const parts = String(historyKey || '').split('@@@');
  return { siteKey: parts[0] || '', vodId: parts[1] || '' };
}

function positiveTimestamp(value, fallback) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number > 0 ? number : fallback;
}

function positiveNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : 0;
}

function boundedNumber(value, min, max) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Math.max(min, Math.min(max, number));
}

function booleanValue(value) {
  if (value === true || value === 1) return true;
  const text = String(value == null ? '' : value).trim().toLowerCase();
  return text === 'true' || text === '1' || text === 'yes';
}

function cleanString(value, maxLength) {
  return String(value == null ? '' : value).trim().slice(0, maxLength);
}

function compactObject(value) {
  return Object.fromEntries(Object.entries(value).filter(([, item]) => item !== undefined && item !== ''));
}

function firstArray(object, ...keys) {
  for (const key of keys) if (Array.isArray(object[key])) return object[key];
  return null;
}

function firstRow(cursor) {
  const result = cursor.next();
  return result.done ? null : result.value;
}

function resultFor(event, action, sequence, message) {
  return compactObject({
    action,
    sequence,
    message,
    eventId: event.eventId,
    configKey: event.configKey,
    historyKey: event.historyKey,
    siteKey: event.siteKey,
    vodId: event.vodId,
    updatedAt: event.updatedAt,
    deletedAt: event.deletedAt
  });
}

function playbackToken(request) {
  const direct = String(request.headers.get('x-webhtv-token') || '').trim();
  if (direct) return direct;
  const authorization = request.headers.get('authorization') || '';
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match ? String(match[1] || '').trim() : '';
}

function basePlaybackPath(pathname) {
  const path = normalizePath(pathname);
  for (const base of PLAYBACK_SYNC_PATHS) if (path === base || path === `${base}/status`) return base;
  return '/api/playback/sync';
}

function normalizePath(pathname) {
  const path = String(pathname || '').replace(/\/+$/, '');
  return path || '/';
}

async function sha256(value) {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((item) => item.toString(16).padStart(2, '0')).join('');
}

function playbackJson(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store'
    }
  });
}

function playbackError(status, message) {
  return playbackCors(playbackJson({ ok: false, error: message }, status));
}

function playbackCors(response) {
  const headers = new Headers(response.headers);
  headers.set('access-control-allow-origin', '*');
  headers.set('access-control-allow-methods', 'GET,POST,OPTIONS');
  headers.set('access-control-allow-headers', [
    'authorization',
    'content-type',
    'idempotency-key',
    'x-webhtv-token',
    'x-webhtv-config-key',
    'x-webhtv-config-name',
    'x-webhtv-timestamp',
    'x-webhtv-since',
    'x-webhtv-limit',
    'x-webhtv-webhook-id',
    'x-webhtv-dedupe-key'
  ].join(','));
  headers.set('access-control-expose-headers', '*');
  headers.set('access-control-max-age', '86400');
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function playbackHttpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}


// ========== index.js (entry) ==========


const SERVER_NAME = 'Cloudflare Worker Relay';
const RELAY_DO_NAME = 'default';
const RELAY_STATE_KEY = 'relayState';

export class WebHTVRemoteRelayDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.relayState = createRelayState();
    this.ready = state.blockConcurrencyWhile(async () => {
      const snapshot = await state.storage.get(RELAY_STATE_KEY);
      this.relayState = createRelayState(snapshot);
    });
  }

  async fetch(request) {
    await this.ready;
    const response = await handleRelayRequest(request, {
      serverName: SERVER_NAME,
      relayMode: 'cloudflare-durable-object',
      persistentStorage: true,
      playbackSync: Boolean(this.env && this.env.PLAYBACK_DO),
      playbackPersistentStorage: Boolean(this.env && this.env.PLAYBACK_DO),
      state: this.relayState
    });
    await this.saveState();
    return response;
  }

  async saveState() {
    try {
      await this.state.storage.put(RELAY_STATE_KEY, snapshotRelayState(this.relayState));
    } catch (e) {
      console.error('Failed to persist relay state', e && e.message ? e.message : e);
    }
  }
}

export default {
  async fetch(request, env) {
    if (isPlaybackSyncPath(new URL(request.url).pathname)) return handlePlaybackSyncGateway(request, env);
    if (env && env.RELAY_DO) return env.RELAY_DO.getByName(RELAY_DO_NAME).fetch(request);
    return handleRelayRequest(request, {
      serverName: SERVER_NAME,
      playbackSync: Boolean(env && env.PLAYBACK_DO),
      playbackPersistentStorage: Boolean(env && env.PLAYBACK_DO)
    });
  }
};
