/**
 * Silence watchdog + ping/pong liveness (4.0.5).
 *
 * The failure these tests reproduce (bridge outage, 2026-10-29 → 2026-10-31): a SHIP endpoint
 * behind a proxy stopped delivering blocks on an OPEN connection — no close frame, no socket
 * error. 4.0.4 reconnects only on `close`/`error` and so waited forever. These tests use a REAL
 * websocket server and the REAL Connection; only block deserialization is mocked, because the
 * frames the server sends are not real SHIP blocks.
 */
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const { Serialize } = require('eosjs');
const StateReceiver = require('../src/state-receiver');
const { deserialize } = require('../src/deserialize');
const deserializeDeep = require('../src/deserialize-deep');

jest.mock('../src/deserialize-deep');

const ABI_TEXT = fs.readFileSync(path.join(__dirname, 'abi.json'), 'utf8');
const TYPES = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), JSON.parse(ABI_TEXT));

const START_BLOCK = 1000;

function makeLogger() {
  return { info: jest.fn(), debug: jest.fn(), warn: jest.fn(), error: jest.fn() };
}

function waitFor(predicate, timeoutMs, label) {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      if (predicate()) return resolve(Date.now() - started);
      if (Date.now() - started > timeoutMs) {
        return reject(new Error(`timed out after ${timeoutMs} ms waiting for: ${label}`));
      }
      setTimeout(tick, 10);
    };
    tick();
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * A minimal SHIP-shaped server. Each accepted connection gets the ABI (as the real plugin sends
 * it), then `behaviour(ws, index)` decides what happens next. Every decoded client request is
 * recorded per connection, so a test can read the start block of each get_blocks_request.
 */
function startServer(behaviour) {
  return new Promise((resolve) => {
    const server = new WebSocket.Server({ port: 0 }, () => resolve(server));
    server.connectionsSeen = [];
    server.on('connection', (ws) => {
      const record = { ws, requests: [] };
      const index = server.connectionsSeen.push(record) - 1;
      ws.on('message', (data) => {
        try {
          record.requests.push(deserialize(TYPES, 'request', new Uint8Array(data)));
        } catch (_e) {
          record.requests.push(['undecodable', null]);
        }
      });
      ws.send(ABI_TEXT);
      behaviour(ws, index, record);
    });
  });
}

function closeServer(server) {
  return new Promise((resolve) => {
    for (const c of server.clients) c.terminate();
    server.close(() => resolve());
  });
}

/** Send `count` block frames; deserializeDeep is mocked to hand them out in block order. */
function sendBlocks(ws, count) {
  for (let i = 0; i < count; i++) ws.send(Buffer.from([0x01, i]));
}

function getBlocksRequests(record) {
  return record.requests.filter((r) => r[0] === 'get_blocks_request_v0');
}

describe('statereceiver liveness watchdog (silent stream + missing pong)', () => {
  let server;
  let receiver;
  let nextBlock;

  beforeEach(() => {
    nextBlock = START_BLOCK;
    deserializeDeep.mockImplementation(async () => {
      const n = nextBlock++;
      return [
        'get_blocks_result_v0',
        {
          this_block: { block_num: n },
          head: { block_num: n + 10 },
          last_irreversible: { block_num: n + 5 },
          block: null,
          traces: [],
        },
      ];
    });
  });

  afterEach(async () => {
    if (receiver) receiver.stop();
    receiver = null;
    if (server) await closeServer(server);
    server = null;
  });

  function makeReceiver(extra) {
    const port = server.address().port;
    receiver = new StateReceiver({
      logger: makeLogger(),
      startBlock: START_BLOCK,
      socketAddresses: [`ws://127.0.0.1:${port}`],
      eosApi: {},
      maxQueueSize: 10,
      fetchBlockTime: false,
      ...extra,
    });
    receiver.registerTraceHandler({ processTrace: async () => {} });
    return receiver;
  }

  it('(a) a server that goes silent WITHOUT closing is abandoned and the client reconnects from the current block', async () => {
    server = await startServer((ws, index) => {
      // First connection: a few blocks, then silence on an open socket (the Oct 29 failure).
      // Later connections: also silent — we only need to observe the reconnect.
      if (index === 0) setTimeout(() => sendBlocks(ws, 3), 50);
    });
    makeReceiver({ silenceTimeoutMs: 400, pingIntervalMs: 60000 });
    receiver.start();

    await waitFor(() => receiver.current_block === START_BLOCK + 2, 2000, 'three blocks delivered');
    // The first socket is still OPEN on the server side: nothing closed it.
    expect(server.connectionsSeen[0].ws.readyState).toBe(WebSocket.OPEN);

    await waitFor(() => server.connectionsSeen.length >= 2, 3000, 'a second connection');
    await waitFor(
      () => getBlocksRequests(server.connectionsSeen[1]).length >= 1,
      2000,
      'get_blocks_request on the new connection'
    );

    // Resumed from the CURRENT block, not from startBlock.
    const [, args] = getBlocksRequests(server.connectionsSeen[1])[0];
    expect(args.start_block_num).toBe(START_BLOCK + 3);

    // Exactly one clear line naming the cause, the silence and the last block.
    const lines = receiver.logger.error.mock.calls.map((c) => String(c[0]));
    const fired = lines.filter((l) => l.includes('watchdog'));
    expect(fired).toHaveLength(1);
    expect(fired[0]).toMatch(/no message for \d+ ms/);
    expect(fired[0]).toContain(`last block ${START_BLOCK + 2}`);
  });

  it('(a2) the watchdog stays armed after it fires: repeated silent connections each get abandoned', async () => {
    server = await startServer((ws) => {
      setTimeout(() => sendBlocks(ws, 1), 30);
    });
    makeReceiver({ silenceTimeoutMs: 300, pingIntervalMs: 60000 });
    receiver.start();
    await waitFor(() => server.connectionsSeen.length >= 2, 4000, 'second connection');
    await sleep(20);
    expect(receiver._silenceTimer).not.toBeNull(); // re-armed for the new socket
    await waitFor(
      () => server.connectionsSeen.length >= 3,
      4000,
      'a third connection (second watchdog fire)'
    );
    const fired = receiver.logger.error.mock.calls
      .map((c) => String(c[0]))
      .filter((l) => l.includes('watchdog'));
    expect(fired.length).toBeGreaterThanOrEqual(2);
  });

  it('(a-control) a stream that keeps delivering is never torn down', async () => {
    server = await startServer((ws) => {
      const t = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) sendBlocks(ws, 1);
      }, 50);
      ws.on('close', () => clearInterval(t));
    });
    makeReceiver({ silenceTimeoutMs: 300, pingIntervalMs: 60000 });
    receiver.start();

    await sleep(1200);
    expect(server.connectionsSeen).toHaveLength(1);
    expect(receiver.current_block).toBeGreaterThan(START_BLOCK + 5);
  });

  it('(b) a server that never answers pings is treated as dead and the client reconnects', async () => {
    server = await startServer((ws) => {
      // `ws` auto-answers pings; replacing pong() on this server-side socket suppresses that.
      ws.pong = () => {};
    });
    // Silence timeout far away, so only the ping path can explain a reconnect.
    makeReceiver({ silenceTimeoutMs: 60000, pingIntervalMs: 150 });
    receiver.start();

    await waitFor(() => server.connectionsSeen.length >= 2, 2000, 'a second connection');
    const lines = receiver.logger.error.mock.calls.map((c) => String(c[0]));
    expect(lines.filter((l) => l.includes('watchdog') && l.includes('pong'))).toHaveLength(1);
  });

  it('(b-control) a server that answers pings is kept, even when it sends no blocks', async () => {
    server = await startServer(() => {});
    makeReceiver({ silenceTimeoutMs: 60000, pingIntervalMs: 100 });
    receiver.start();

    await sleep(1000);
    expect(server.connectionsSeen).toHaveLength(1);
  });

  it('stop() clears every watchdog timer and no reconnect follows', async () => {
    server = await startServer(() => {});
    makeReceiver({ silenceTimeoutMs: 200, pingIntervalMs: 100 });
    receiver.start();
    await waitFor(() => receiver.connection && receiver.connection.connected, 2000, 'open');

    receiver.stop();
    expect(receiver._silenceTimer).toBeNull();
    expect(receiver._pingTimer).toBeNull();

    await sleep(800);
    expect(server.connectionsSeen).toHaveLength(1);
  });

  it('both intervals can be disabled with 0 (opt-out preserves 4.0.4 behaviour)', async () => {
    server = await startServer(() => {});
    makeReceiver({ silenceTimeoutMs: 0, pingIntervalMs: 0 });
    receiver.start();
    await waitFor(() => receiver.connection && receiver.connection.connected, 2000, 'open');
    expect(receiver._silenceTimer).toBeNull();
    expect(receiver._pingTimer).toBeNull();
  });

  it('defaults are 90 s silence and 20 s ping', () => {
    server = null;
    const r = new StateReceiver({ logger: makeLogger(), socketAddresses: [], eosApi: {} });
    expect(r.silenceTimeoutMs).toBe(90000);
    expect(r.pingIntervalMs).toBe(20000);
  });
});
