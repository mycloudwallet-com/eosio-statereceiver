const { Serialize } = require('eosjs');
const Connection = require('./connection');
const createEosApi = require('./create-eos-api');
const serialize = require('./serialize');
const deserializeDeep = require('./deserialize-deep');

/**
 * Default silence budget. On WAX an irreversible SHIP stream delivers a block about every 0.5 s,
 * so 90 s of nothing on an open socket is ~180 missed blocks: never a slow stream, always a dead
 * one. Chosen well above the 30 s handshake budget so the two timeouts never race.
 */
const DEFAULT_SILENCE_TIMEOUT_MS = 90000;

/** Default websocket ping cadence. A ping with no pong by the next tick marks the socket dead. */
const DEFAULT_PING_INTERVAL_MS = 20000;

/**
 * @typedef {import('eosjs').Api} EosApi
 */

/**
 * @callback ProcessTrace
 * @param {number} block_num
 * @param {Array<*>} traces
 * @param {string} block_time
 * @returns {Promise<void>}
 */

/**
 * @typedef {Object} TraceHandler
 * @property {string} contractName interest in contract name
 * @property {string} actionName interest in action name
 * @property {ProcessTrace} processTrace handler function
 */

class StateReceiver {
  /**
   * @type {Connection}
   */
  connection;

  /**
   * @type {EosApi}
   */
  eosApi;

  /**
   * @type {Array<Buffer>}
   */
  serializedMessageQueue;

  /**
   * @type {Set<string>}
   */
  deserializerActionSet;

  /**
   * @type {number}
   */
  inflightMessageCount = 0;

  /**
   * @type {boolean}
   */
  fetchBlockTime;

  /**
   * @param {Object} config - StateReceiver configuration
   * @param {number} config.startBlock - default 0,
   * @param {number} config.endBlock - default 0xffffffff
   * @param {string[]} config.socketAddresses - websoket endpoint (state history node)
   * @param {EosApi} config.eosApi - EosApi object. This is for deserializing action data. If it is not provided, it will be created.
   * @param {string} config.eosEndpoint - endpoint of eos history node. This is required for deserializing action data.
   * @param {object} config.logger - default is console
   * @param {function} config.onError - error handler
   * @param {number} config.maxQueueSize - max buffer message size, default 100
   * @param {boolean} config.fetchBlockTime - fetch block time, default true
   * @param {string[]} config.deserializerActions - list of actions to be deserialized. Ex: ['eosio.token::transfer', 'bridge.wax::reqnft']
   * @param {number} [config.silenceTimeoutMs] - reconnect when no message has arrived for this long while the receiver is idle, default 90000. 0 disables.
   * @param {number} [config.pingIntervalMs] - send a websocket ping this often; no pong before the next ping means the socket is dead, default 20000. 0 disables.
   */
  constructor(config) {
    this.logger = config.logger || console;

    this.traceHandlers = [];

    if (!config.maxQueueSize) {
      config.maxQueueSize = 100;
    }

    this.config = Object.freeze(config);

    this.deserializerActionSet = new Set(config.deserializerActions || []);
    this.startBlock = config.startBlock || 0;
    this.endBlock = config.endBlock || '0xffffffff';
    this.current_block = -1;
    this.types = null;
    this.processingMessageData = false;
    /**
     * Incremented on every start()/reconnect. A processMessageData() invocation captures this
     * value and abandons itself as soon as it no longer matches, so a batch belonging to a dead
     * connection can never deliver blocks, ACK, or clear the in-progress flag of a live one.
     */
    this._connectionEpoch = 0;
    this.fetchBlockTime = config.fetchBlockTime === undefined ? true : config.fetchBlockTime;

    this.silenceTimeoutMs = Number(config.silenceTimeoutMs ?? DEFAULT_SILENCE_TIMEOUT_MS);
    this.pingIntervalMs = Number(config.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS);
    this._silenceTimer = null;
    this._pingTimer = null;
    this._lastActivityAt = 0;
    this._awaitingPong = false;

    this.init();

    if (!config.eosApi) {
      this.logger.info(`Creating eosApi with endpoint: ${config.eosEndpoint}`);
      this.eosApi = createEosApi(config.eosEndpoint);
    } else {
      this.eosApi = config.eosApi;
    }
    this.processCount = 0;
    this.debuging = process.env.DEBUG_STATE_RECEIVER == 1;
    /** One get_blocks_ack per processed message (num_messages:1). Fixes throughput with some state-history nodes. */
    this._ackEachBlock = process.env.WAX_STATE_HISTORY_ACK_EACH_BLOCK === '1';
    if (this._ackEachBlock) {
      this.logger.info(
        '[statereceiver] WAX_STATE_HISTORY_ACK_EACH_BLOCK=1: per-message ACK pacing enabled.'
      );
    }
    this._lastAckBackpressureLog = 0;
    this._ackBackpressureLogMs = parseInt(
      process.env.WAX_STATE_HISTORY_ACK_BACKPRESSURE_LOG_MS || '5000',
      10
    );
    this._drainBackoffMs = parseInt(process.env.WAX_STATE_HISTORY_DRAIN_BACKOFF_MS || '250', 10);
    const statsEnv = (process.env.WAX_STATE_HISTORY_STATS_LOG || '').toLowerCase();
    this._statsLog = statsEnv !== '0' && statsEnv !== 'false' && statsEnv !== 'off';
    this._statsIntervalMs = parseInt(process.env.WAX_STATE_HISTORY_STATS_INTERVAL_MS || '1000', 10);
    this._statsWindowStartHr = null;
    this._statsBlocksInWindow = 0;
  }

  init() {
    this.serializedMessageQueue = [];
    // Defence in depth. start() already clears this before calling init(); keeping it here means
    // any other caller of init() also gets a clean slate rather than inheriting a stuck flag.
    this.processingMessageData = false;

    /**
     * This needs to be reset so that the message handler know that
     * it is going to to get a first message as ABI
     */
    this.abi = null;
    this._lastAckBackpressureLog = 0;
    this._statsWindowStartHr = null;
    this._statsBlocksInWindow = 0;
  }

  start() {
    this.logger.info(`==== Starting the receiver.`);

    /**
     * A (re)start SUPERSEDES any in-flight batch rather than waiting for it.
     *
     * This used to `return` here whenever processingMessageData was true, re-arming a 1s timer.
     * That flag is only cleared by processMessageData()'s `finally`, which never runs if one of
     * its awaits never settles (deserializeDeep performs network I/O; deliverDeserializedBlock is
     * consumer code -- neither is bounded by a timeout). A socket death mid-batch therefore
     * latched the receiver into a permanent 1s no-op loop: it logged "Starting the receiver"
     * forever and never dialled again. Observed in production as a 55.9h silent outage.
     *
     * Bumping the epoch invalidates the in-flight batch (see processMessageData), so it is safe to
     * clear the flag and reconnect immediately.
     */
    this._connectionEpoch += 1;
    if (this.processingMessageData) {
      this.logger.warn(
        `Restarting while a message batch was in flight (epoch ${this._connectionEpoch}); ` +
          `abandoning it. Its connection is gone and its results are no longer valid.`
      );
      this.processingMessageData = false;
    }
    this.init();
    this._clearWatchdog();

    if (this.connection) {
      // close previous connection
      this.connection.disconnect();
      this.connection = null;
    }

    const connection = new Connection({
      logger: this.logger,
      socketAddresses: Array.from(new Set(this.config.socketAddresses)),
      onError: (err) => this._onError(err),
      onMessage: this.onMessage.bind(this),
      onClose: () => {
        // A late close from a connection that stop() or a later start() already replaced must not
        // restart the receiver: that is how a stopped receiver came back to life.
        if (this.connection !== connection) return;
        if (this.debuging) {
          this.logger.info(`Connection is closed. Restart!`);
        }
        this.start();
      },
    });
    this.connection = connection;

    this.connection.connect();
    this._armWatchdog(connection);
  }

  /**
   * THE FIX FOR THE 2026-10-29 BRIDGE STALL (48 h).
   *
   * The SHIP stream stopped delivering on an OPEN socket: no close frame, no socket error. The
   * only reconnect triggers were `close` and `error`, so the receiver waited forever while the
   * process looked healthy. Two independent checks make a dead-but-open socket detectable:
   *
   *  - silence: no message for `silenceTimeoutMs` while the receiver is idle (queue empty, no
   *    batch in flight). Time spent processing is NOT silence: with the ACK window exhausted the
   *    server is correctly waiting for us, and tearing that down would churn a slow consumer.
   *  - ping: a websocket ping every `pingIntervalMs`; no pong before the next ping is a dead peer.
   *
   * Either one goes through the SAME path as the 4.0.4 handshake timeout — Connection._failAttempt
   * — which retires the socket's generation, tears it down and calls onClose → start(). start()
   * re-requests blocks from current_block + 1 (see requestBlocks), so nothing is skipped.
   *
   * Timers are bound to ONE Connection. start() and stop() clear them, and every callback returns
   * early if its Connection is no longer current, so a replaced socket can never fire a reconnect.
   */
  _armWatchdog(connection) {
    this._lastActivityAt = Date.now();
    this._awaitingPong = false;

    if (Number.isFinite(this.silenceTimeoutMs) && this.silenceTimeoutMs > 0) {
      const tickMs = Math.max(10, Math.min(1000, Math.floor(this.silenceTimeoutMs / 4)));
      this._silenceTimer = setInterval(() => this._checkSilence(connection), tickMs);
      unrefTimer(this._silenceTimer);
    }

    if (Number.isFinite(this.pingIntervalMs) && this.pingIntervalMs > 0) {
      const ws = connection.ws;
      if (ws && typeof ws.on === 'function') {
        ws.on('pong', () => {
          if (this.connection !== connection || connection.ws !== ws) return;
          this._awaitingPong = false;
        });
      }
      this._pingTimer = setInterval(() => this._checkPing(connection), this.pingIntervalMs);
      unrefTimer(this._pingTimer);
    }
  }

  _clearWatchdog() {
    if (this._silenceTimer) {
      clearInterval(this._silenceTimer);
      this._silenceTimer = null;
    }
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
    this._awaitingPong = false;
  }

  _checkSilence(connection) {
    if (this.connection !== connection) return;
    const now = Date.now();
    if (this.processingMessageData || this.serializedMessageQueue.length > 0) {
      // We are the bottleneck, not the server.
      this._lastActivityAt = now;
      return;
    }
    // Before `open`, the 30 s handshake timeout in Connection owns the socket.
    if (!connection.connected) return;
    const silentMs = now - this._lastActivityAt;
    if (silentMs >= this.silenceTimeoutMs) {
      this._watchdogFire(connection, `no message within ${this.silenceTimeoutMs} ms`);
    }
  }

  _checkPing(connection) {
    if (this.connection !== connection) return;
    const ws = connection.ws;
    if (!connection.connected || !ws || ws.readyState !== 1 /* OPEN */) return;
    if (this._awaitingPong) {
      this._watchdogFire(connection, `no pong within ${this.pingIntervalMs} ms`);
      return;
    }
    try {
      this._awaitingPong = true;
      ws.ping();
    } catch (e) {
      this._onError(e);
    }
  }

  _watchdogFire(connection, reason) {
    if (this.connection !== connection) return;
    this._clearWatchdog();

    const silentMs = Date.now() - this._lastActivityAt;
    const resumeFrom = this.current_block > 0 ? this.current_block + 1 : this.startBlock;
    // logger.error, not warn: consumers pass their own logger and only info/error are guaranteed.
    const message =
      `[statereceiver] watchdog fired: ${reason}; no message for ${silentMs} ms, ` +
      `last block ${this.current_block}. Terminating the socket and reconnecting from block ${resumeFrom}.`;

    // terminate(), not close(): a close handshake with a dead peer would wait for a reply that is
    // never coming. The generation bump inside _failAttempt makes its late `close` event stale.
    const ws = connection.ws;
    try {
      if (ws) ws.terminate();
    } catch (_e) {
      // safe to ignore: the socket is being abandoned either way
    }

    if (typeof connection._failAttempt === 'function') {
      // Logs `message` once, releases the guard and calls onClose → start().
      connection._failAttempt(connection._generation, message);
    } else {
      this.logger.error(message);
      this.start();
    }
  }

  restart(startBlock, endBlock) {
    this.stop();

    this.startBlock = startBlock;
    this.endBlock = endBlock;

    this.start();
  }

  stop() {
    this.logger.info(`==== Receive stop command: stopping receiver.`);
    this._clearWatchdog();
    if (this.connection) {
      this.connection.disconnect();
      this.connection = null;
    }
    this.logger.info(`==== Receiver is stopped.`);
  }

  /**
   * Register Trace Handler
   * @param {TraceHandler} h
   */
  registerTraceHandler(h) {
    if (!h || typeof h.processTrace !== 'function') {
      throw new Error(`Handler is not valid: missing 'processTrace'.`);
    }

    this.traceHandlers.push(h);
    if (h.contractName && h.actionName) {
      this.deserializerActionSet.add(`${h.contractName}::${h.actionName}`);
    }
  }

  onMessage(data) {
    this._lastActivityAt = Date.now();
    try {
      if (!this.abi) {
        this.receivedAbi(data);
      } else {
        this.inflightMessageCount--;
        if (!this._ackEachBlock) {
          this.sendAck();
        }

        if (this.debuging) {
          this.logger.info(`onMessage: add message to queue ${this.serializedMessageQueue.length}`);
        }

        // queuing data
        this.serializedMessageQueue.push(data);

        // Intentionally, not to block the receiving message from state node.
        // no "await" here.
        this.processMessageData(this.serializedMessageQueue).catch((err) => {
          this._onError(err);
        });
      }
    } catch (e) {
      this._onError(e);
    }
  }

  receivedAbi(data) {
    this.logger.info('Receiving abi...');
    this.abi = JSON.parse(data);
    this.types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), this.abi);

    // ready to request blocks
    this.requestBlocks();
  }

  requestBlocks() {
    let startBlock = parseInt(this.startBlock);
    if (this.current_block > 0) {
      startBlock = this.current_block + 1;
    } else {
      this.current_block = 0;
    }

    const args = {
      start_block_num: startBlock,
      end_block_num: parseInt(this.endBlock),
      max_messages_in_flight: this.config.maxQueueSize,
      have_positions: [],
      irreversible_only: true,
      fetch_block: this.fetchBlockTime,
      fetch_traces: this.traceHandlers.length > 0,
      fetch_deltas: false,
    };

    this.logger.info(
      `Requesting blocks, Start : ${args.start_block_num}, End : ${args.end_block_num}, Max Messages In Flight : ${args.max_messages_in_flight}`
    );
    this.send(['get_blocks_request_v0', args]);
    this.inflightMessageCount = args.max_messages_in_flight;
  }

  requestStatus() {
    this.send(['get_status_request_v0', {}]);
  }

  _logBackpressure(level, msg) {
    const throttleMs = this._ackBackpressureLogMs;
    const now = Date.now();
    if (
      throttleMs <= 0 ||
      !this._lastAckBackpressureLog ||
      now - this._lastAckBackpressureLog >= throttleMs
    ) {
      this._lastAckBackpressureLog = now;
      this.logger[level](msg);
    }
  }

  sendAck() {
    const freeQueueSize =
      this.config.maxQueueSize - this.inflightMessageCount - this.serializedMessageQueue.length;
    if (freeQueueSize > 0) {
      this.send(['get_blocks_ack_request_v0', { num_messages: freeQueueSize }]);
      this.inflightMessageCount += freeQueueSize;
    } else {
      this._logBackpressure(
        'debug',
        `[statereceiver] ACK paused (backpressure): queue=${this.serializedMessageQueue.length} inflight=${this.inflightMessageCount} max=${this.config.maxQueueSize}`
      );
    }
  }

  sendAckOne() {
    const freeQueueSize =
      this.config.maxQueueSize - this.inflightMessageCount - this.serializedMessageQueue.length;
    if (freeQueueSize > 0) {
      this.send(['get_blocks_ack_request_v0', { num_messages: 1 }]);
      this.inflightMessageCount += 1;
    } else {
      this._logBackpressure(
        'warn',
        `[statereceiver] sendAckOne skipped (no window): queue=${this.serializedMessageQueue.length} inflight=${this.inflightMessageCount} max=${this.config.maxQueueSize}`
      );
    }
  }

  send(request) {
    if (this.connection && this.connection.ws && this.connection.connected == true) {
      this.connection.ws.send(serialize(this.types, 'request', request));
    } else {
      this.logger.warn('Connection is not ready, cannot send message.');
    }
  }

  /**
   * Rolling blocks/sec and ETA to chain head / LIB (irreversible stream).
   * @param {object} result - get_blocks_result_v0 payload (blockData[1])
   */
  _maybeEmitSyncStats(result) {
    if (!this._statsLog || !result?.this_block || !result.head || !result.last_irreversible) {
      return;
    }
    const blockNum = +result.this_block.block_num;
    const headNum = +result.head.block_num;
    const irrNum = +result.last_irreversible.block_num;

    const now = process.hrtime.bigint();
    if (this._statsWindowStartHr == null) {
      this._statsWindowStartHr = now;
      this._statsBlocksInWindow = 0;
    }
    this._statsBlocksInWindow += 1;

    const intervalNs = BigInt(this._statsIntervalMs) * 1_000_000n;
    if (now - this._statsWindowStartHr < intervalNs) {
      return;
    }

    const elapsedSec = Number(now - this._statsWindowStartHr) / 1e9;
    const bps = elapsedSec > 0 ? this._statsBlocksInWindow / elapsedSec : 0;
    const behindHead = Math.max(0, headNum - blockNum);
    const behindIrr = Math.max(0, irrNum - blockNum);
    const etaHeadSec = bps > 0.0001 && behindHead > 0 ? Math.round(behindHead / bps) : null;
    const etaIrrSec = bps > 0.0001 && behindIrr > 0 ? Math.round(behindIrr / bps) : null;

    this.logger.info(
      `[statereceiver/stats] synced_block=${blockNum} head_block=${headNum} irr_block=${irrNum} blocks_per_sec=${bps.toFixed(
        2
      )} behind_head=${behindHead} behind_irr=${behindIrr} eta_head_sec=${
        etaHeadSec == null ? 'n/a' : etaHeadSec
      } eta_irr_sec=${etaIrrSec == null ? 'n/a' : etaIrrSec} window_blocks=${
        this._statsBlocksInWindow
      }`
    );

    this._statsWindowStartHr = now;
    this._statsBlocksInWindow = 0;
  }

  /**
   *
   * @param {Array<Buffer>} serializedMessageQueue
   * @returns {Promise<void>}
   */
  async processMessageData(serializedMessageQueue) {
    if (this.processingMessageData) {
      return;
    }
    this.processCount += 1;
    if (this.debuging) {
      this.logger.info(
        `Enter processMessageData ${this.processCount}, ${this.processingMessageData}, ${serializedMessageQueue.length}`
      );
    }
    this.processingMessageData = true;
    const myEpoch = this._connectionEpoch;
    /** True once a reconnect has superseded this batch; see start(). */
    const superseded = () => this._connectionEpoch !== myEpoch;
    let drainErrored = false;

    // this.logger.debug(`Processing message data...`);
    try {
      const deserializingOptions = {
        deserializeTraces: true,
        actionSet: this.deserializerActionSet,
      };

      while (serializedMessageQueue.length > 0) {
        if (this.debuging) {
          this.logger.info(
            `Processing message loop ${this.processCount}, ${this.processingMessageData}, ${serializedMessageQueue.length}`
          );
        }
        if (superseded()) {
          this.logger.warn(
            `Abandoning message batch: superseded by a reconnect (epoch ${myEpoch} -> ${this._connectionEpoch}).`
          );
          return;
        }
        const serializedMessage = serializedMessageQueue.shift();

        if (!this._ackEachBlock && serializedMessageQueue.length < 2) {
          this.sendAck();
        }

        if (this.debuging) {
          this.logger.info(`Start deserialize block data`);
        }
        const blockData = await deserializeDeep({
          eosApi: this.eosApi,
          types: this.types,
          type: 'result',
          data: serializedMessage,
          options: deserializingOptions,
        });

        if (superseded()) {
          this.logger.warn(
            `Abandoning message batch: superseded by a reconnect (epoch ${myEpoch} -> ${this._connectionEpoch}).`
          );
          return;
        }

        if (blockData[1] && blockData[1].this_block) {
          await this.deliverDeserializedBlock(blockData[1]);
          this._maybeEmitSyncStats(blockData[1]);
        } else {
          this.logger.info(`Reached the head of the chain: ${JSON.stringify(blockData)}`);
        }

        if (this._ackEachBlock) {
          this.sendAckOne();
        }
      }

      if (!this._ackEachBlock) {
        this.sendAck();
      }
    } catch (err) {
      drainErrored = true;
      this._onError(err);
    } finally {
      // Only release the flag if it is still OURS. A superseded batch must not clear the flag of
      // the live invocation that replaced it.
      if (!superseded()) {
        this.processingMessageData = false;
      }
      if (this.debuging) {
        this.logger.info(`Exit processMessageData ${this.processCount}`);
      }
      const q = superseded() ? [] : serializedMessageQueue;
      if (q.length > 0) {
        const reentry = () => {
          this.processMessageData(q).catch((err) => this._onError(err));
        };
        if (drainErrored) {
          // back off after errors to avoid hot-looping on a malformed/poison block
          setTimeout(reentry, this._drainBackoffMs);
        } else {
          setImmediate(reentry);
        }
      }
    }
    // this.logger.debug(`Processing message data stop.`);
  }

  status() {
    return {
      start: this.startBlock,
      end: this.endBlock,
      current: this.current_block,
      serializedMessageQueueSize: this.serializedMessageQueue.length,
    };
  }

  async deliverDeserializedBlock(blockData) {
    if (!blockData || !blockData.this_block) {
      this.logger.warn(`Block data is not valid.`, blockData);
      return;
    }

    const block_num = +blockData.this_block.block_num;
    const block_time = blockData.block ? blockData.block.timestamp : null;
    const head_block_num = blockData.head.block_num;
    const last_irreversible_block_num = blockData.last_irreversible.block_num;

    if (this.debuging) {
      this.logger.info(`deliverDeserializedBlock ${block_num}`);
    }

    for (const handler of this.traceHandlers) {
      await handler.processTrace(
        block_num,
        blockData.traces,
        block_time,
        head_block_num,
        last_irreversible_block_num
      );
    }

    this.current_block = block_num;
  }

  _onError(e) {
    if (typeof this.config.onError === 'function') {
      this.config.onError(e);
    } else {
      this.logger.error(e);
    }
  }
}

function unrefTimer(t) {
  // Never hold the event loop open for a watchdog. Guarded: a fake-timer setInterval may not
  // return a Node Timeout.
  if (t && typeof t.unref === 'function') t.unref();
}

module.exports = StateReceiver;
