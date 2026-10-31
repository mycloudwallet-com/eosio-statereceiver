export = StateReceiver;
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
declare class StateReceiver {
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
    constructor(config: {
        startBlock: number;
        endBlock: number;
        socketAddresses: string[];
        eosApi: EosApi;
        eosEndpoint: string;
        logger: object;
        onError: Function;
        maxQueueSize: number;
        fetchBlockTime: boolean;
        deserializerActions: string[];
        silenceTimeoutMs?: number;
        pingIntervalMs?: number;
    });
    /**
     * @type {Connection}
     */
    connection: Connection;
    /**
     * @type {EosApi}
     */
    eosApi: EosApi;
    /**
     * @type {Array<Buffer>}
     */
    serializedMessageQueue: Array<Buffer>;
    /**
     * @type {Set<string>}
     */
    deserializerActionSet: Set<string>;
    /**
     * @type {number}
     */
    inflightMessageCount: number;
    /**
     * @type {boolean}
     */
    fetchBlockTime: boolean;
    logger: any;
    traceHandlers: any[];
    config: Readonly<{
        startBlock: number;
        endBlock: number;
        socketAddresses: string[];
        eosApi: EosApi;
        eosEndpoint: string;
        logger: object;
        onError: Function;
        maxQueueSize: number;
        fetchBlockTime: boolean;
        deserializerActions: string[];
        silenceTimeoutMs?: number;
        pingIntervalMs?: number;
    }>;
    startBlock: number;
    endBlock: string | number;
    current_block: number;
    types: Map<string, Serialize.Type>;
    processingMessageData: boolean;
    /**
     * Incremented on every start()/reconnect. A processMessageData() invocation captures this
     * value and abandons itself as soon as it no longer matches, so a batch belonging to a dead
     * connection can never deliver blocks, ACK, or clear the in-progress flag of a live one.
     */
    _connectionEpoch: number;
    silenceTimeoutMs: number;
    pingIntervalMs: number;
    drainTimeoutMs: number;
    _inflightDelivery: Promise<void>;
    _draining: boolean;
    _requestedEpoch: number;
    _silenceTimer: NodeJS.Timer;
    _pingTimer: NodeJS.Timer;
    _lastActivityAt: number;
    _awaitingPong: boolean;
    processCount: number;
    debuging: boolean;
    /** One get_blocks_ack per processed message (num_messages:1). Fixes throughput with some state-history nodes. */
    _ackEachBlock: boolean;
    _lastAckBackpressureLog: number;
    _ackBackpressureLogMs: number;
    _drainBackoffMs: number;
    _statsLog: boolean;
    _statsIntervalMs: number;
    _statsWindowStartHr: bigint;
    _statsBlocksInWindow: number;
    init(): void;
    /**
     * This needs to be reset so that the message handler know that
     * it is going to to get a first message as ABI
     */
    abi: any;
    start(): void;
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
    _armWatchdog(connection: any): void;
    _clearWatchdog(): void;
    _checkSilence(connection: any): void;
    _checkPing(connection: any): void;
    _watchdogFire(connection: any, reason: any): void;
    restart(startBlock: any, endBlock: any): void;
    stop(): void;
    /**
     * Register Trace Handler
     * @param {TraceHandler} h
     */
    registerTraceHandler(h: TraceHandler): void;
    onMessage(data: any): void;
    receivedAbi(data: any): void;
    requestBlocks(): Promise<void>;
    requestStatus(): void;
    _logBackpressure(level: any, msg: any): void;
    sendAck(): void;
    sendAckOne(): void;
    send(request: any): void;
    /**
     * Rolling blocks/sec and ETA to chain head / LIB (irreversible stream).
     * @param {object} result - get_blocks_result_v0 payload (blockData[1])
     */
    _maybeEmitSyncStats(result: object): void;
    /**
     *
     * @param {Array<Buffer>} serializedMessageQueue
     * @returns {Promise<void>}
     */
    processMessageData(serializedMessageQueue: Array<Buffer>): Promise<void>;
    status(): {
        start: number;
        end: string | number;
        current: number;
        serializedMessageQueueSize: number;
    };
    deliverDeserializedBlock(blockData: any): Promise<void>;
    _warn(...args: any[]): void;
    _onError(e: any): void;
}
declare namespace StateReceiver {
    export { EosApi, ProcessTrace, TraceHandler };
}
import Connection = require("./connection");
type EosApi = import('eosjs').Api;
import { Serialize } from "eosjs";
type TraceHandler = {
    /**
     * interest in contract name
     */
    contractName: string;
    /**
     * interest in action name
     */
    actionName: string;
    /**
     * handler function
     */
    processTrace: ProcessTrace;
};
type ProcessTrace = (block_num: number, traces: Array<any>, block_time: string) => Promise<void>;
//# sourceMappingURL=state-receiver.d.ts.map