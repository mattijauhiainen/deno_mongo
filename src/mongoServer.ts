import { DenoConnectOptions } from "./cluster.ts";
import { MongoError } from "./error.ts";
import { WireProtocol } from "./protocol/mod.ts";
import { assert } from "../deps.ts";
import { AuthContext, ScramAuthPlugin, X509AuthPlugin } from "./auth/mod.ts";
import { ConnectOptions } from "./types.ts";
import { IsMasterResponse } from "./topology.ts";
import * as logger from "./logger.ts";

export class MongoServer {
  protocol?: WireProtocol;

  #host: string;
  #port: number;
  #connection?: Deno.Conn;
  #options: ConnectOptions;

  constructor(host: string, port: number, options: ConnectOptions) {
    this.#host = host;
    this.#port = port;
    this.#options = options;
  }

  get hostAndPort(): string {
    return `${this.#host}:${this.#port}`;
  }

  async init() {
    await this.connect();
    await this.authenticate();
  }

  async connect() {
    logger.info(`Connecting to ${this.hostAndPort}`);
    const denoConnectOps: DenoConnectOptions = {
      hostname: this.#host,
      port: this.#port,
    };
    if (this.#options.tls) {
      if (this.#options.certFile) {
        denoConnectOps.certFile = this.#options.certFile;
      }
      if (this.#options.keyFile) {
        if (this.#options.keyFilePassword) {
          throw new MongoError(
            `Tls keyFilePassword not implemented in Deno driver`,
          );
          //TODO, need something like const key = decrypt(options.keyFile) ...
        }
        throw new MongoError(`Tls keyFile not implemented in Deno driver`);
        //TODO, need Deno.connectTls with something like key or keyFile option.
      }
      this.#connection = await Deno.connectTls(denoConnectOps);
    } else {
      this.#connection = await Deno.connect(denoConnectOps);
    }
    logger.info(`Connected to ${this.hostAndPort}`);
  }

  private async authenticate() {
    logger.info(`Authenticating to ${this.hostAndPort}`);
    assert(this.#connection, "Tried to authenticate when not connected");
    const protocol = new WireProtocol(this.#connection);
    if (this.#options.credential) {
      const authContext = new AuthContext(
        protocol,
        this.#options.credential,
        this.#options,
      );
      const mechanism = this.#options.credential!.mechanism;
      let authPlugin;
      if (mechanism === "SCRAM-SHA-256") {
        authPlugin = new ScramAuthPlugin("sha256"); //TODO AJUST sha256
      } else if (mechanism === "SCRAM-SHA-1") {
        authPlugin = new ScramAuthPlugin("sha1");
      } else if (mechanism === "MONGODB-X509") {
        authPlugin = new X509AuthPlugin();
      } else {
        throw new MongoError(
          `Auth mechanism not implemented in Deno driver: ${mechanism}`,
        );
      }
      const request = authPlugin.prepare(authContext);
      authContext.response = await protocol.commandSingle(
        "admin", // TODO: Should get the auth db from connectionOptions?
        request,
      );
      await authPlugin.auth(authContext);
    } else {
      await protocol.connect();
    }
    this.protocol = protocol;
    logger.info(`Authenticated to ${this.hostAndPort}`);
  }

  async updateState(): Promise<IsMasterResponse> {
    assert(this.protocol, "Tried to poll for ismaster while not authenticated");
    const response = await this.protocol.commandSingle(
      "admin", // TODO: Which db this should run on?
      { ismaster: 1 },
    ) as IsMasterResponse; // TODO: How's the error response returned?
    return response;
  }

  close() {
    this.#connection?.close();
    this.#connection = undefined;
  }

  toString() {
    return `${this.#host}:${this.#port}\tconnected: ${!!this
      .#connection}`;
  }
}
