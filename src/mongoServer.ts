import { DenoConnectOptions } from "./cluster.ts";
import { MongoError } from "./error.ts";
import { WireProtocol } from "./protocol/mod.ts";
import { assert } from "../deps.ts";
import { AuthContext, ScramAuthPlugin, X509AuthPlugin } from "./auth/mod.ts";
import { ConnectOptions } from "./types.ts";

export type ServerState =
  | "Unknown"
  | "Standalone"
  | "Mongos"
  | "PossiblePrimary"
  | "RSPrimary"
  | "RSSecondary"
  | "RSArbiter"
  | "RSOther"
  | "RSGhost"
  | "LoadBalanced";

type OnDiscovery = (hostString: string) => Promise<void>;

export class MongoServer {
  state: ServerState = "Unknown";
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

  async init(onDiscovery?: OnDiscovery) {
    await this.connect();
    await this.authenticate();
    await this.updateState(onDiscovery);
  }

  async connect() {
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
  }

  private async authenticate() {
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
  }

  private async updateState(onDiscovery?: OnDiscovery) {
    assert(this.protocol, "Tried to poll for ismaster while not authenticated");
    const response = await this.protocol.commandSingle(
      "admin",
      //   { ismaster: 1 }
      { hello: 1 },
    );
    console.log(response);
    if (response.secondary === true && !!response.setName) {
      this.state = "RSSecondary";
    } else if (response.isWritablePrimary === true && !!response.setName) {
      this.state = "RSPrimary";
    } else if (response.arbiterOnly === true && !!response.setName) {
      this.state = "RSArbiter";
    } else {
      this.state = "Unknown";
    }

    if (response.hosts?.length > 0 && typeof onDiscovery === "function") {
      await Promise.all(
        response.hosts.map((hostString: string) => onDiscovery(hostString)),
      );
    }
  }

  close() {
    this.#connection?.close();
    this.#connection = undefined;
  }

  toString() {
    return `${this.#host}:${this.#port}\t${this.state} connected: ${!!this
      .#connection}`;
  }
}
