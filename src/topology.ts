import { ObjectIDType } from "./utils/bson.ts";

function unknownDefault(
  props: Partial<ServerDescription> = {},
): ServerDescription {
  return {
    type: "Unknown",
    electionId: null,
    setVersion: null,
    setName: null,
    minWireVersion: 0,
    maxWireVersion: 0,
    hosts: [],
    arbiters: [],
    passives: [],
    topologyVersion: null,
    logicalSessionTimeoutMinutes: null,
    me: null,
    primary: null,
    ...props,
  };
}

function normaliseResponse(response: IsMasterResponse) {
  return {
    ...response,
    hosts: response.hosts?.map((host) => host.toLowerCase()) || [],
    arbiters: response.arbiters?.map((host) => host.toLowerCase()) || [],
    passives: response.passives?.map((host) => host.toLowerCase()) || [],
    primary: response.primary?.toLowerCase(),
    setName: response.setName?.toLowerCase(),
    me: response.me?.toLowerCase(),
  };
}

function isStateChangedError(error: ApplicationError) {
  return isRecovering(
    error?.response?.errmsg as string,
    error?.response?.code as number,
  ) ||
    isNotMaster(
      error?.response?.errmsg as string,
      error?.response?.code as number,
    );
}

function isNotMaster(message?: string, code?: number) {
  const notMasterCodes = [10107, 13435, 10058];
  if (code) return notMasterCodes.includes(code);
  if (isRecovering(message)) return false;

  return (message || "").match(
    /not master/,
  );
}
function isRecovering(message?: string, code?: number) {
  const recoveringCodes = [11600, 11602, 13436, 189, 91];
  if (code) return recoveringCodes.includes(code);

  return (message || "").match(
    /not master or secondary|node is recovering/,
  );
}

function compareTopologyVersion(
  oldTopologyVersion?: TopologyVersion | null,
  newTopologyVersion?: TopologyVersion | null,
) {
  if (!oldTopologyVersion || !newTopologyVersion) return -1;
  if (
    !oldTopologyVersion.processId.equals(newTopologyVersion.processId)
  ) {
    return -1;
  }

  const oldCounter = oldTopologyVersion.counter;
  const newCounter = newTopologyVersion.counter;

  if (oldCounter === newCounter) return 0;
  if (oldCounter < newCounter) return -1;
  return 1;
}

function typeFromResponse(response: IsMasterResponse): ServerType {
  if (response.arbiterOnly === true && !!response.setName) {
    return "RSArbiter";
  }
  if (
    response.msg !== "isdbgrid" &&
    !response.setName &&
    response.isreplicaset !== true
  ) {
    return "Standalone";
  }
  if (response.isreplicaset === true) return "RSGhost";
  if (
    (response.ismaster === true || response.isWritablePrimary === true) &&
    !!response.setName
  ) {
    return "RSPrimary";
  }
  if (
    response.setName &&
    (response.hidden ||
      (!response.ismaster && !response.secondary && !response.arbiterOnly))
  ) {
    return "RSOther";
  }
  if (response.secondary && !!response.setName) return "RSSecondary";
  if (response.msg === "isdbgrid") return "Mongos";

  return "Unknown";
}

export type ServerType =
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

export type TopologyType =
  | "Single"
  | "ReplicaSetNoPrimary"
  | "ReplicaSetWithPrimary"
  | "Sharded"
  | "Unknown"
  | "LoadBalanced"; // TODO

export interface IsMasterResponse {
  ok: number;
  ismaster: boolean;
  minWireVersion: number;
  maxWireVersion: number;

  isWritablePrimary?: boolean;
  isreplicaset?: boolean;
  secondary?: boolean;
  setName?: string;
  hosts?: string[];
  arbiters?: string[];
  passives?: string[];
  hidden?: boolean;
  arbiterOnly?: boolean;
  topologyVersion?: TopologyVersion;
  me?: string;
  primary?: string;
  setVersion?: number;
  electionId?: ObjectIDType;
  msg?: string;
  logicalSessionTimeoutMinutes?: number | null;
}

export interface ApplicationError {
  address: string;
  maxWireVersion: number;
  type: "command" | "network" | "timeout";

  when?: "afterHandshakeCompletes";
  generation?: number;
  response?: Record<string, unknown>;
}

export interface ServerDescription {
  type: ServerType;
  minWireVersion: number;
  maxWireVersion: number;
  hosts: string[];
  arbiters: string[];
  passives: string[];

  logicalSessionTimeoutMinutes: number | null;
  topologyVersion: TopologyVersion | null;
  setName: string | null;
  setVersion: number | null;
  electionId: ObjectIDType | null;

  me: string | null;
  primary: string | null;
}

interface TopologyVersion {
  processId: ObjectIDType;
  counter: number;
}

type TopologyOptions = {
  replicaSet?: string;
  directConnection?: boolean;
};

export class Topology {
  #minWireVersion = 2; // TODO: What should be the supported range?
  #maxWireVersion = 9; // TODO: What should be the supported range?
  #seeds: string[];
  #serverDescriptions: Map<string, ServerDescription> = new Map();
  #type: TopologyType = "Unknown";

  #setName: string | null = null;
  #maxSetVersion: number | null = null;
  #maxElectionId: ObjectIDType | null = null;
  #logicalSessionTimeoutMinutes: number | null = null;

  constructor(
    initialServerDescriptions: { host: string; port: number }[],
    options: TopologyOptions,
  ) {
    this.#seeds = initialServerDescriptions.map(({ host, port }) =>
      `${host}:${port}`
    );
    initialServerDescriptions.forEach(({ host, port }) => {
      const key = `${host}:${port}`;
      this.setDefaultServerDescription(key);
    });
    if (options.directConnection === true) {
      this.#setName = options.replicaSet || null;
      this.#type = "Single";
    } else if (options.replicaSet) {
      this.#setName = options.replicaSet;
      this.#type = "ReplicaSetNoPrimary";
    }
  }

  private setDefaultServerDescription(hostAndPort: string) {
    this.#serverDescriptions.set(
      hostAndPort.toLowerCase(),
      unknownDefault(),
    );
  }

  private deleteServerDescription(hostAndPort: string) {
    this.#serverDescriptions.delete(hostAndPort);
    if (this.#type === "ReplicaSetWithPrimary") this.checkIfHasPrimary();
  }

  updateServerDescription(
    hostAndPort: string,
    response: IsMasterResponse,
  ) {
    hostAndPort = hostAndPort.toLowerCase();
    response = normaliseResponse(response);

    if (response.ok !== 1) {
      this.setDefaultServerDescription(hostAndPort);
      if (this.#type === "ReplicaSetWithPrimary") {
        this.checkIfHasPrimary();
      }
      return;
    }

    const serverDescription: ServerDescription = {
      type: typeFromResponse(response),
      logicalSessionTimeoutMinutes: response.logicalSessionTimeoutMinutes ??
        null,
      topologyVersion: response.topologyVersion ?? null,
      setName: response.setName ?? null,
      electionId: response.electionId ?? null,
      setVersion: response.setVersion ?? null,
      minWireVersion: response.minWireVersion ?? 0,
      maxWireVersion: response.maxWireVersion ?? 0,
      hosts: response.hosts ?? [],
      arbiters: response.arbiters ?? [],
      passives: response.passives ?? [],
      me: response.me ?? null,
      primary: response.primary ?? null,
    };

    if (!this.#serverDescriptions.has(hostAndPort)) return;
    if (
      compareTopologyVersion(
        this.#serverDescriptions.get(hostAndPort)?.topologyVersion,
        serverDescription.topologyVersion,
      ) > 0
    ) {
      return;
    }

    this.#serverDescriptions.set(hostAndPort, serverDescription);

    switch (serverDescription.type) {
      case "Unknown":
        if (this.#type === "ReplicaSetWithPrimary") this.checkIfHasPrimary();
        break;
      case "Standalone":
        if (this.#type === "Unknown") {
          if (this.#seeds.length === 1) {
            this.#type = "Single";
          } else {
            this.#serverDescriptions.delete(hostAndPort);
          }
        } else if (this.#type === "Single") {
          for (const key of this.#serverDescriptions.keys()) {
            if (key !== hostAndPort) this.#serverDescriptions.delete(key);
          }
        } else {
          this.deleteServerDescription(hostAndPort);
        }
        break;
      case "Mongos":
        if (this.#type === "Unknown") {
          this.#type = "Sharded";
        } else if (this.#type === "Sharded") {
          // no-op
        } else if (
          this.#type === "ReplicaSetWithPrimary" ||
          this.#type === "ReplicaSetNoPrimary"
        ) {
          this.deleteServerDescription(hostAndPort);
        }
        break;
      case "RSPrimary":
        if (
          this.#type === "Unknown" || this.#type === "ReplicaSetNoPrimary" ||
          this.#type === "ReplicaSetWithPrimary"
        ) {
          this.#type = "ReplicaSetWithPrimary";
          this.updateRSFromPrimary(hostAndPort, serverDescription);
        } else if (this.#type === "Sharded") {
          this.deleteServerDescription(hostAndPort);
        } else if (this.#type === "Single") {
          if (this.#setName && this.#setName !== serverDescription.setName) {
            serverDescription.type = "Unknown";
          }
          for (const key of this.#serverDescriptions.keys()) {
            if (key !== hostAndPort) this.#serverDescriptions.delete(key);
          }
        }
        break;
      case "RSSecondary":
      case "RSArbiter":
      case "RSOther":
        if (
          this.#type === "ReplicaSetNoPrimary" || this.#type === "Unknown"
        ) {
          this.#type = "ReplicaSetNoPrimary";
          this.updateRSWithoutPrimary(hostAndPort, serverDescription);
        } else if (this.#type === "ReplicaSetWithPrimary") {
          this.updateRSWithPrimaryFromMember(hostAndPort, serverDescription);
        } else if (this.#type === "Sharded") {
          this.deleteServerDescription(hostAndPort);
        } else if (this.#type === "Single") {
          for (const key of this.#serverDescriptions.keys()) {
            if (key !== hostAndPort) this.#serverDescriptions.delete(key);
          }
        }
        break;
      case "RSGhost":
        if (this.#type === "ReplicaSetWithPrimary") {
          this.checkIfHasPrimary();
        } else if (this.#type === "Sharded") {
          this.deleteServerDescription(hostAndPort);
        }
        break;
      default:
        throw new Error(
          `Not handling implemented for response ${typeFromResponse(response)}`,
        );
    }
    this.updateLogicalSessionTimeoutMinutes();
  }

  handleError(
    error: ApplicationError,
  ) {
    if (this.isStaleError(error)) return;

    if (isStateChangedError(error)) {
      if (this.#type !== "LoadBalanced") {
        this.#serverDescriptions.set(
          error.address,
          unknownDefault({
            topologyVersion:
              error.response?.topologyVersion as TopologyVersion ?? null,
          }),
        );
        if (this.#type === "ReplicaSetWithPrimary") this.checkIfHasPrimary();
      }
    } else if (
      error.type === "network" ||
      (error.when !== "afterHandshakeCompletes" &&
        (error.type === "timeout" /* TODO: || isAuthError */))
    ) {
      if (this.#type !== "LoadBalanced") {
        this.#serverDescriptions.set(
          error.address,
          unknownDefault({
            topologyVersion:
              error.response?.topologyVersion as TopologyVersion ?? null,
          }),
        );
        if (this.#type === "ReplicaSetWithPrimary") this.checkIfHasPrimary();
      } else {
        // TODO
      }
    }
  }

  isStaleError(error: ApplicationError) {
    const currentServer = this.#serverDescriptions.get(error.address);
    return compareTopologyVersion(
      currentServer?.topologyVersion,
      error.response?.topologyVersion as TopologyVersion,
    ) >= 0;
  }

  private updateRSFromPrimary(
    hostAndPort: string,
    serverDescription: ServerDescription,
  ) {
    if (this.#setName === null) {
      this.#setName = serverDescription.setName!;
    } else if (this.#setName !== serverDescription.setName) {
      this.deleteServerDescription(hostAndPort);
      return;
    }

    if (serverDescription.setVersion && serverDescription.electionId) {
      if (
        this.#maxElectionId &&
        this.#maxSetVersion &&
        (this.#maxSetVersion > serverDescription.setVersion ||
          (this.#maxSetVersion === serverDescription.setVersion &&
            this.#maxElectionId >
              serverDescription.electionId))
      ) {
        this.#serverDescriptions.set(hostAndPort, unknownDefault());
        this.checkIfHasPrimary();
        return;
      }

      this.#maxElectionId = serverDescription.electionId;
    }
    if (
      serverDescription.setVersion &&
      (
        this.#maxSetVersion === null ||
        serverDescription.setVersion > this.#maxSetVersion
      )
    ) {
      this.#maxSetVersion = serverDescription.setVersion;
    }

    for (const peerHostAndPort of this.#serverDescriptions.keys()) {
      const peer = this.#serverDescriptions.get(peerHostAndPort);
      if (hostAndPort !== peerHostAndPort && peer?.type === "RSPrimary") {
        this.#serverDescriptions.set(peerHostAndPort, unknownDefault());
        // TODO:
        // # See note below about invalidating an old primary.
        // replace the server with a default ServerDescription of type "Unknown"
      }
    }

    const peers = [
      ...serverDescription.hosts,
      ...serverDescription.arbiters,
      ...serverDescription.passives,
    ];

    peers
      .filter((peerHostAndPort) =>
        !this.#serverDescriptions.has(peerHostAndPort)
      )
      .forEach((peerHostAndPort) =>
        this.setDefaultServerDescription(peerHostAndPort)
      );

    // Delete anything that is not in peers. If the primary itself is not on it's
    // own peers list we need to remove it too
    for (const descHostAndPort of this.#serverDescriptions.keys()) {
      if (!peers.includes(descHostAndPort)) {
        this.#serverDescriptions.delete(descHostAndPort);
      }
    }

    this.checkIfHasPrimary();
  }

  private updateRSWithPrimaryFromMember(
    hostAndPort: string,
    serverDescription: ServerDescription,
  ) {
    if (this.#setName !== serverDescription.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    // TODO: Is this correct? Specs often omit "me", is it guaranteed
    // to be there?
    if (serverDescription.me && hostAndPort !== serverDescription.me) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    if (!this.getPrimary()) {
      this.#type = "ReplicaSetNoPrimary";
      if (serverDescription.primary) {
        const possiblePrimary = this.#serverDescriptions.get(
          serverDescription.primary,
        );
        if (possiblePrimary?.type === "Unknown") {
          possiblePrimary.type = "PossiblePrimary";
        }
      }
    }
  }

  private updateRSWithoutPrimary(
    hostAndPort: string,
    serverDescription: ServerDescription,
  ) {
    if (!this.#setName) {
      this.#setName = serverDescription.setName;
    } else if (this.#setName !== serverDescription.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }

    const peers = [
      ...serverDescription.hosts,
      ...serverDescription.arbiters,
      ...serverDescription.passives,
    ];

    peers
      .filter((peerHostAndPort) =>
        !this.#serverDescriptions.has(peerHostAndPort)
      )
      .forEach((peerHostAndPort) =>
        this.setDefaultServerDescription(peerHostAndPort)
      );

    if (!this.getPrimary()) {
      this.#type = "ReplicaSetNoPrimary";
      if (serverDescription.primary) {
        const possiblePrimary = this.#serverDescriptions.get(
          serverDescription.primary,
        );
        if (possiblePrimary?.type === "Unknown") {
          possiblePrimary.type = "PossiblePrimary";
        }
      }
    }

    // TODO: Is this right? Atleast RSOther doesn't have "me" prop
    // but it needs to be kept in set
    if (serverDescription.me && hostAndPort !== serverDescription.me) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }
  }

  private checkIfHasPrimary() {
    for (const description of this.#serverDescriptions.values()) {
      if (description.type === "RSPrimary") {
        this.#type = "ReplicaSetWithPrimary";
        return;
      }
    }
    this.#type = "ReplicaSetNoPrimary";
  }

  private updateLogicalSessionTimeoutMinutes() {
    const descs = Array.from(this.#serverDescriptions.values()).filter(
      (desc) =>
        desc.type === "RSPrimary" ||
        desc.type === "RSSecondary" ||
        desc.type === "Mongos" ||
        desc.type === "Standalone",
    );
    const timeouts = descs.reduce<number[]>(
      (acc, { logicalSessionTimeoutMinutes }) =>
        logicalSessionTimeoutMinutes !== null
          ? [...acc, logicalSessionTimeoutMinutes]
          : acc,
      [],
    );
    if (timeouts.length === 0 || timeouts.length < descs.length) {
      this.#logicalSessionTimeoutMinutes = null;
      return;
    } else {
      this.#logicalSessionTimeoutMinutes = Math.min(...timeouts);
    }
  }

  private getPrimary() {
    for (const desc of this.#serverDescriptions.values()) {
      if (desc.type === "RSPrimary") return desc;
    }
    return undefined;
  }

  isCompatible() {
    return Array
      .from(this.#serverDescriptions.values())
      .filter((desc) =>
        desc.type !== "Unknown" && desc.type !== "PossiblePrimary"
      )
      .every((desc) => {
        return desc.maxWireVersion >= this.#minWireVersion &&
          desc.minWireVersion <= this.#maxWireVersion;
      });
  }

  describe() {
    type TopologyDescription = {
      servers: Record<string, ServerDescription>;

      setName: string | null;
      topologyType: TopologyType;
      logicalSessionTimeoutMinutes: number | null;
      compatible: boolean;
      maxSetVersion: number | null;
      maxElectionId: ObjectIDType | null;
    };

    const description: TopologyDescription = {
      servers: Array.from(this.#serverDescriptions).reduce(
        (acc, [hostAndPort, desc]) => {
          return {
            ...acc,
            [hostAndPort]: desc,
          };
        },
        {},
      ),
      setName: this.#setName,
      topologyType: this.#type,
      logicalSessionTimeoutMinutes: this.#logicalSessionTimeoutMinutes,
      maxSetVersion: this.#maxSetVersion,
      compatible: this.isCompatible(),
      maxElectionId: this.#maxElectionId,
    };
    return description;
  }

  servers() {
    return Array.from(this.#serverDescriptions.entries());
  }

  getMaster(): [string, ServerDescription] | undefined {
    for (const [hostAndPort, serverDescription] of this.#serverDescriptions) {
      if (serverDescription.type === "RSPrimary") {
        return [hostAndPort, serverDescription];
      }
    }
  }
}
