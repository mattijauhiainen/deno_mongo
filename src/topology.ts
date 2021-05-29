function unknownDefault(): ServerDescription {
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

function topologyVersionIsStale(
  newVersion?: TopologyVersion | null,
  oldVersion?: TopologyVersion | null,
) {
  if (!newVersion) return false;
  if (!oldVersion) return false;
  if (newVersion.processId.$oid !== oldVersion.processId.$oid) {
    return false;
  }
  if (
    BigInt(newVersion.counter.$numberLong) >=
      BigInt(oldVersion.counter.$numberLong)
  ) {
    return false;
  }
  return true;
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
  | "Unknown";

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
  electionId?: ElectionId;
  msg?: string;
  logicalSessionTimeoutMinutes?: number | null;
}

interface ServerDescription {
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
  electionId: ElectionId | null;

  me: string | null;
  primary: string | null;
}

interface TopologyVersion {
  processId: { $oid: string };
  counter: { $numberLong: string };
}

interface ElectionId {
  $oid: string;
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
  #maxElectionId: ElectionId | null = null;
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
      if (
        this.#type === "ReplicaSetNoPrimary" ||
        this.#type === "ReplicaSetWithPrimary"
      ) {
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
      topologyVersionIsStale(
        serverDescription.topologyVersion,
        this.#serverDescriptions.get(hostAndPort)?.topologyVersion,
      )
    ) {
      return;
    }

    this.#serverDescriptions.set(hostAndPort, serverDescription);

    switch (serverDescription.type) {
      case "Unknown":
        this.checkIfHasPrimary();
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
            BigInt(this.#maxElectionId.$oid) >
              BigInt(serverDescription.electionId.$oid)))
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

    // If the primary is not on it's own hosts list we need to remove it
    Array.from(this.#serverDescriptions.keys()).forEach((hostAndPort) => {
      if (!peers.includes(hostAndPort)) {
        this.#serverDescriptions.delete(hostAndPort);
      }
    });

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
    return Array.from(this.#serverDescriptions.values()).find((desc) =>
      desc.type === "RSPrimary"
    );
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
      maxElectionId: ElectionId | null;
    };

    const description: TopologyDescription = {
      servers: Array.from(this.#serverDescriptions).reduce(
        (acc, [hostAndPort, desc]) => {
          return {
            ...acc,
            [hostAndPort]: {
              setName: desc.setName,
              type: desc.type,
              topologyVersion: desc.topologyVersion,
              setVersion: desc.setVersion,
              electionId: desc.electionId,
            },
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
}
