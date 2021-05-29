const unknownDefault = (): ServerDescription => ({
  type: "Unknown",
  electionId: null,
  setVersion: null,
  setName: null,
  minWireVersion: 0,
  maxWireVersion: 0,
  hosts: [],
  topologyVersion: null,
});

function normaliseResponse(response: IsMasterResponse) {
  // TODO: Dupe it
  response.hosts = response.hosts?.map((host) => host.toLowerCase()) ||
    [];
  response.arbiters = response.arbiters?.map((host) => host.toLowerCase()) ||
    [];
  response.passives = response.passives?.map((host) => host.toLowerCase()) ||
    [];
  response.primary = response.primary?.toLowerCase();
  response.setName = response.setName?.toLowerCase();
  response.me = response.me?.toLowerCase();
  response.maxWireVersion = response.maxWireVersion ?? 0;
  response.minWireVersion = response.minWireVersion ?? 0;
  return response;
}

function topologyVersionIsStale(
  newVersion: TopologyVersion,
  oldVersion?: TopologyVersion | null,
) {
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

export interface IsMasterResponse {
  ok: number;
  ismaster: boolean;
  isWritablePrimary?: boolean;
  isreplicaset?: boolean;
  secondary?: boolean;
  minWireVersion: number;
  maxWireVersion: number;
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

export type TopologyType =
  | "Single"
  | "ReplicaSetNoPrimary"
  | "ReplicaSetWithPrimary"
  | "Sharded"
  | "Unknown";

interface ServerDescription {
  type: ServerType;
  minWireVersion: number;
  maxWireVersion: number;
  me?: string;
  hosts: string[];
  setName?: string | null;
  primary?: string;
  topologyVersion?: TopologyVersion | null;
  setVersion?: number | null;
  electionId?: ElectionId | null;
  logicalSessionTimeoutMinutes?: number | null;
}

interface TopologyVersion {
  processId: { $oid: string };
  counter: { $numberLong: string };
}

interface ElectionId {
  $oid: string;
}

export class Topology {
  #minWireVersion = 2; // TODO: What should be the supported range?
  #maxWireVersion = 9; // TODO: What should be the supported range?

  #seeds: string[];
  #serverDescriptions: Map<string, ServerDescription> = new Map();
  #type: TopologyType = "Unknown";
  #setName: string | null = null;
  #maxSetVersion?: number;
  #maxElectionId?: ElectionId;
  #logicalSessionTimeoutMinutes: number | null = null;

  constructor(
    initialServerDescriptions: { host: string; port: number }[],
    options: Partial<{ replicaSet?: string }>,
  ) {
    this.#seeds = initialServerDescriptions.map(({ host, port }) =>
      `${host}:${port}`
    );
    initialServerDescriptions.forEach(({ host, port }) => {
      const key = `${host}:${port}`;
      this.addInitialServerDescription(key);
    });
    if (options.replicaSet) {
      this.#setName = options.replicaSet;
      this.#type = "ReplicaSetNoPrimary";
    }
  }

  private addInitialServerDescription(hostAndPort: string) {
    hostAndPort = hostAndPort.toLowerCase();
    this.#serverDescriptions.set(hostAndPort, unknownDefault());
  }

  updateRSFromPrimary(
    hostAndPort: string,
    response: IsMasterResponse,
    props: Partial<ServerDescription> & { type: ServerType },
  ) {
    if (!this.#serverDescriptions.has(hostAndPort)) {
      console.warn(
        `Ignoring response for server which is no longer monitored (${hostAndPort})`,
      );
      return;
    }

    if (this.#setName === null) {
      this.#setName = response.setName!;
    } else if (this.#setName !== response.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    if (response.setVersion && response.electionId) {
      if (
        this.#maxElectionId &&
        this.#maxSetVersion &&
        (this.#maxSetVersion > response.setVersion ||
          (this.#maxSetVersion === response.setVersion &&
            BigInt(this.#maxElectionId.$oid) >
              BigInt(response.electionId.$oid)))
      ) {
        this.#serverDescriptions.set(hostAndPort, unknownDefault());
        this.checkIfHasPrimary();
        return;
      }

      this.#maxElectionId = response.electionId;
    }
    if (
      response.setVersion &&
      (
        this.#maxSetVersion === null ||
        // TODO: Initialize as nulls?
        this.#maxSetVersion === undefined ||
        response.setVersion > this.#maxSetVersion
      )
    ) {
      this.#maxSetVersion = response.setVersion;
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

    // Only add if the 'me' matches
    if (
      !response.me ||
      hostAndPort === response.me ||
      // TODO: This propbably isn't right?
      !response.hosts?.includes(response.me)
    ) {
      const serverDescription = {
        ...props,
        setName: response.setName || null,
        minWireVersion: response.minWireVersion,
        maxWireVersion: response.maxWireVersion,
        hosts: response.hosts || [],
        electionId: response.electionId || null,
        setVersion: response.setVersion || null,
      };
      this.#serverDescriptions.set(hostAndPort, serverDescription);
    }

    const peers = [
      ...(response.hosts || []),
      ...(response.arbiters || []),
      ...(response.passives || []),
    ];

    peers
      .filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));

    // Do this after we've added the primary. If the primary is not on it's
    // own hosts list we need to remove it
    Array.from(this.#serverDescriptions.keys()).forEach((hostAndPort) => {
      if (!peers.includes(hostAndPort)) {
        this.#serverDescriptions.delete(hostAndPort);
      }
    });

    this.checkIfHasPrimary();
  }

  updateRSWithPrimaryFromMember(
    hostAndPort: string,
    response: IsMasterResponse,
    props: Partial<ServerDescription> & { type: ServerType },
  ) {
    if (!this.#serverDescriptions.has(hostAndPort)) {
      // While this request was on flight, another server has already
      // reported this server not to be part of the replica set. Remove
      // and bail
      return;
    }

    if (this.#setName !== response.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    // TODO: Is this correct? Specs often omit "me"
    if (response.me && hostAndPort !== response.me) {
      this.#serverDescriptions.delete(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    this.#serverDescriptions.set(hostAndPort, {
      ...props,
      setName: response.setName || null,
      minWireVersion: response.minWireVersion,
      maxWireVersion: response.maxWireVersion,
      hosts: response.hosts || [],
    });

    if (
      !Array.from(this.#serverDescriptions.values()).some((desc) =>
        desc.type === "RSPrimary"
      )
    ) {
      this.#type = "ReplicaSetNoPrimary";
      if (response.primary) {
        const possiblePrimary = this.#serverDescriptions.get(response.primary);
        if (possiblePrimary?.type === "Unknown") {
          possiblePrimary.type = "PossiblePrimary";
        }
      }
    }
  }

  updateRSWithoutPrimary(
    hostAndPort: string,
    response: IsMasterResponse,
    props: Partial<ServerDescription> & { type: ServerType },
  ) {
    if (!this.#serverDescriptions.has(hostAndPort)) {
      // While this request was in flight, another server has already
      // reported this server not being part of the replica set
      return;
    }

    // TODO: How to type things better so no bang?
    if (!this.#setName) {
      this.#setName = response.setName!;
    } else if (this.#setName !== response.setName) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }

    this.#serverDescriptions.set(hostAndPort, {
      ...props,
      setName: response.setName || null,
      minWireVersion: response.minWireVersion,
      maxWireVersion: response.maxWireVersion,
      hosts: response.hosts || [],
    });

    const peers = [
      ...(response.hosts || []),
      ...(response.arbiters || []),
      ...(response.passives || []),
    ];

    peers
      .filter((hostAndPort) => !this.#serverDescriptions.has(hostAndPort))
      .forEach((hostAndPort) => this.addInitialServerDescription(hostAndPort));

    if (
      !Array.from(this.#serverDescriptions.values()).some((desc) =>
        desc.type === "RSPrimary"
      )
    ) {
      this.#type = "ReplicaSetNoPrimary";
      if (response.primary) {
        const possiblePrimary = this.#serverDescriptions.get(response.primary);
        if (possiblePrimary?.type === "Unknown") {
          possiblePrimary.type = "PossiblePrimary";
        }
      }
    }

    // TODO: Is this right? Atleast RSOther doesn't have "me" prop
    // but it needs to be kept in set
    if (response.me && hostAndPort !== response.me) {
      this.#serverDescriptions.delete(hostAndPort);
      return;
    }
  }

  checkIfHasPrimary() {
    for (const description of this.#serverDescriptions.values()) {
      if (description.type === "RSPrimary") {
        this.#type = "ReplicaSetWithPrimary";
        return;
      }
    }
    this.#type = "ReplicaSetNoPrimary";
  }

  updateServerDescription(
    hostAndPort: string,
    response: IsMasterResponse,
  ) {
    hostAndPort = hostAndPort.toLowerCase();
    response = normaliseResponse(response);

    if (response.ok !== 1) {
      this.addInitialServerDescription(hostAndPort);
      this.checkIfHasPrimary();
      return;
    }

    const props: Partial<ServerDescription> & { type: ServerType } = {
      type: typeFromResponse(response),
    };
    if (
      response.logicalSessionTimeoutMinutes !== null &&
      response.logicalSessionTimeoutMinutes !== undefined
    ) {
      props.logicalSessionTimeoutMinutes =
        response.logicalSessionTimeoutMinutes;
    }
    props.topologyVersion = response.topologyVersion || null;
    if (response.topologyVersion) {
      if (
        topologyVersionIsStale(
          response.topologyVersion,
          this.#serverDescriptions.get(hostAndPort)?.topologyVersion,
        )
      ) {
        console.warn(`Received stale response...`);
        return;
      }
    }

    const updateLogicalSessionTimeoutMinutes = () => {
      const descs = Array.from(this.#serverDescriptions.values()).filter(
        (desc) =>
          desc.type === "RSPrimary" ||
          desc.type === "RSSecondary" ||
          desc.type === "Mongos" ||
          desc.type === "Standalone",
      );
      const timeouts = descs.reduce<number[]>(
        (acc, { logicalSessionTimeoutMinutes }) =>
          logicalSessionTimeoutMinutes !== null &&
            logicalSessionTimeoutMinutes !== undefined
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
    };

    switch (props.type) {
      case "RSPrimary":
        this.updateRSFromPrimary(hostAndPort, response, props);
        updateLogicalSessionTimeoutMinutes();
        break;
      case "RSArbiter":
      case "RSOther":
      case "RSSecondary":
        if (this.#type === "ReplicaSetWithPrimary") {
          this.updateRSWithPrimaryFromMember(hostAndPort, response, props);
        }
        if (this.#type === "ReplicaSetNoPrimary" || this.#type === "Unknown") {
          this.updateRSWithoutPrimary(hostAndPort, response, props);
        }
        updateLogicalSessionTimeoutMinutes();
        break;
      case "Mongos":
        this.#serverDescriptions.delete(hostAndPort);
        this.checkIfHasPrimary();
        updateLogicalSessionTimeoutMinutes();
        break;
      case "RSGhost":
        this.#serverDescriptions.set(hostAndPort, {
          ...props,
          minWireVersion: response.minWireVersion,
          maxWireVersion: response.maxWireVersion,
          hosts: response.hosts || [],
          setName: response.setName || null,
        });
        if (this.#type === "ReplicaSetWithPrimary") this.checkIfHasPrimary();
        break;
      case "Standalone":
        if (this.#type === "Unknown") {
          if (!this.#serverDescriptions.has(hostAndPort)) return;
          if (this.#seeds.length === 1) {
            this.#type = "Single";
          } else {
            this.#serverDescriptions.delete(hostAndPort);
          }
        } else if (this.#type === "ReplicaSetWithPrimary") {
          this.#serverDescriptions.delete(hostAndPort);
          this.checkIfHasPrimary();
        } else {
          this.#serverDescriptions.delete(hostAndPort);
        }
        break;
      case "Unknown":
        this.#serverDescriptions.delete(hostAndPort);
        this.checkIfHasPrimary();
        return;
      default:
        throw new Error(
          `Not handling implemented for response ${typeFromResponse(response)}`,
        );
    }
  }

  describe() {
    type TopologyDescription = {
      servers: Record<string, TopologyServerDescription>;
      setName?: string | null;
      topologyType: TopologyType;
      logicalSessionTimeoutMinutes: number | null;
      compatible?: boolean;
      maxSetVersion?: number;
      maxElectionId?: ElectionId;
    };
    type TopologyServerDescription = {
      setName?: string | null;
      type: ServerType;
      topologyVersion?: TopologyVersion | null;
      electionId?: ElectionId | null;
      setVersion?: number | null;
    };
    const description: TopologyDescription = {
      servers: Array.from(this.#serverDescriptions).reduce(
        (acc, [hostAndPort, desc]) => {
          const serverDescription: TopologyServerDescription = {
            setName: desc.setName,
            type: desc.type,
          };
          serverDescription.topologyVersion = desc.topologyVersion;
          serverDescription.setVersion = desc.setVersion;
          serverDescription.electionId = desc.electionId;
          return {
            ...acc,
            [hostAndPort]: serverDescription,
          };
        },
        {},
      ),
      setName: this.#setName,
      topologyType: this.#type,
      logicalSessionTimeoutMinutes: this.#logicalSessionTimeoutMinutes,
    };
    description.compatible = this.isCompatible();
    description.maxSetVersion = this.#maxSetVersion;
    description.maxElectionId = this.#maxElectionId;
    return description;
  }

  isCompatible() {
    const descs = Array.from(this.#serverDescriptions.values());
    if (
      descs.some((desc) =>
        desc.type !== "Unknown" &&
        desc.type !== "PossiblePrimary" &&
        desc.maxWireVersion < this.#minWireVersion
      )
    ) {
      return false;
    }
    if (
      descs.some((desc) =>
        desc.type !== "Unknown" &&
        desc.type !== "PossiblePrimary" &&
        desc.minWireVersion > this.#maxWireVersion
      )
    ) {
      return false;
    }
    return true;
  }
}
