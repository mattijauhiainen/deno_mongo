import { assertEquals } from "../test.deps.ts";
import { parse } from "../../src/utils/uri.ts";
// TODO: Fix import
import { parse as parseYaml } from "https://deno.land/std/encoding/yaml.ts";

import { IsMasterResponse, Topology } from "../../src/Topology.ts";

interface TestSample {
  description: string;
  uri: string;
  phases: Phase[];
}

interface Phase {
  responses: [string, IsMasterResponse][];
  outcome: Record<string, unknown>;
}

function getSpecs(
  dir: string,
  specsToRun: RegExp[] = [],
): TestSample[] {
  const samples = Array.from(Deno.readDirSync(dir)).filter((file) =>
    file.isFile && /.yml$/.test(file.name)
  )
    .map((file) => Deno.readTextFileSync(`${dir}/${file.name}`))
    .map((text) => parseYaml(text) as TestSample);
  console.log(samples.map((sample) => sample.description));

  return samples
    .filter((specDesc) =>
      specsToRun.length === 0 ||
      specsToRun.some((regex) => regex.test(specDesc.description))
    );
}

async function runSpec() {
  const specDir =
    "/Users/matti/work/rebootramen/deno_scrapers/deno_mongo/tests/unified_specs";
  const rsSamples = getSpecs(
    `${specDir}/rs`,
    [
      /Primary becomes mongos/,
      /Discover ghost with replicaSet URI option/,
      /Incompatible ghost/,
      /Member brought up as standalone/,
      /Primary with older topologyVersion/,
      /Wrong setName/,
      /Secondary wrong setName with primary/,
      /Secondary with mismatched 'me' tells us who the primary is/,
      /Primary changes setName/,
      /Member removed by reconfig/,
      /setVersion is ignored if there is no electionId/,
      /Discover RSOther with directConnection URI option/,
      /Secondary mismatched me/,
      /Primary wrong setName/,
      /Discover primary with directConnection URI option/,
      /Discover arbiters with replicaSet URI option/,
      /Discover passives with directConnection URI option/,
      /Primary mismatched me/,
      /Discover ghost with directConnection URI option/,
      /Replica set case normalization/,
      /Replica set member with large minWireVersion/,
      /Replica set discovery/,
      /New primary with equal electionId/,
      /Primary mismatched me is not removed/,
      /Host list differs from seeds/,
      /Disconnected from primary/,
      /Discover secondary with directConnection URI option/,
      /Discover RSOther with replicaSet URI option/,
      /Primary becomes a secondary with wrong setName/,
      /Secondary wrong setName/,
      /Parse logicalSessionTimeoutMinutes from replica set/,
      /Primary becomes ghost/,
      /Replica set mixed case normalization/,
      /Record max setVersion, even from primary without electionId/,
      /New primary/,
      /Primary reports a new member/,
      /Replica set member and an unknown server/,
      /Replica set member with default maxWireVersion of 0/,
      /Primary becomes standalone/,
      /Repeated ismaster response must be processed/,
      /Secondary's host list is not authoritative/,
      /Discover secondary with replicaSet URI option/,
      /Incompatible arbiter/,
      /New primary with wrong setName/,
      /Disconnected from primary, reject primary with stale electionId/,
      /Primary to no primary with mismatched me/,
      /Disconnected from primary, reject primary with stale setVersion/,
      /Primary with equal topologyVersion/,
      /Unexpected mongos/,
      /Non replicaSet member responds/,
      /Response from removed server/,
      /New primary/,
      /Discover hidden with replicaSet URI option/,
      /Primary with newer topologyVersion/,
      /New primary with greater setVersion/,
      /Discover hidden with directConnection URI option/,
      /Incompatible other/,
      /New primary with greater setVersion and electionId/,
      /Primaries with and without electionIds/,
      /replicaSet URI option causes starting topology to be RSNP/,
      /Replica set member with large maxWireVersion/,
      /Discover arbiters with directConnection URI option/,
      /Discover passives with replicaSet URI option/,
      /Discover primary with replicaSet URI optio/,
    ],
  );

  const shardedSamples = getSpecs(
    `${specDir}/sharded`,
    [
      /Parse logicalSessionTimeoutMinutes from mongoses/,
      /Non-Mongos server in sharded cluster/,
      /Multiple mongoses with large minWireVersion/,
      /Discover single mongos/,
      /Normalize URI case/,
      /Multiple mongoses with default maxWireVersion of 0/,
      /Multiple mongoses/,
      /Mongos disconnect/,
      /Multiple mongoses with large maxWireVersion/,
    ],
  );

  const singleSamples = getSpecs(
    `${specDir}/single`,
  );

  const errorsSamples = getSpecs(
    `${specDir}/errors`,
    [
      /Non-stale network error/,
      /Post-4.2 ShutdownInProgress error/,
      /Pre-4.2 NotMasterNoSlaveOk error/,
      /Stale generation InterruptedAtShutdown error afterHandshakeCompletes/,
      /Stale generation network error afterHandshakeCompletes/,
      /Post-4.2 InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion missing NotMasterNoSlaveOk error/,
      /Stale generation PrimarySteppedDown error/,
      /Stale generation ShutdownInProgress error/,
      /Pre-4.2 InterruptedAtShutdown error/,
      /Stale generation InterruptedDueToReplStateChange error beforeHandshakeCompletes/,
      /Post-4.2 PrimarySteppedDown error/,
      /writeErrors field is ignored/,
      /Network timeouts before and after the handshake completes/,
      /Stale generation ShutdownInProgress error afterHandshakeCompletes/,
      /Stale generation InterruptedDueToReplStateChange error afterHandshakeCompletes/,
      /Stale generation NotMaster error beforeHandshakeCompletes/,
      /Stale generation PrimarySteppedDown error afterHandshakeCompletes/,
      /Post-4.2 NotMaster error/,
      /Non-stale topologyVersion missing InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion greater NotMasterNoSlaveOk error/,
      /Stale generation timeout error beforeHandshakeCompletes/,
      /Stale generation ShutdownInProgress error beforeHandshakeCompletes/,
      /Stale generation LegacyNotPrimary error beforeHandshakeCompletes/,
      /Post-4.2 NotMasterOrSecondary error/,
      /Stale generation PrimarySteppedDown error beforeHandshakeCompletes/,
      /Stale topologyVersion NotMasterNoSlaveOk error/,
      /Non-stale topologyVersion proccessId changed InterruptedAtShutdown error/,
      /Stale generation LegacyNotPrimary error afterHandshakeCompletes/,
      /Non-stale topologyVersion missing InterruptedAtShutdown error/,
      /Non-stale topologyVersion proccessId changed NotMasterOrSecondary error/,
      /Non-stale topologyVersion greater NotMaster error/,
      /Non-stale topologyVersion proccessId changed NotMasterNoSlaveOk error/,
      /Non-stale topologyVersion greater LegacyNotPrimary error/,
      /Stale generation NotMasterOrSecondary error beforeHandshakeCompletes/,
      /Pre-4.2 InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion proccessId changed LegacyNotPrimary error/,
      /Non-stale network timeout error/,
      /Non-stale topologyVersion proccessId changed NotMaster error/,
      /Stale topologyVersion NotMasterOrSecondary error/,
      /Stale generation NotMasterOrSecondary error/,
      /Non-stale topologyVersion proccessId changed InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion greater InterruptedDueToReplStateChange error/,
      /Stale generation NotMasterNoSlaveOk error afterHandshakeCompletes/,
      /Stale topologyVersion InterruptedAtShutdown error/,
      /Pre-4.2 LegacyNotPrimary error/,
      /Pre-4.2 PrimarySteppedDown error/,
      /Post-4.2 LegacyNotPrimary error/,
      /Non-stale topologyVersion missing PrimarySteppedDown error/,
      /Stale generation NotMasterNoSlaveOk error/,
      /Non-stale topologyVersion missing ShutdownInProgress error/,
      /Stale generation network error beforeHandshakeCompletes/,
      /Pre-4.2 ShutdownInProgress error/,
      /Post-4.2 NotMasterNoSlaveOk error/,
      /Stale generation NotMaster error afterHandshakeCompletes/,
      /Non-stale topologyVersion missing NotMaster error/,
      /Pre-4.2 NotMasterOrSecondary error/,
      /Non-stale topologyVersion greater InterruptedAtShutdown error/,
      /Non-stale topologyVersion proccessId changed PrimarySteppedDown error/,
      /Stale generation InterruptedAtShutdown error beforeHandshakeCompletes/,
      /Stale generation InterruptedDueToReplStateChange error/,
      /Non-stale topologyVersion proccessId changed ShutdownInProgress error/,
      /Stale generation InterruptedAtShutdown error/,
      /Stale generation NotMasterOrSecondary error afterHandshakeCompletes/,
      /Do not check errmsg when code exists/,
      /Pre-4.2 NotMaster error/,
      /Stale generation NotMaster error/,
      /Non-stale topologyVersion greater PrimarySteppedDown error/,
      /Stale topologyVersion ShutdownInProgress error/,
      /Stale topologyVersion InterruptedDueToReplStateChange error/,
      /Stale topologyVersion NotMaster error/,
      /Non-stale topologyVersion greater NotMasterOrSecondary error/,
      /Non-stale topologyVersion missing LegacyNotPrimary error/,
      /Stale generation NotMasterNoSlaveOk error beforeHandshakeCompletes/,
      /Stale topologyVersion LegacyNotPrimary error/,
      /Stale generation timeout error afterHandshakeCompletes/,
      /Stale topologyVersion PrimarySteppedDown error/,
      /Post-4.2 InterruptedAtShutdown error/,
      /Non-stale topologyVersion greater ShutdownInProgress error/,
      /Non-stale topologyVersion missing NotMasterOrSecondary error/,
    ],
  );

  const samplesToRun = [
    ...rsSamples,
    ...shardedSamples,
    ...singleSamples,
    ...errorsSamples,
  ];
  for (const testSample of samplesToRun) {
    console.log(`${testSample.description}...`);
    const options = await parse(testSample.uri);
    const topology = new Topology(options.servers, options);
    testSample.phases.forEach((phase, phaseIndex) => {
      console.log(`\tPhase ${phaseIndex + 1}`);
      phase.responses.forEach((response) => {
        topology.updateServerDescription(
          response[0] as string,
          response[1] as IsMasterResponse,
        );
      });
      try {
        const pathsToCheck = getPaths(phase.outcome);
        const actual = {};
        pathsToCheck.forEach((path) => pick(topology.describe(), actual, path));
        assertEquals(actual, phase.outcome);
      } catch (error) {
        console.log(JSON.stringify(testSample, null, 2));
        console.log(JSON.stringify(topology.describe(), null, 2));
        throw error;
      }
    });
    console.log("ok");
  }
}

function pick(
  src: Record<string, unknown>,
  dest: Record<string, unknown>,
  path: string,
): Record<string, unknown> {
  const segments = path.split(".");
  for (const segment of segments) {
    const value = src[segment];
    if (value !== null && !Array.isArray(value) && typeof value === "object") {
      dest[segment] = dest[segment] || {};
      dest = dest[segment] as Record<string, unknown>;
      src = src[segment] as Record<string, unknown>;
    } else {
      dest[segment] = value;
    }
  }
  return dest;
}

function getPaths(
  obj: Record<string, unknown>,
  paths: string[] = [],
  basePath = "",
): string[] {
  const entries = Object.entries(obj);
  if (entries.length === 0) {
    return [...paths, basePath];
  }
  entries.forEach(([key, value]: [string, unknown]) => {
    const path = basePath ? `${basePath}.${key}` : key;
    if (Array.isArray(value)) {
      // TODO: Might need a way to map object arrays and nested arrays
      paths.push(path);
    } else if (value === null) {
      paths.push(path);
    } else if (typeof value === "object") {
      const nested = getPaths(value as Record<string, unknown>, paths, path);
      paths = [...paths, ...nested];
    } else {
      paths.push(path);
    }
  });
  return paths;
}

runSpec();
