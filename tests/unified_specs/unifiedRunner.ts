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
  const samplesToRun = getSpecs(
    "/Users/matti/work/rebootramen/deno_scrapers/deno_mongo/tests/unified_specs/rs",
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