// TODO: Move to deps
import { format } from "https://deno.land/std@0.95.0/datetime/mod.ts";

// TODO: Use logging library
export function info(thing: unknown) {
  console.log(
    `${format(new Date(), "yyyy-MM-ddTHH:mm:ss.SSS")}: ${thing}`,
  );
}
