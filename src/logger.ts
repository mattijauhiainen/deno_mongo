// TODO: Move to deps
import { format } from "https://deno.land/std@0.95.0/datetime/mod.ts";

const loggingEnabled = Deno.env.get("ENABLE_MONGO_LOGGING") === "true" ?? false;
// TODO: Use logging library
export function info(thing: unknown) {
  if (loggingEnabled) {
    console.log(
      `${format(new Date(), "yyyy-MM-ddTHH:mm:ss.SSS")}: ${thing}`,
    );
  }
}
