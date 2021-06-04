import uriTests from "./cases/00_uri.ts";
import authTests from "./cases/01_auth.ts";
import connectTests from "./cases/02_connect.ts";
import curdTests from "./cases/03_curd.ts";
import indexesTests from "./cases/04_indexes.ts";
import srvTests from "./cases/05_srv.ts";
import unifiedTests from "./unified_specs/unifiedRunner.ts";

import cleanup from "./cases/99_cleanup.ts";

uriTests();
authTests();
connectTests();
curdTests();
indexesTests();
srvTests();
unifiedTests();

cleanup();
