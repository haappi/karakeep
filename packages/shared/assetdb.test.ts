import * as fs from "fs";
import * as mockfs from "mock-fs";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as zstd from "zstd.ts";

import { ASSET_TYPES, readAsset, saveAsset } from "./assetdb";

// import * as lz4 from 'lz4';
