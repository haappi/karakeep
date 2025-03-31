import * as fs from "fs";
import * as path from "path";
import { Readable } from "stream";
import { gunzip, gzip } from "zlib";
import { Glob } from "glob";
import * as lz4 from "lz4";
import { z } from "zod";
import * as zstd from "zstd.ts";

import serverConfig from "./config";
import logger from "./logger";

const COMPRESSION_TYPE: string = serverConfig.compressionType;
const COMPRESSION_LEVEL: number = serverConfig.compressionLevel;

const ROOT_PATH = serverConfig.assetsDir;

export const enum ASSET_TYPES {
  IMAGE_JPEG = "image/jpeg",
  IMAGE_PNG = "image/png",
  IMAGE_WEBP = "image/webp",
  APPLICATION_PDF = "application/pdf",
  TEXT_HTML = "text/html",
  VIDEO_MP4 = "video/mp4",
}

export const IMAGE_ASSET_TYPES: Set<string> = new Set<string>([
  ASSET_TYPES.IMAGE_JPEG,
  ASSET_TYPES.IMAGE_PNG,
  ASSET_TYPES.IMAGE_WEBP,
]);

// The assets that we allow the users to upload
export const SUPPORTED_UPLOAD_ASSET_TYPES: Set<string> = new Set<string>([
  ...IMAGE_ASSET_TYPES,
  ASSET_TYPES.TEXT_HTML,
  ASSET_TYPES.APPLICATION_PDF,
]);

// The assets that we allow as a bookmark of type asset
export const SUPPORTED_BOOKMARK_ASSET_TYPES: Set<string> = new Set<string>([
  ...IMAGE_ASSET_TYPES,
  ASSET_TYPES.APPLICATION_PDF,
]);

// The assets that we support saving in the asset db
export const SUPPORTED_ASSET_TYPES: Set<string> = new Set<string>([
  ...SUPPORTED_UPLOAD_ASSET_TYPES,
  ASSET_TYPES.TEXT_HTML,
  ASSET_TYPES.VIDEO_MP4,
]);

function getAssetDir(userId: string, assetId: string) {
  return path.join(ROOT_PATH, userId, assetId);
}

export const zAssetMetadataSchema = z.object({
  contentType: z.string(),
  fileName: z.string().nullish(),
  originalSize: z.number().nullish(),
});

export function newAssetId() {
  return crypto.randomUUID();
}

export async function saveAsset({
  userId,
  assetId,
  asset,
  metadata,
}: {
  userId: string;
  assetId: string;
  asset: Buffer;
  metadata: z.infer<typeof zAssetMetadataSchema>;
}) {
  if (!SUPPORTED_ASSET_TYPES.has(metadata.contentType)) {
    throw new Error("Unsupported asset type");
  }
  const assetDir = getAssetDir(userId, assetId);
  await fs.promises.mkdir(assetDir, { recursive: true });

  metadata.originalSize = asset.byteLength;

  const [compressed, extension] = await compress(asset, COMPRESSION_TYPE);

  await Promise.all([
    fs.promises.writeFile(
      path.join(assetDir, "asset.bin" + extension),
      compressed,
    ),
    fs.promises.writeFile(
      path.join(assetDir, "metadata.json"),
      JSON.stringify(metadata),
    ),
  ]);
}

export async function saveAssetFromFile({
  userId,
  assetId,
  assetPath,
  metadata,
}: {
  userId: string;
  assetId: string;
  assetPath: string;
  metadata: z.infer<typeof zAssetMetadataSchema>;
}) {
  if (!SUPPORTED_ASSET_TYPES.has(metadata.contentType)) {
    throw new Error("Unsupported asset type");
  }
  const assetDir = getAssetDir(userId, assetId);
  await fs.promises.mkdir(assetDir, { recursive: true });

  await Promise.all([
    // We'll have to copy first then delete the original file as inside the docker container
    // we can't move file between mounts.
    fs.promises.copyFile(assetPath, path.join(assetDir, "asset.bin")),
    // Little to no point in compressing uploaded assets as they are
    // primarily media files
    fs.promises.writeFile(
      path.join(assetDir, "metadata.json"),
      JSON.stringify(metadata),
    ),
  ]);
  await fs.promises.rm(assetPath);
}

async function getAssetPath(assetDir: string): Promise<string> {
  for (const ext of ["", ".zst", ".lz4", ".gz"]) {
    const pathWithExt = path.join(assetDir, `asset.bin${ext}`);
    try {
      await fs.promises.access(pathWithExt);
      return pathWithExt;
    } catch (err) {
      // file doesn't exist, proceed to next.
    }
  }
  return "";
}

export async function readAsset({
  userId,
  assetId,
}: {
  userId: string;
  assetId: string;
}) {
  const assetDir = getAssetDir(userId, assetId);
  const assetPath: string = await getAssetPath(assetDir);

  if (!assetPath) {
    throw new Error("Asset file not found: " + assetId);
  }

  const data: Buffer = await fs.promises.readFile(assetPath);
  const decompressedData: Buffer = await decompress(data, getDataType(data));

  const [asset, metadataStr] = await Promise.all([
    decompressedData,
    fs.promises.readFile(path.join(assetDir, "metadata.json"), {
      encoding: "utf8",
    }),
  ]);

  const metadata = zAssetMetadataSchema.parse(JSON.parse(metadataStr));
  return { asset, metadata };
}

export async function createAssetReadStream({
  userId,
  assetId,
  start,
  end,
}: {
  userId: string;
  assetId: string;
  start?: number;
  end?: number;
}) {
  const assetDir = getAssetDir(userId, assetId);
  const assetPath: string = await getAssetPath(assetDir);

  const stream = new Readable();
  const data: Buffer = await fs.promises.readFile(assetPath);
  logger.debug(getDataType(data));
  const decompressedData: Buffer = await decompress(data, getDataType(data));

  if (start !== undefined && end !== undefined) {
    const slicedBuffer = decompressedData.subarray(start, end);
    stream.push(slicedBuffer);
  } else {
    stream.push(decompressedData);
  }

  stream.push(null);

  return stream;
}

export async function readAssetMetadata({
  userId,
  assetId,
}: {
  userId: string;
  assetId: string;
}) {
  const assetDir = getAssetDir(userId, assetId);

  const metadataStr = await fs.promises.readFile(
    path.join(assetDir, "metadata.json"),
    {
      encoding: "utf8",
    },
  );

  return zAssetMetadataSchema.parse(JSON.parse(metadataStr));
}

export async function getAssetSize({
  userId,
  assetId,
}: {
  userId: string;
  assetId: string;
}) {
  const metadata = await readAssetMetadata({ userId, assetId });
  const size = metadata.originalSize;
  if (size === undefined || size == null) {
    // this should rarely happen
    const assetDir = getAssetDir(userId, assetId);
    const assetPath: string = await getAssetPath(assetDir);
    const stat = await fs.promises.stat(assetPath);
    return stat.size; // in case of edge cases that the metadata size is incorrect
  }
  return size;
}

/**
 * Deletes the passed in asset if it exists and ignores any errors
 * @param userId the id of the user the asset belongs to
 * @param assetId the id of the asset to delete
 */
export async function silentDeleteAsset(
  userId: string,
  assetId: string | undefined,
) {
  if (assetId) {
    await deleteAsset({ userId, assetId }).catch(() => ({}));
  }
}

export async function deleteAsset({
  userId,
  assetId,
}: {
  userId: string;
  assetId: string;
}) {
  const assetDir = getAssetDir(userId, assetId);
  await fs.promises.rm(path.join(assetDir), { recursive: true });
}

export async function deleteUserAssets({ userId }: { userId: string }) {
  const userDir = path.join(ROOT_PATH, userId);
  const dirExists = await fs.promises
    .access(userDir)
    .then(() => true)
    .catch(() => false);
  if (!dirExists) {
    return;
  }
  await fs.promises.rm(userDir, { recursive: true });
}

export async function* getAllAssets() {
  const g = new Glob(`/**/**/asset.bin*`, {
    maxDepth: 3,
    root: ROOT_PATH,
    cwd: ROOT_PATH,
    absolute: false,
  });
  for await (const file of g) {
    const [userId, assetId] = file.split("/").slice(0, 2);
    const [size, metadata] = await Promise.all([
      getAssetSize({ userId, assetId }),
      readAssetMetadata({ userId, assetId }),
    ]);
    yield {
      userId,
      assetId,
      ...metadata,
      size,
    };
  }
}

export async function storeScreenshot(
  screenshot: Buffer | undefined,
  userId: string,
  jobId: string,
) {
  if (!serverConfig.crawler.storeScreenshot) {
    logger.info(
      `[Crawler][${jobId}] Skipping storing the screenshot as per the config.`,
    );
    return null;
  }
  if (!screenshot) {
    logger.info(
      `[Crawler][${jobId}] Skipping storing the screenshot as it's empty.`,
    );
    return null;
  }
  const assetId = newAssetId();
  const contentType = "image/png";
  const fileName = "screenshot.png";
  await saveAsset({
    userId,
    assetId,
    metadata: { contentType, fileName, originalSize: screenshot.byteLength },
    asset: screenshot,
  });
  logger.info(
    `[Crawler][${jobId}] Stored the screenshot as assetId: ${assetId}`,
  );
  return { assetId, contentType, fileName, size: screenshot.byteLength };
}

const compress = async (
  data: Buffer,
  type: string,
): Promise<[Buffer, string]> => {
  switch (type) {
    case "zstd": {
      return [
        await zstd.compress({
          compressLevel: Math.min(
            Math.max(Math.floor(COMPRESSION_LEVEL), 1),
            19,
          ),
          input: data,
        }),
        ".zst",
      ];
    }
    case "lz4": {
      let output = Buffer.alloc(lz4.encodeBound(data.length));
      const compressedSize: number = lz4.encodeBlockHC(
        data,
        output,
        Math.min(Math.max(Math.floor(COMPRESSION_LEVEL), 3), 12),
      );
      output = output.subarray(0, compressedSize);
      return [output, ".lz4"];
    }

    case "gzip": {
      gzip(data, (err, compressedGzip) => {
        if (err) {
          console.error("Error during gzip compression:", err);
          return [data, ""];
        } else {
          return [compressedGzip, ".gz"];
        }
      });
      return [data, ""];
    }
    default:
      return [data, ""];
  }
};

const getDataType = (data: Buffer): "zstd" | "lz4" | "gzip" | "" => {
  if (data.length < 4) {
    return "";
  }

  // magic numbers
  // zstd: 0x28B52FFD
  // gzip: 0x1F8B
  // lz4: 0x04224D18

  const zstdHeader = Buffer.from([0x28, 0xb5, 0x2f, 0xfd]);
  if (data.subarray(0, 4).equals(zstdHeader)) {
    return "zstd";
  }

  const gzipHeader = Buffer.from([0x1f, 0x8b]);
  if (data.subarray(0, 2).equals(gzipHeader)) {
    return "gzip";
  }

  const lz4Header = Buffer.from([0x04, 0x22, 0x4d, 0x18]);
  if (data.subarray(0, 4).equals(lz4Header)) {
    return "lz4";
  }

  return "";
};

const decompress = async (
  data: Buffer,
  type: "zstd" | "lz4" | "gzip" | "",
): Promise<Buffer> => {
  switch (type) {
    case "zstd": {
      try {
        return await zstd.decompress({ input: data });
      } catch (err) {
        console.error("Error during zstd decompression:", err);
        return data;
      }
    }

    case "lz4": {
      try {
        let output = Buffer.alloc(data.length);
        const size: number = lz4.decodeBlock(data, output);
        output = output.subarray(0, size);
        return output;
      } catch (err) {
        console.error("Error during lz4 decompression:", err);
        return data;
      }
    }

    case "gzip": {
      gunzip(data, (err, decompressed) => {
        if (err) {
          console.error("Error during gzip decompression:", err);
          return data;
        }
        return decompressed;
      });
      return data;
    }

    default:
      return data;
  }
};
