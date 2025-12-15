import { NextResponse } from "next/server";
import {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";

export const runtime = "nodejs";

const s3 = new S3Client({
  region: process.env.AWS_REGION!,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

async function clearBucket(bucket: string) {
  const listed = await s3.send(
    new ListObjectsV2Command({ Bucket: bucket })
  );

  if (!listed.Contents?.length) return;

  await s3.send(
    new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: {
        Objects: listed.Contents.map(obj => ({ Key: obj.Key! })),
      },
    })
  );
}

export async function POST() {
  await clearBucket(process.env.S3_INPUT_BUCKET_NAME!);
  await clearBucket(process.env.S3_OUTPUT_BUCKET_NAME!);

  return NextResponse.json({ cleared: true });
}
