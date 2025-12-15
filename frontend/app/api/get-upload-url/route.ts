import { NextRequest, NextResponse } from "next/server";
import { S3Client } from "@aws-sdk/client-s3";
import { createPresignedPost } from "@aws-sdk/s3-presigned-post";

export const runtime = "nodejs";

const s3 = new S3Client({
  region: process.env.AWS_REGION!,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
});

export async function POST(req: NextRequest) {
  const { filename } = await req.json();

  if (!filename) {
    return NextResponse.json({ error: "Filename required" }, { status: 400 });
  }

  const { url, fields } = await createPresignedPost(s3, {
    Bucket: process.env.S3_INPUT_BUCKET_NAME!,
    Key: filename,
    Conditions: [
      ["content-length-range", 0, 10 * 1024 * 1024], // 0-10MB
    ],
    Expires: 600, // 10 minutes
  });

  return NextResponse.json({ url, fields });
}
