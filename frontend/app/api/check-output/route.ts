import { NextRequest, NextResponse } from "next/server";
import {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

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

  try {
    await s3.send(
      new HeadObjectCommand({
        Bucket: process.env.S3_OUTPUT_BUCKET_NAME!,
        Key: filename,
      })
    );

    const signedUrl = await getSignedUrl(
      s3,
      new GetObjectCommand({
        Bucket: process.env.S3_OUTPUT_BUCKET_NAME!,
        Key: filename,
      }),
      { expiresIn: 300 }
    );

    return NextResponse.json({
      exists: true,
      processedImageUrl: signedUrl,
    });
  } catch {
    return NextResponse.json({ exists: false });
  }
}
