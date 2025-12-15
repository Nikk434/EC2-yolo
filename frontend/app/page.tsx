"use client";

import React, { useState, ChangeEvent, useRef, useEffect } from "react";
import { Upload, ArrowRight, Loader2 } from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

export default function ImageProcessor() {
  const [inputImage, setInputImage] = useState<string | null>(null);
  const [outputImage, setOutputImage] = useState<string | null>(null);
  const [filename, setFilename] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [instanceRunning, setInstanceRunning] = useState(true);

  const pollingRef = useRef<NodeJS.Timeout | null>(null);

  // ---------- EC2 STATUS ----------
  useEffect(() => {
    fetch("/api/ec2-status")
      .then(res => res.json())
      .then(data => setInstanceRunning(data.state === "running"))
      .catch(() => setInstanceRunning(false));
  }, []);

  // ---------- IMAGE SELECT ----------
  const handleImageUpload = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setFilename(file.name);

    const reader = new FileReader();
    reader.onloadend = () => {
      setInputImage(reader.result as string);
      setOutputImage(null);
    };
    reader.readAsDataURL(file);
  };

  // ---------- CLEAR BUCKETS ----------
  const clearBuckets = async () => {
    setClearing(true);
    setInputImage(null);
    setOutputImage(null);
    setFilename(null);

    if (pollingRef.current) clearInterval(pollingRef.current);

    try {
      await fetch("/api/clear-buckets", { method: "POST" });
    } catch (err) {
      console.error(err);
    } finally {
      setClearing(false);
    }
  };

  // ---------- UPLOAD + POLL ----------
  const uploadAndWait = async () => {
    if (!inputImage || !filename) return;

    if (!instanceRunning) {
      alert("EC2 instance is stopped");
      return;
    }

    setLoading(true);

    try {
      // 1️⃣ Get presigned URL
      const presignRes = await fetch("/api/get-upload-url", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ filename }),
      });

      if (!presignRes.ok) throw new Error("Failed to get upload URL");
      const { url, fields } = await presignRes.json();

      // 2️⃣ Upload to S3
      const blob = await (await fetch(inputImage)).blob();
      const formData = new FormData();
      Object.entries(fields).forEach(([k, v]) => formData.append(k, v as string));
      formData.append("file", blob, filename);

      const uploadResponse = await fetch(url, { method: "POST", body: formData });
      if (!uploadResponse.ok) {
        const text = await uploadResponse.text();
        console.error("S3 Upload failed:", text);
        throw new Error("Upload failed");
      }

      // 3️⃣ Poll for output from S3
      let timeout = 120000; // 2 min
      pollingRef.current = setInterval(async () => {
        timeout -= 5000;

        try {
          const res = await fetch("/api/check-output", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ filename }),
          });

          const data = await res.json();
          if (data.exists && data.processedImageUrl) {
            setOutputImage(data.processedImageUrl);
            setLoading(false);
            if (pollingRef.current) clearInterval(pollingRef.current);
          }

          if (timeout <= 0) {
            setLoading(false);
            if (pollingRef.current) clearInterval(pollingRef.current);
            alert("Processing timeout");
          }
        } catch (err) {
          console.error("Polling error:", err);
        }
      }, 5000);
    } catch (err) {
      console.error("Upload/Processing error:", err);
      alert("Failed to process image");
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex items-center justify-center p-8">
      <div className="w-full max-w-6xl">
        <h1 className="text-4xl font-bold text-white mb-12 text-center">
          Image Processor
        </h1>

        {!instanceRunning && (
          <p className="text-red-400 text-center mb-4">
            EC2 instance is stopped
          </p>
        )}

        <div className="grid grid-cols-1 md:grid-cols-[1fr_auto_1fr] gap-6 items-center">
          {/* Input Box */}
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur p-8 h-[500px] flex flex-col">
            <h2 className="text-xl font-semibold text-white mb-4">
              Input Image
            </h2>

            {!inputImage ? (
              <label
                onClick={clearBuckets}
                className="flex-1 border-2 border-dashed border-slate-600 rounded-lg flex flex-col items-center justify-center cursor-pointer hover:border-slate-500 transition-colors relative"
              >
                {clearing && (
                  <div className="absolute inset-0 bg-slate-900/80 flex items-center justify-center z-10 rounded-lg">
                    <Loader2 className="w-10 h-10 text-blue-500 animate-spin" />
                  </div>
                )}
                <Upload className="w-16 h-16 text-slate-400 mb-4" />
                <span className="text-slate-400 mb-2">Click to upload image</span>
                <span className="text-sm text-slate-500">PNG, JPG up to 10MB</span>
                <input
                  type="file"
                  accept="image/*"
                  onChange={handleImageUpload}
                  className="hidden"
                />
              </label>
            ) : (
              <div className="flex-1 relative rounded-lg overflow-hidden bg-slate-900">
                <img src={inputImage} alt="Input" className="w-full h-full object-contain" />
                <button
                  onClick={clearBuckets}
                  className="absolute top-2 right-2 bg-red-500 text-white px-3 py-1 rounded text-sm hover:bg-red-600"
                >
                  Remove
                </button>
              </div>
            )}
          </Card>

          {/* Process Button */}
          <div className="flex justify-center">
            <Button
              onClick={uploadAndWait}
              disabled={!inputImage || loading || !instanceRunning}
              className="bg-blue-600 hover:bg-blue-700 disabled:bg-slate-700 disabled:cursor-not-allowed px-6 py-6"
            >
              <ArrowRight className="w-6 h-6" />
            </Button>
          </div>

          {/* Output Box */}
          <Card className="bg-slate-800/50 border-slate-700 backdrop-blur p-8 h-[500px] flex flex-col">
            <h2 className="text-xl font-semibold text-white mb-4">
              Processed Image
            </h2>

            <div className="flex-1 border-2 border-dashed border-slate-600 rounded-lg flex items-center justify-center bg-slate-900">
              {loading ? (
                <Loader2 className="w-16 h-16 text-blue-500 animate-spin" />
              ) : outputImage ? (
                <img src={outputImage} alt="Output" className="w-full h-full object-contain rounded-lg" />
              ) : (
                <span className="text-slate-500">Processed image will appear here</span>
              )}
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}
