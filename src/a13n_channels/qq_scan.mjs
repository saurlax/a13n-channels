import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

// Standard output is a private pipe to the Service. Keep all SDK diagnostics off it.
const write = process.stdout.write.bind(process.stdout);
const send = (event) => write(JSON.stringify(event) + "\n");
const finish = (event) => write(JSON.stringify(event) + "\n", () => process.exit(0));

try {
  const { startQrConnect } = await import(pathToFileURL(resolve(process.argv[2], "dist/esm/index.js")).href);
  startQrConnect({
    onQrDisplayed(url) {
      send({ kind: "qr", url });
    },
    onSuccess(credentials) {
      if (credentials.length !== 1) {
        finish({ kind: "failed" });
        return;
      }
      const { appId, appSecret } = credentials[0];
      finish({ kind: "confirmed", appId, appSecret });
    },
    onFailure() {
      finish({ kind: "failed" });
    },
  }, { displayQrCodeToConsole: false });
} catch {
  finish({ kind: "failed" });
}
