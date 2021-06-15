import {lambda} from "./lambda";
import fetch from "node-fetch";

const main = async () => {
  const host = process.env.AWS_LAMBDA_RUNTIME_API;
  if (!host) throw new Error(`FIXME bad env ${JSON.stringify(process.env)}`)
  const base = `http://${host}/2018-06-01`;

  for (;;) {
    const resp = await fetch(`${base}/runtime/invocation/next`);
    if (resp.status !== 200)
      throw new Error(`FIXME next: ${resp.status}; ${await resp.text()}`);

    const id = resp.headers.get("Lambda-Runtime-Aws-Request-Id");
    if (!id) throw new Error("FIXME request without id :(");

    const event = await resp.json();
    console.debug(JSON.stringify(event));  // you may want to remove this

    await lambda(event, null as any, null as any);
    await fetch(`${base}/runtime/invocation/${id}/response`, { method: "POST", body: "SUCCESS" });
    // you may want to report failures too
  }
};

(async () => {
  try {
    await main();
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
})();