// In-memory state survives sleep/wake because guest RAM is snapshotted.
const http = require("http");
const bootedAt = Date.now();
let hits = 0;
http
  .createServer((req, res) => {
    hits++;
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ hello: "from a microVM", hits, pid: process.pid, bootedAt }) + "\n");
  })
  .listen(process.env.PORT || 3000, "0.0.0.0", () => console.log("listening on", process.env.PORT || 3000));
