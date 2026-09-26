(function () {
  "use strict";

  const params = new URLSearchParams(window.location.search);
  const requestedMode = params.get("mode");
  const hostedStatic = /\.github\.io$|\.netlify\.app$|\.vercel\.app$/i.test(window.location.hostname);
  const mode = requestedMode === "api"
    ? "api"
    : (requestedMode === "mock" || hostedStatic ? "mock" : "api");

  const baseUrl = params.get("baseUrl")
    || (window.location.hostname
      ? window.location.protocol + "//" + window.location.hostname + ":8000/api/v1"
      : "http://localhost:8000/api/v1");

  window.VAULT_CONFIG = { mode, baseUrl };
})();