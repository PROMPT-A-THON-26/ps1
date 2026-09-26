(function () {
  "use strict";

  const params = new URLSearchParams(window.location.search);
  const baseUrl = params.get("baseUrl")
    || window.__VAULT_API_BASE_URL
    || (window.location.hostname
      ? window.location.protocol + "//" + window.location.hostname + ":8000/api/v1"
      : "http://localhost:8000/api/v1");

  // Live Part B is the only production mode. No demo/mock data is loaded.
  window.VAULT_CONFIG = { mode: "api", baseUrl };
})();
