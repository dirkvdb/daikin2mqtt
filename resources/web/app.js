(() => {
  const byId = (id) => document.getElementById(id);
  const ui = {
    card: byId("connection-card"),
    badge: byId("status-badge"),
    status: byId("status-label"),
    heading: byId("state-heading"),
    description: byId("state-description"),
    action: byId("connection-action"),
    actionLabel: byId("action-label"),
    actionHelper: byId("action-helper"),
    spinner: byId("action-spinner"),
    actionIcon: byId("action-icon"),
    redirect: byId("redirect-uri"),
    feedback: byId("feedback"),
    feedbackText: byId("feedback-text"),
    dismiss: byId("dismiss-feedback"),
    network: byId("network-notice"),
    networkText: byId("network-text"),
    retry: byId("retry-status"),
    dialog: byId("revoke-dialog"),
    dialogTitle: byId("revoke-title"),
    dialogDescription: byId("revoke-description"),
    cancel: byId("cancel-revoke"),
    confirm: byId("confirm-revoke"),
  };

  const states = {
    unauthorized: {
      label: "Not connected",
      heading: "Connect your Daikin account",
      description: "Authorize daikin2mqtt to access your Daikin account. You’ll sign in securely on Daikin’s website and return here automatically.",
      action: "Connect to Daikin",
      helper: "Opens Daikin’s sign-in page in this tab.",
    },
    authorizing: {
      label: "Authorizing",
      heading: "Completing authorization",
      description: "Your sign-in callback was received. daikin2mqtt is exchanging it for access; temporary provider failures are retried automatically.",
      action: "Waiting for Daikin…",
      helper: "Checking authorization status every 5 seconds.",
    },
    authorized: {
      label: "Connected",
      heading: "Your Daikin account is connected",
      description: "daikin2mqtt is authorized to access your Daikin account. You can disconnect and revoke this authorization at any time.",
      action: "Disconnect",
      helper: "Disconnecting revokes your account authorization.",
    },
    revocation_pending: {
      label: "Revoke pending",
      heading: "Disconnection needs another attempt",
      description: "Revocation has not completed. Your Daikin authorization may still be active. Retry disconnecting to finish revoking access.",
      action: "Retry disconnect",
      helper: "Cloud access is suspended until revocation succeeds.",
    },
  };

  let status = null;
  let statusHealthy = false;
  let statusRequest = null;
  let busy = null;
  let pollTimer = null;

  function showFeedback(message, kind = "success") {
    ui.feedback.className = `notice is-${kind}`;
    ui.feedback.setAttribute("role", kind === "error" ? "alert" : "status");
    ui.feedback.setAttribute("aria-live", kind === "error" ? "assertive" : "polite");
    ui.feedbackText.textContent = message;
    ui.feedback.hidden = false;
  }

  function consumeCallbackResult() {
    const url = new URL(window.location.href);
    const result = url.searchParams.get("auth");
    // Remove OAuth-related query data without displaying or logging it.
    const keys = ["auth", "code", "state", "error", "error_description", "error_uri"];
    const hasCallbackParams = keys.some((key) => url.searchParams.has(key));
    for (const key of keys) url.searchParams.delete(key);
    if (hasCallbackParams) {
      window.history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
    }
    if (result === "success") {
      showFeedback("Daikin authorization completed.");
    } else if (result === "error") {
      showFeedback("Daikin authorization could not be completed. Check your connection status and try again.", "error");
    }
  }

  function render() {
    const view = status ? states[status.state] : null;
    const loading = Boolean(busy) || (!status && Boolean(statusRequest));
    ui.card.setAttribute("aria-busy", String(loading));
    ui.retry.disabled = Boolean(statusRequest) || Boolean(busy);
    ui.action.disabled = !statusHealthy || !status || Boolean(busy) || status.state === "authorizing";
    ui.badge.className = `status-badge${status ? ` is-${status.state}` : ""}`;
    ui.status.textContent = view ? view.label : (statusRequest ? "Checking" : "Unavailable");
    ui.heading.textContent = view ? view.heading : (statusRequest ? "Checking your connection" : "Connection status unavailable");
    ui.description.textContent = view ? view.description : (statusRequest
      ? "Please wait while we retrieve your authorization status."
      : "We couldn’t reach the status endpoint. Retry the check below; connection actions are disabled until status is available.");
    ui.redirect.textContent = status ? status.redirect_uri : "Waiting for server…";
    ui.action.className = `button ${status && ["authorized", "revocation_pending"].includes(status.state) ? "button-secondary" : "button-primary"}`;
    ui.actionLabel.textContent = busy === "start" ? "Opening Daikin…" : busy === "revoke" ? "Revoking authorization…" : view ? view.action : statusRequest ? "Checking connection…" : "Connection unavailable";
    ui.actionHelper.textContent = view ? view.helper : "Secure authorization through Daikin.";
    ui.spinner.hidden = !(loading || (status && status.state === "authorizing"));
    ui.actionIcon.hidden = !ui.spinner.hidden || !status || status.state !== "unauthorized";
  }

  function stopPolling() {
    window.clearTimeout(pollTimer);
    pollTimer = null;
  }

  function schedulePoll() {
    stopPolling();
    if (busy) return;
    pollTimer = window.setTimeout(() => {
      if (ui.dialog.open) {
        schedulePoll();
      } else {
        void refreshStatus();
      }
    }, 5000);
  }

  async function requestJson(path, options = {}) {
    const controller = new AbortController();
    // Cloud requests are bounded at 30 seconds each; both-token revocation can take longer.
    const timeout = window.setTimeout(() => controller.abort(), 150000);
    try {
      const response = await fetch(path, {
        ...options,
        credentials: "same-origin",
        cache: "no-store",
        signal: controller.signal,
        headers: { Accept: "application/json", ...options.headers },
      });
      let data;
      try {
        data = await response.json();
      } catch {
        throw new Error(response.ok
          ? "The server returned an invalid response. Please try again."
          : `The request failed (HTTP ${response.status}). Please try again.`);
      }
      if (!response.ok) {
        throw new Error(typeof data?.message === "string" && data.message.trim()
          ? data.message
          : `The request failed (HTTP ${response.status}). Please try again.`);
      }
      return data;
    } catch (error) {
      if (controller.signal.aborted) {
        throw new Error("The request timed out. Check that daikin2mqtt is running and try again.");
      }
      if (error instanceof TypeError) {
        throw new Error("Unable to reach daikin2mqtt. Check your network connection and try again.");
      }
      throw error;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  function errorMessage(error) {
    return error instanceof Error ? error.message : "Something went wrong. Please try again.";
  }

  function refreshStatus() {
    if (statusRequest) return statusRequest;
    stopPolling();
    statusRequest = (async () => {
      try {
        const data = await requestJson("/api/auth/status");
        if (!data || !Object.hasOwn(states, data.state)
          || typeof data.redirect_uri !== "string" || !data.redirect_uri.trim()
          || typeof data.csrf_token !== "string" || !data.csrf_token.trim()) {
          throw new Error("The server returned an invalid connection status. Please try again.");
        }
        status = { state: data.state, redirect_uri: data.redirect_uri, csrf_token: data.csrf_token };
        statusHealthy = true;
        ui.network.hidden = true;
      } catch (error) {
        statusHealthy = false;
        ui.networkText.textContent = `${errorMessage(error)} Connection actions are disabled until a status check succeeds.`;
        ui.network.hidden = false;
      } finally {
        statusRequest = null;
        render();
        schedulePoll();
      }
    })();
    render();
    return statusRequest;
  }

  async function performAction(kind) {
    if (busy) return;
    busy = kind;
    stopPolling();
    render();
    let navigating = false;
    try {
      // Finish any in-flight poll before choosing the state and CSRF token.
      if (statusRequest) await statusRequest;
      const allowed = kind === "start" ? ["unauthorized"] : ["authorized", "revocation_pending"];
      if (!statusHealthy || !status || !allowed.includes(status.state)) {
        throw new Error("Your connection status changed or is unavailable. Check the status below before trying again.");
      }
      const data = await requestJson(`/api/auth/${kind}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-CSRF-Token": status.csrf_token },
        body: JSON.stringify({}),
      });
      if (kind === "start") {
        if (typeof data?.authorization_url !== "string" || !data.authorization_url.trim()) {
          throw new Error("The server did not return a valid Daikin sign-in URL. Please try again.");
        }
        let authorizationUrl;
        try {
          authorizationUrl = new URL(data.authorization_url);
        } catch {
          throw new Error("The server did not return a valid Daikin sign-in URL. Please try again.");
        }
        if (!["https:", "http:"].includes(authorizationUrl.protocol) || authorizationUrl.username || authorizationUrl.password) {
          throw new Error("The server returned an unsafe Daikin sign-in URL. Authorization was not started.");
        }
        window.location.assign(authorizationUrl.href);
        navigating = true;
      } else {
        if (typeof data?.message !== "string" || !data.message.trim()) {
          throw new Error("The server returned an invalid revocation response. Check your connection status before retrying.");
        }
        showFeedback(data.message);
        await refreshStatus();
      }
    } catch (error) {
      showFeedback(errorMessage(error), "error");
      // Refresh state and CSRF after failures, including rejected or expired tokens.
      await refreshStatus();
    } finally {
      if (!navigating) {
        busy = null;
        render();
        schedulePoll();
      }
    }
  }

  ui.action.addEventListener("click", () => {
    if (ui.action.disabled || !status) return;
    if (status.state === "unauthorized") {
      void performAction("start");
    } else if (["authorized", "revocation_pending"].includes(status.state)) {
      const pending = status.state === "revocation_pending";
      ui.dialogTitle.textContent = pending ? "Retry revoking Daikin access?" : "Disconnect from Daikin?";
      ui.dialogDescription.textContent = pending
        ? "The previous revocation did not complete. Retry to revoke Daikin account access. Cloud access stays suspended until revocation is resolved."
        : "This will revoke your account authorization. daikin2mqtt will no longer be able to access your Daikin account. You can reconnect at any time.";
      ui.confirm.textContent = pending ? "Retry disconnect" : "Disconnect and revoke";
      ui.dialog.showModal();
    }
  });
  ui.cancel.addEventListener("click", () => ui.dialog.close());
  ui.confirm.addEventListener("click", () => {
    ui.dialog.close();
    void performAction("revoke");
  });
  ui.dismiss.addEventListener("click", () => { ui.feedback.hidden = true; });
  ui.retry.addEventListener("click", () => { void refreshStatus(); });

  // Returning from the provider via browser history must not retain a busy button.
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) {
      busy = null;
      consumeCallbackResult();
      void refreshStatus();
    }
  });

  consumeCallbackResult();
  void refreshStatus();
})();
