const API_BASE = "/api";
const REMOTE_MODE = window.location.hostname.endsWith("github.io");
const CONFIG_FILE = "live-config.json";
const TOKEN_KEY = "image_gallery_token";
const USER_KEY = "image_gallery_user";
const GALLERY_VIEW_KEY = "image_gallery_view";
const MAX_UPLOAD_BYTES = 500 * 1024 * 1024;
const GALLERY_PAGE_SIZE = 15;
const LIVE_CHECK_INTERVAL_MS = 15000;
const SEARCH_DEBOUNCE_MS = 250;
const DEFAULT_USER_SETTINGS = {
  theme_mode: "system",
  accent_color: "#37c9a7",
  grid_density: "comfortable",
  default_sort: "new",
  items_per_page: GALLERY_PAGE_SIZE,
  autoplay_previews: false,
  muted_previews: true,
  reduce_motion: false,
  open_original_in_new_tab: false,
  blur_video_previews: false,
  profile_show_uploads: true,
  profile_show_collections: true,
  profile_show_friends: true,
  profile_show_follow_counts: true,
};
const ACCOUNT_NAVIGATION = {
  signedOutUsernameTarget: "auth",
  signedInUsernameTarget: "profile",
  sidebarProfileTarget: "profile",
  settingsTarget: "settings",
};

let apiOrigin = "";
let token = readStore(TOKEN_KEY);
let currentUser = readJsonStore(USER_KEY);
let categories = [];
let mediaItems = [];
let collectionsState = [];
let galleryMode = "main";
let galleryPage = 1;
let galleryHasNext = false;
let galleryLoading = false;
let latestLiveSnapshot = null;
let activeDetail = null;
let selectedCollectionMediaId = null;
let selectedReportMediaId = null;
let registerMode = false;
let uploadInFlight = false;
let uploadStartedAt = 0;
let activeProfileUsername = "";
let galleryViewRestored = false;
let toastTimer = 0;
const revealedAdultMedia = new Set();

const $ = (id) => document.getElementById(id);

function safeEl(id) {
  return document.getElementById(id);
}

function on(id, eventName, handler) {
  const el = safeEl(id);
  if (el) el.addEventListener(eventName, handler);
  return el;
}

function setTextIfPresent(id, value) {
  const el = safeEl(id);
  if (el) el.textContent = value;
}

function showIfPresent(id, visible) {
  const el = safeEl(id);
  if (el) el.hidden = !visible;
}

function setDisabledIfPresent(id, disabled) {
  const el = safeEl(id);
  if (el) el.disabled = Boolean(disabled);
}

function showToast(message, kind = "info") {
  const region = safeEl("toast-region");
  if (!region) return;
  region.textContent = message || "";
  region.dataset.kind = kind;
  region.hidden = !message;
  clearTimeout(toastTimer);
  if (message) toastTimer = setTimeout(() => { region.hidden = true; }, 3200);
}


function readStore(key) {
  try { return localStorage.getItem(key) || ""; } catch (_err) { return ""; }
}

function writeStore(key, value) {
  try {
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
  } catch (_err) {}
}

function readJsonStore(key) {
  try { return JSON.parse(readStore(key) || "null"); } catch (_err) { return null; }
}

async function copyText(value, successMessage = "Copied.") {
  const text = String(value || "");
  if (!text) return false;
  try {
    await navigator.clipboard.writeText(text);
  } catch (_err) {
    const temp = document.createElement("input");
    temp.value = text;
    document.body.appendChild(temp);
    temp.select();
    document.execCommand("copy");
    temp.remove();
  }
  showToast(successMessage, "success");
  return true;
}

function apiUrl(path) {
  const normalized = path.startsWith("/") ? path : `${API_BASE}/${path}`;
  return `${apiOrigin}${normalized}`;
}

async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  let response;
  try {
    response = await fetch(apiUrl(path), { ...options, headers });
  } catch (err) {
    const error = new Error(`Backend unreachable: ${err.message || err}`);
    error.status = 0;
    throw error;
  }
  const text = await response.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch (_err) { data = { detail: text || "Invalid server response" }; }
  if (!response.ok) {
    const error = new Error(data.detail || data.message || "Request failed");
    error.status = response.status;
    throw error;
  }
  return data;
}

async function apiBlobFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  let response;
  try {
    response = await fetch(apiUrl(path), { ...options, headers });
  } catch (err) {
    const error = new Error(`Backend unreachable: ${err.message || err}`);
    error.status = 0;
    throw error;
  }
  if (!response.ok) {
    let detail = "Request failed";
    try {
      const data = await response.json();
      detail = data.detail || data.message || detail;
    } catch (_err) {}
    const error = new Error(detail);
    error.status = response.status;
    throw error;
  }
  return response;
}

function apiUpload(path, body, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", apiUrl(path));
    if (token) xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    xhr.upload.addEventListener("loadstart", () => {
      onProgress?.({ loaded: 0, total: 0, percent: 0, phase: "starting" });
    });
    xhr.upload.addEventListener("progress", (event) => {
      if (!event.lengthComputable) {
        onProgress?.({ loaded: event.loaded || 0, total: 0, percent: 0, phase: "uploading" });
        return;
      }
      onProgress?.({
        loaded: event.loaded,
        total: event.total,
        percent: Math.max(0, Math.min(100, Math.round((event.loaded / event.total) * 100))),
        phase: "uploading",
      });
    });
    xhr.upload.addEventListener("load", () => {
      onProgress?.({ loaded: 0, total: 0, percent: 100, phase: "processing" });
    });
    xhr.addEventListener("load", () => {
      let data = {};
      try { data = xhr.responseText ? JSON.parse(xhr.responseText) : {}; } catch (_err) { data = { detail: xhr.responseText || "Invalid server response" }; }
      if (xhr.status >= 200 && xhr.status < 300) resolve(data);
      else {
        const error = new Error(data.detail || data.message || "Upload failed");
        error.status = xhr.status;
        reject(error);
      }
    });
    xhr.addEventListener("error", () => {
      const error = new Error("Backend unreachable during upload.");
      error.status = 0;
      reject(error);
    });
    xhr.addEventListener("abort", () => {
      const error = new Error("Upload was cancelled.");
      error.status = 0;
      reject(error);
    });
    xhr.send(body);
  });
}

function filenameFromDisposition(disposition, fallback = "download") {
  const match = String(disposition || "").match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
  const raw = decodeURIComponent(match?.[1] || match?.[2] || fallback);
  return raw.replace(/[\\/\0]/g, "_").slice(0, 180) || fallback;
}

function setNotice(id, message, kind = "error") {
  const el = $(id);
  el.textContent = message || "";
  el.hidden = !message;
  el.classList.toggle("error", Boolean(message) && kind === "error");
  el.classList.toggle("success", Boolean(message) && kind === "success");
}

function formatBytes(bytes) {
  const value = Number(bytes || 0);
  if (value >= 1024 * 1024 * 1024) return `${(value / 1024 / 1024 / 1024).toFixed(1)} GB`;
  if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${value} B`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[char]));
}

function renderAuth() {
  $("auth-open").textContent = currentUser ? currentUser.display_name || currentUser.username : "Login";
  $("auth-open").title = currentUser ? "Open your profile" : "Login";
  $("logout").hidden = !currentUser;
  $("settings-open").hidden = !currentUser;
  $("studio-open").hidden = !currentUser;
  showIfPresent("profile-open", Boolean(currentUser));
  showIfPresent("friends-open", Boolean(currentUser));
  $("account-card").hidden = !currentUser;
  if (currentUser) {
    $("account-name").textContent = currentUser.display_name || currentUser.username;
    $("account-name").title = `Open @${currentUser.username}`;
    $("account-avatar").title = `Open @${currentUser.username}`;
    const emailNote = currentUser.email && !currentUser.email_verified
      ? " Email verification is pending; enter the emailed code in settings."
      : currentUser.email_verified
        ? " Email verified."
        : "";
    $("account-bio").textContent = currentUser.age_verified
      ? `${currentUser.bio || "Age verified. Your settings apply only to this account."}${emailNote}`
      : `${currentUser.bio || "Verify age in settings to unlock 18+ posts."}${emailNote}`;
    $("resend-email-verification").hidden = !currentUser.email || currentUser.email_verified;
    renderAvatar("account-avatar", currentUser);
  }
  applyAccountSettings();
  applyDesmondVisibility();
}

function isDesmondUser() {
  const username = String(currentUser?.username || "").trim().toLowerCase();
  const displayName = String(currentUser?.display_name || "").trim().toLowerCase();
  return username === "desmond" || displayName === "desmond";
}

function applyDesmondVisibility() {
  const canSeePrivateData = isDesmondUser();
  const status = safeEl("connection-status");
  const stats = document.querySelector(".stats-grid");
  if (status) status.hidden = false;
  if (stats) stats.hidden = !canSeePrivateData;
}

function openAgeDialog(message = "") {
  if (!currentUser) return $("auth-dialog").showModal();
  setNotice("age-error", message);
  $("age-dialog").showModal();
}

function canRevealAdult(item) {
  return !item?.is_adult || (currentUser?.age_verified && item?.url);
}

function adultBadge(item) {
  if (!item?.is_adult) return "";
  return `<span class="adult-badge">18+</span>`;
}

function renderPreview(item, size = "card") {
  const isAdult = Boolean(item.is_adult);
  const revealed = revealedAdultMedia.has(Number(item.id));
  const locked = isAdult && !canRevealAdult(item);
  const blur = isAdult && !revealed && !locked;
  const placeholderText = locked ? "18+ Verify Age" : "Click To Reveal";
  const body = locked
    ? `<div class="locked-preview"><strong>18+</strong><span>Verify age to view</span></div>`
    : item.media_kind === "video"
      ? `<video src="${item.url}" ${userSettings().muted_previews ? "muted" : ""} ${userSettings().autoplay_previews ? "autoplay loop" : ""} playsinline preload="metadata"></video>`
      : `<img src="${item.url}" alt="${escapeHtml(item.title)}" loading="${size === "card" ? "lazy" : "eager"}" />`;
  return `
    <span class="${blur ? "adult-blur" : ""}">${body}</span>
    ${isAdult && !revealed ? `<span class="adult-overlay">${placeholderText}</span>` : ""}
  `;
}

function userSettings() {
  return { ...DEFAULT_USER_SETTINGS, ...(currentUser?.user_settings || {}) };
}

function renderAvatar(id, user) {
  const el = $(id);
  const name = user?.display_name || user?.username || "IG";
  if (user?.avatar_url || user?.user_avatar_url) {
    el.innerHTML = `<img src="${user.avatar_url || user.user_avatar_url}" alt="">`;
  } else {
    el.textContent = name.slice(0, 2).toUpperCase();
  }
  el.style.borderColor = user?.profile_color || userSettings().accent_color || "#37c9a7";
}

function applyAccountSettings() {
  const prefs = userSettings();
  document.documentElement.style.setProperty("--accent", prefs.accent_color || "#37c9a7");
  document.body.dataset.theme = prefs.theme_mode || "system";
  document.body.dataset.density = prefs.grid_density || "comfortable";
  document.body.dataset.reduceMotion = prefs.reduce_motion ? "1" : "0";
  document.body.dataset.blurVideos = prefs.blur_video_previews ? "1" : "0";
  if (currentUser && $("sort-filter").value === "new") $("sort-filter").value = prefs.default_sort || "new";
}


function setRegisterMode(enabled) {
  registerMode = Boolean(enabled);
  setNotice("auth-error", "");
  $("auth-title").textContent = registerMode ? "Create Account" : "Login";
  $("auth-submit").textContent = registerMode ? "Create Account" : "Login";
  $("auth-toggle").textContent = registerMode ? "I already have an account" : "Create Account";
  showIfPresent("display-name-wrap", registerMode);
  showIfPresent("email-wrap", registerMode);
  const password = safeEl("auth-password");
  if (password) password.autocomplete = registerMode ? "new-password" : "current-password";
}

async function submitAuth(event) {
  event.preventDefault();
  setNotice("auth-error", "");
  const submit = safeEl("auth-submit");
  setDisabledIfPresent("auth-submit", true);
  try {
    const payload = {
      username: $("auth-username").value.trim(),
      password: $("auth-password").value,
    };
    if (registerMode) {
      payload.display_name = $("auth-display-name").value.trim() || payload.username;
      payload.email = $("auth-email").value.trim() || null;
    }
    const data = await apiFetch(registerMode ? "/api/auth/register" : "/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    token = data.token || "";
    currentUser = data.user || null;
    writeStore(TOKEN_KEY, token);
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    fillSettingsForm();
    $("auth-dialog").close();
    $("auth-form").reset();
    if (data.email_error) {
      setNotice("settings-error", `Account created, but email was not sent: ${data.email_error}`, "error");
      setTextIfPresent("account-bio", `Email verification could not be sent: ${data.email_error}`);
    }
    await refreshAll();
  } catch (err) {
    setNotice("auth-error", err.message);
  } finally {
    if (submit) submit.disabled = false;
  }
}

function fillSettingsForm() {
  if (!currentUser) return;
  const prefs = userSettings();
  const setValue = (id, value) => { const el = safeEl(id); if (el) el.value = value ?? ""; };
  const setChecked = (id, value) => { const el = safeEl(id); if (el) el.checked = Boolean(value); };
  setValue("settings-display-name", currentUser.display_name || currentUser.username || "");
  setValue("settings-email", currentUser.email || "");
  setValue("settings-profile-color", currentUser.profile_color || prefs.accent_color || "#37c9a7");
  setValue("settings-website", currentUser.website_url || "");
  setValue("settings-location", currentUser.location_label || "");
  setValue("settings-profile-headline", currentUser.profile_headline || "");
  setValue("settings-featured-tags", (currentUser.featured_tags || []).join(", "));
  setValue("settings-bio", currentUser.bio || "");
  setChecked("settings-public-profile", currentUser.public_profile !== false);
  setChecked("settings-show-liked-count", currentUser.show_liked_count !== false);
  setChecked("settings-show-collections", currentUser.show_collections !== false);
  setChecked("settings-show-uploads", currentUser.show_recent_uploads !== false);
  setChecked("settings-show-friends", currentUser.show_friends !== false);
  setValue("pref-theme-mode", prefs.theme_mode || "system");
  setValue("pref-accent-color", prefs.accent_color || "#37c9a7");
  setValue("pref-grid-density", prefs.grid_density || "comfortable");
  setValue("pref-default-sort", prefs.default_sort || "new");
  setValue("pref-items-per-page", GALLERY_PAGE_SIZE);
  setChecked("pref-autoplay-previews", prefs.autoplay_previews);
  setChecked("pref-muted-previews", prefs.muted_previews !== false);
  setChecked("pref-reduce-motion", prefs.reduce_motion);
  setChecked("pref-open-original", prefs.open_original_in_new_tab);
  setChecked("pref-blur-video-previews", prefs.blur_video_previews);
  setTextIfPresent("settings-email-status", currentUser.email ? (currentUser.email_verified ? "Email verified" : "Email verification pending") : "No email set");
  setTextIfPresent("settings-age-status", currentUser.age_verified ? "Verified" : "Not verified");
  renderAvatar("settings-avatar-preview", currentUser);
}

async function submitSettings(event) {
  event.preventDefault();
  if (!currentUser) return $("auth-dialog").showModal();
  setNotice("settings-error", "");
  try {
    const profilePayload = {
      display_name: $("settings-display-name").value.trim() || currentUser.username,
      bio: $("settings-bio").value.trim() || null,
      website_url: $("settings-website").value.trim() || null,
      location_label: $("settings-location").value.trim() || null,
      profile_headline: $("settings-profile-headline").value.trim() || null,
      featured_tags: $("settings-featured-tags").value.split(",").map((tag) => tag.trim()).filter(Boolean),
      profile_color: $("settings-profile-color").value || "#37c9a7",
      public_profile: $("settings-public-profile").checked,
      show_liked_count: $("settings-show-liked-count").checked,
      show_collections: $("settings-show-collections").checked,
      show_recent_uploads: $("settings-show-uploads").checked,
      show_friends: $("settings-show-friends").checked,
    };
    let data = await apiFetch("/api/me/profile", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(profilePayload),
    });
    currentUser = data.user;
    const settingsPayload = {
      theme_mode: $("pref-theme-mode").value,
      accent_color: $("pref-accent-color").value,
      grid_density: $("pref-grid-density").value,
      default_sort: $("pref-default-sort").value,
      items_per_page: GALLERY_PAGE_SIZE,
      autoplay_previews: $("pref-autoplay-previews").checked,
      muted_previews: $("pref-muted-previews").checked,
      reduce_motion: $("pref-reduce-motion").checked,
      open_original_in_new_tab: $("pref-open-original").checked,
      blur_video_previews: $("pref-blur-video-previews").checked,
      profile_show_uploads: $("settings-show-uploads").checked,
      profile_show_collections: $("settings-show-collections").checked,
      profile_show_friends: $("settings-show-friends").checked,
      profile_show_follow_counts: $("settings-show-liked-count").checked,
    };
    data = await apiFetch("/api/me/settings", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(settingsPayload),
    });
    currentUser = data.user;
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    fillSettingsForm();
    setNotice("settings-error", "Saved.", "success");
    await refreshAll();
  } catch (err) {
    setNotice("settings-error", err.message);
  }
}

async function saveEmailAndSendCode() {
  if (!currentUser) return;
  setNotice("settings-error", "");
  try {
    const data = await apiFetch("/api/me/email", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: $("settings-email").value.trim() || null }),
    });
    currentUser = data.user;
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    fillSettingsForm();
    setTextIfPresent("settings-email-status", data.email_verification_sent ? "Verification code sent." : "Email saved.");
    setNotice("settings-error", data.email_verification_sent ? "Verification code sent." : "Email saved.", "success");
  } catch (err) {
    setNotice("settings-error", err.message);
  }
}

async function verifyEmailCode() {
  if (!currentUser) return;
  const code = $("settings-email-code").value.trim();
  if (!code) return setNotice("settings-error", "Enter the email verification code.");
  try {
    const data = await apiFetch("/api/me/email/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code }),
    });
    currentUser = data.user;
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    fillSettingsForm();
    setNotice("settings-error", "Email verified.", "success");
  } catch (err) {
    setNotice("settings-error", err.message);
  }
}

async function submitAgeVerification(event) {
  if (event) event.preventDefault();
  if (!currentUser) return $("auth-dialog").showModal();
  const fromSettings = event?.currentTarget?.id === "settings-age-save" || event?.currentTarget?.id === "settings-form";
  const birthdate = (fromSettings ? safeEl("settings-birthdate") : safeEl("age-birthdate"))?.value || safeEl("settings-birthdate")?.value || safeEl("age-birthdate")?.value;
  const confirm_over_18 = Boolean((fromSettings ? safeEl("settings-age-confirm") : safeEl("age-confirm"))?.checked || safeEl("settings-age-confirm")?.checked || safeEl("age-confirm")?.checked);
  const noticeId = fromSettings ? "settings-error" : "age-error";
  try {
    const data = await apiFetch("/api/me/age-verification", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ birthdate, confirm_over_18 }),
    });
    currentUser = data.user;
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    fillSettingsForm();
    revealedAdultMedia.clear();
    if (safeEl("age-dialog")?.open) $("age-dialog").close();
    setNotice(noticeId, "Age verified.", "success");
    await refreshAll();
  } catch (err) {
    setNotice(noticeId, err.message);
  }
}

function toggleNewCategory() {
  const creating = !safeEl("upload-category")?.value;
  showIfPresent("new-category-wrap", creating);
  showIfPresent("new-category-kind-wrap", creating);
  const name = safeEl("new-category-name");
  if (name) name.required = creating;
}

function handleAdultOpen(id) {
  const numericId = Number(id);
  const item = mediaItems.find((entry) => Number(entry.id) === numericId)
    || activeDetail
    || collectionsState.flatMap((collection) => collection.items || []).find((entry) => Number(entry.id) === numericId);
  if (!item?.is_adult) return false;
  if (!currentUser || !currentUser.age_verified || !item.url) {
    openAgeDialog("Verify your age to view this 18+ post.");
    return true;
  }
  if (!revealedAdultMedia.has(numericId)) {
    revealedAdultMedia.add(numericId);
    renderMediaGrid();
    return true;
  }
  return false;
}

async function copyAddress(id) {
  const numericId = Number(id);
  const item = mediaItems.find((entry) => Number(entry.id) === numericId) || activeDetail;
  const url = item?.url || (item?.id ? apiUrl(`/api/media/${item.id}/file`) : "");
  if (!url) return;
  await copyText(url, "Media address copied.");
}

async function downloadMedia(id) {
  const numericId = Number(id);
  const item = mediaItems.find((entry) => Number(entry.id) === numericId) || activeDetail;
  if (!item) return;
  if (item.downloads_enabled === false) return alert("Downloads are disabled for this post.");
  if (item.is_adult && (!currentUser || !currentUser.age_verified)) {
    openAgeDialog("Verify your age before downloading this 18+ post.");
    return;
  }
  try {
    const response = await apiBlobFetch(`/api/media/${numericId}/download`);
    const blob = await response.blob();
    const fallback = item.original_filename || `${(item.title || "download").replace(/\s+/g, "_")}`;
    const filename = filenameFromDisposition(response.headers.get("Content-Disposition"), fallback);
    const objectUrl = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = objectUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    showToast("Download started.", "success");
    setTimeout(() => URL.revokeObjectURL(objectUrl), 1500);
  } catch (err) {
    if (err.status === 403 && item.is_adult) openAgeDialog(err.message);
    else showToast(err.message || "Download failed.", "error");
  }
}

function stopMediaPlayback(root = document) {
  root.querySelectorAll("video, audio").forEach((media) => {
    try {
      media.pause();
      media.currentTime = 0;
      media.removeAttribute("src");
      media.load();
    } catch (_err) {}
  });
}

function closeDetailDialog() {
  const dialog = $("detail-dialog");
  stopMediaPlayback(dialog);
  if (dialog.open) dialog.close();
}

async function refreshMe() {
  if (!token) return;
  let data;
  try {
    data = await apiFetch("/api/me");
  } catch (err) {
    if (err.status === 401) {
      token = "";
      currentUser = null;
      writeStore(TOKEN_KEY, "");
      writeStore(USER_KEY, "");
      renderAuth();
      return;
    }
    throw err;
  }
  currentUser = data.user;
  writeStore(USER_KEY, JSON.stringify(currentUser));
  renderAuth();
  fillSettingsForm();
}

async function initApiOrigin() {
  const status = safeEl("connection-status");
  if (!REMOTE_MODE) {
    apiOrigin = "";
    if (status) status.textContent = "Local";
    applyDesmondVisibility();
    return true;
  }
  try {
    const response = await fetch(`${CONFIG_FILE}?t=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`Config HTTP ${response.status}`);
    const config = await response.json();
    apiOrigin = String(config.gallery_url || "").replace(/\/$/, "");
    if (!apiOrigin) throw new Error("live-config.json has no gallery_url");
    if (status) status.textContent = "Live";
    applyDesmondVisibility();
    return true;
  } catch (err) {
    apiOrigin = "";
    if (status) {
      status.textContent = "No backend";
      status.dataset.state = "error";
      status.title = err.message || String(err);
    }
    setTextIfPresent("result-count", `Backend config failed: ${err.message || err}`);
    showIfPresent("empty-state", true);
    applyDesmondVisibility();
    return false;
  }
}

async function refreshAll() {
  await runLiveChecks({ silent: true }).catch(() => {});
  await Promise.all([loadCategories(), isDesmondUser() ? loadStats() : Promise.resolve(), loadTags()]);
  if (!galleryViewRestored) restoreGalleryViewState();
  await loadMedia(galleryMode === "main" ? galleryPage : 1);
}

async function loadTags() {
  const data = await apiFetch("/api/tags");
  const tags = data.tags || [];
  const showCounts = isDesmondUser();
  $("tag-cloud").innerHTML = tags.length ? tags.map((item) => (
    `<button type="button" data-tag="${escapeHtml(item.tag)}">${escapeHtml(item.tag)}${showCounts ? ` <span>${item.count}</span>` : ""}</button>`
  )).join("") : `<span class="muted">${latestLiveSnapshot?.media_active ? "No public tags on loaded posts" : "Checking tags"}</span>`;
}

async function loadCategories() {
  const data = await apiFetch("/api/categories");
  categories = data.categories || [];
  const filter = $("category-filter");
  const upload = $("upload-category");
  const edit = safeEl("edit-media-category");
  const selectedFilter = filter.value;
  const selectedUpload = upload.value;
  const selectedEdit = edit?.value || "";
  filter.innerHTML = `<option value="">All categories</option>`;
  upload.innerHTML = `<option value="">Create new category</option>`;
  if (edit) edit.innerHTML = "";
  for (const category of categories) {
    const count = isDesmondUser() ? ` (${category.media_count || 0})` : "";
    filter.insertAdjacentHTML("beforeend", `<option value="${category.id}">${escapeHtml(category.name)}${count}</option>`);
    upload.insertAdjacentHTML("beforeend", `<option value="${category.id}">${escapeHtml(category.name)}</option>`);
    if (edit) edit.insertAdjacentHTML("beforeend", `<option value="${category.id}">${escapeHtml(category.name)}</option>`);
  }
  filter.value = selectedFilter;
  upload.value = selectedUpload || (categories[0]?.id ?? "");
  if (edit) edit.value = selectedEdit || (categories[0]?.id ?? "");
  toggleNewCategory();
}

async function loadStats() {
  const data = await apiFetch("/api/stats");
  const stats = data.stats || {};
  $("stat-media").textContent = stats.media || 0;
  $("stat-likes").textContent = stats.likes || 0;
  $("stat-users").textContent = stats.users || 0;
  $("stat-bytes").textContent = formatBytes(stats.bytes || 0);
}

function galleryFiltersActive() {
  return Boolean(
    safeEl("search")?.value?.trim()
    || safeEl("kind-filter")?.value
    || safeEl("category-filter")?.value
  );
}

function galleryViewState() {
  return {
    search: safeEl("search")?.value?.trim() || "",
    kind: safeEl("kind-filter")?.value || "",
    category: safeEl("category-filter")?.value || "",
    sort: safeEl("sort-filter")?.value || "new",
    page: galleryPage,
  };
}

function saveGalleryViewState() {
  writeStore(GALLERY_VIEW_KEY, JSON.stringify(galleryViewState()));
}

function restoreGalleryViewState() {
  galleryViewRestored = true;
  const params = new URLSearchParams(location.search || "");
  const stored = readJsonStore(GALLERY_VIEW_KEY) || {};
  const state = {
    ...stored,
    search: params.get("q") ?? stored.search,
    kind: params.get("kind") ?? stored.kind,
    category: params.get("category") ?? stored.category,
    sort: params.get("sort") ?? stored.sort,
    page: params.get("page") ?? stored.page,
  };
  if (safeEl("search")) $("search").value = state.search || "";
  if (safeEl("kind-filter")) $("kind-filter").value = state.kind || "";
  if (safeEl("category-filter")) $("category-filter").value = state.category || "";
  if (safeEl("sort-filter")) $("sort-filter").value = state.sort || userSettings().default_sort || "new";
  galleryPage = Math.max(1, Number(state.page || 1));
}

function clearGalleryFilters() {
  if (safeEl("search")) $("search").value = "";
  if (safeEl("kind-filter")) $("kind-filter").value = "";
  if (safeEl("category-filter")) $("category-filter").value = "";
  if (safeEl("sort-filter")) $("sort-filter").value = "new";
  galleryPage = 1;
  saveGalleryViewState();
  loadMedia(1, { scrollToTop: true });
}

function renderActiveFilters() {
  const wrap = safeEl("active-filters");
  if (!wrap) return;
  const state = galleryViewState();
  const category = categories.find((item) => String(item.id) === String(state.category));
  const chips = [];
  if (state.search) chips.push(["search", `Search: ${state.search}`]);
  if (state.kind) chips.push(["kind", `Type: ${state.kind === "image" ? "Images/GIFs" : "Videos"}`]);
  if (state.category) chips.push(["category", `Category: ${category?.name || state.category}`]);
  if (state.sort && state.sort !== "new") chips.push(["sort", `Sort: ${$("sort-filter").selectedOptions?.[0]?.textContent || state.sort}`]);
  wrap.hidden = chips.length === 0;
  wrap.innerHTML = chips.map(([key, label]) => `<button type="button" data-clear-filter="${key}">${escapeHtml(label)} <span aria-hidden="true">x</span></button>`).join("")
    + (chips.length ? `<button type="button" data-clear-filter="all" class="clear-all">Clear All</button>` : "");
}

async function shareCurrentView() {
  const params = new URLSearchParams();
  const state = galleryViewState();
  if (state.search) params.set("q", state.search);
  if (state.kind) params.set("kind", state.kind);
  if (state.category) params.set("category", state.category);
  if (state.sort && state.sort !== "new") params.set("sort", state.sort);
  if (state.page > 1) params.set("page", String(state.page));
  const url = `${location.origin}${location.pathname}${params.toString() ? `?${params}` : ""}`;
  await copyText(url, "Current gallery view copied.");
}

function setEmptyState(title, message, visible = true) {
  setTextIfPresent("empty-title", title);
  setTextIfPresent("empty-message", message);
  showIfPresent("empty-state", visible);
}

function updateGalleryPagination() {
  const pagination = safeEl("gallery-pagination");
  if (!pagination) return;
  const hasAnyPaging = galleryPage > 1 || galleryHasNext;
  pagination.hidden = galleryLoading || !hasAnyPaging;
  setTextIfPresent("gallery-page-status", `Page ${galleryPage}`);
  setDisabledIfPresent("gallery-prev", galleryPage <= 1);
  setDisabledIfPresent("gallery-next", !galleryHasNext);
}

async function loadMedia(page = galleryPage, { scrollToTop = false } = {}) {
  galleryMode = "main";
  galleryPage = Math.max(1, Number(page) || 1);
  saveGalleryViewState();
  renderActiveFilters();
  galleryLoading = true;
  setTextIfPresent("result-count", `Loading page ${galleryPage}`);
  showIfPresent("empty-state", false);
  updateGalleryPagination();
  const params = new URLSearchParams();
  if ($("kind-filter").value) params.set("media_kind", $("kind-filter").value);
  if ($("category-filter").value) params.set("category_id", $("category-filter").value);
  if ($("search").value.trim()) params.set("q", $("search").value.trim());
  params.set("sort", $("sort-filter").value);
  params.set("limit", GALLERY_PAGE_SIZE + 1);
  params.set("offset", (galleryPage - 1) * GALLERY_PAGE_SIZE);
  try {
    const data = await apiFetch(`/api/media?${params}`);
    const rows = data.media || [];
    galleryHasNext = rows.length > GALLERY_PAGE_SIZE;
    mediaItems = rows.slice(0, GALLERY_PAGE_SIZE);
    renderMediaGrid();
    if (scrollToTop) safeEl("gallery-grid")?.scrollIntoView({ behavior: userSettings().reduce_motion ? "auto" : "smooth", block: "start" });
  } catch (err) {
    mediaItems = [];
    galleryHasNext = false;
    $("gallery-grid").innerHTML = "";
    setTextIfPresent("result-count", err.message || "Gallery failed to load");
    setEmptyState("Gallery check needed", "The gallery could not load posts. Use Checks or Refresh to test the live backend.");
  } finally {
    galleryLoading = false;
    updateGalleryPagination();
  }
}

function focusGalleryOnNewestUploads() {
  const search = safeEl("search");
  const kind = safeEl("kind-filter");
  const category = safeEl("category-filter");
  const sort = safeEl("sort-filter");
  if (search) search.value = "";
  if (kind) kind.value = "";
  if (category) category.value = "";
  if (sort) sort.value = "new";
  galleryPage = 1;
  saveGalleryViewState();
  renderActiveFilters();
}

function renderMediaGrid() {
  const grid = $("gallery-grid");
  grid.innerHTML = "";
  const start = mediaItems.length ? ((galleryPage - 1) * GALLERY_PAGE_SIZE) + 1 : 0;
  const end = start + mediaItems.length - 1;
  $("result-count").textContent = mediaItems.length
    ? `Page ${galleryPage} · showing ${start}-${end}${galleryHasNext ? "+" : ""}`
    : galleryFiltersActive()
      ? `${galleryMode === "liked" ? "No saved likes" : galleryMode === "following" ? "No following posts" : "No posts match this view"}`
      : latestLiveSnapshot?.media_active
        ? `${Number(latestLiveSnapshot.media_active)} posts live · refresh if they do not appear`
        : "Checking for posts";
  if (!mediaItems.length) {
    if (galleryMode === "following") {
      setEmptyState("Nothing from follows yet", "Follow uploaders to build a paged feed here.");
    } else if (galleryMode === "liked") {
      setEmptyState("No liked posts yet", "Like posts to keep them in this paged view.");
    } else if (galleryFiltersActive()) {
      setEmptyState("No matches", "Try clearing search, category, or media-type filters.");
    } else if (latestLiveSnapshot?.media_active) {
      setEmptyState("Posts are live", "The backend reports posts are available. Refresh or run Checks if this page stays empty.", false);
    } else {
      setEmptyState("Gallery loading", "Checking the live backend before showing an empty gallery.", false);
    }
  } else {
    showIfPresent("empty-state", false);
  }
  renderActiveFilters();
  updateGalleryPagination();
  for (const item of mediaItems) {
    const card = document.createElement("article");
    card.className = `media-card${item.is_adult ? " adult-card" : ""}`;
    const prefs = userSettings();
    card.innerHTML = `
      <button class="media-preview" type="button" data-open="${item.id}">${renderPreview(item)}</button>
      <div class="media-info">
        <div class="author-row">
          <button type="button" class="avatar tiny" style="border-color:${escapeHtml(item.profile_color || "#37c9a7")}" data-profile="${escapeHtml(item.username || "")}">${item.user_avatar_url ? `<img src="${item.user_avatar_url}" alt="">` : escapeHtml((item.display_name || item.username || "IG").slice(0, 2).toUpperCase())}</button>
          <div>
          <h2>${adultBadge(item)}${escapeHtml(item.title)}</h2>
          <p class="muted">${escapeHtml(item.category_name)} by <button type="button" class="text-button" data-profile="${escapeHtml(item.username || "")}">${escapeHtml(item.display_name || item.username)}</button>${item.visibility && item.visibility !== "public" ? ` · ${escapeHtml(item.visibility)}` : ""}${item.pinned_at ? " · pinned" : ""}</p>
          </div>
        </div>
        <div class="metric-row">
          <span>${item.like_count || 0} likes</span>
          <span>${item.downloads || 0} downloads</span>
          <span>${formatBytes(item.file_size)}</span>
        </div>
        <div class="card-actions">
          <button type="button" data-like="${item.id}">${item.liked_by_me ? "Unlike" : "Like"}</button>
          <button type="button" data-bookmark="${item.id}">${item.bookmarked_by_me ? "Saved" : "Save"}</button>
          <button type="button" data-collect="${item.id}">Collect</button>
          <button type="button" data-copy="${item.id}" ${item.url ? "" : "disabled"}>Copy Address</button>
          ${currentUser && Number(item.user_id) === Number(currentUser.id) ? `<button type="button" data-edit-media="${item.id}">Manage</button><button type="button" data-delete-media="${item.id}" class="danger-button">Delete</button>` : ""}
          ${item.downloads_enabled !== false ? `<button type="button" data-download="${item.id}" class="button-link">Download</button>` : `<button type="button" disabled>No downloads</button>`}
        </div>
      </div>
    `;
    grid.appendChild(card);
  }
}

async function loadCurrentGalleryPage(page = galleryPage, options = {}) {
  if (galleryMode === "following") return loadFollowingFeed(page, options);
  if (galleryMode === "liked") return loadLikedFeed(page, options);
  return loadMedia(page, options);
}

async function openDetail(id) {
  let data;
  try {
    data = await apiFetch(`/api/media/${id}`);
  } catch (err) {
    if (err.status === 403) {
      openAgeDialog(err.message);
      return;
    }
    throw err;
  }
  stopMediaPlayback($("detail-dialog"));
  activeDetail = data.media;
  const item = activeDetail;
  $("detail-title").innerHTML = `${adultBadge(item)}${escapeHtml(item.title)}`;
  $("detail-media").innerHTML = item.media_kind === "video"
    ? `<video src="${item.url}" controls autoplay playsinline></video>`
    : `<img src="${item.url}" alt="${escapeHtml(item.title)}" />`;
  $("detail-meta").textContent = `${item.category_name} by ${item.display_name || item.username} - ${formatBytes(item.file_size)} - ${item.like_count || 0} likes`;
  $("detail-description").innerHTML = `
    ${item.user_avatar_url ? `<div class="profile-mini"><button type="button" class="avatar" data-profile="${escapeHtml(item.username || "")}"><img src="${item.user_avatar_url}" alt=""></button><div><button type="button" class="text-button strong" data-profile="${escapeHtml(item.username || "")}">${escapeHtml(item.display_name || item.username)}</button>${item.user_bio ? `<p>${escapeHtml(item.user_bio)}</p>` : ""}${item.user_website_url ? `<a href="${item.user_website_url}" target="_blank" rel="noopener">Website</a>` : ""}</div></div>` : ""}
    <p>${escapeHtml(item.description || "")}</p>
  `;
  $("detail-tags").innerHTML = (item.tags || []).map((tag) => `<span>${escapeHtml(tag)}</span>`).join("");
  $("detail-like").textContent = item.liked_by_me ? "Unlike" : "Like";
  $("detail-bookmark").textContent = item.bookmarked_by_me ? "Saved" : "Bookmark";
  $("detail-download").href = "#";
  $("detail-download").dataset.download = item.id;
  $("detail-download").toggleAttribute("aria-disabled", item.downloads_enabled === false);
  updateDetailNavButtons();
  renderComments(data.comments || []);
  const commentForm = $("comment-form");
  if (commentForm) commentForm.hidden = item.comments_enabled === false && Number(item.user_id) !== Number(currentUser?.id);
  if (!$("detail-dialog").open) $("detail-dialog").showModal();
}

function currentDetailIndex() {
  if (!activeDetail) return -1;
  return mediaItems.findIndex((entry) => Number(entry.id) === Number(activeDetail.id));
}

function updateDetailNavButtons() {
  const index = currentDetailIndex();
  setDisabledIfPresent("detail-prev", index <= 0);
  setDisabledIfPresent("detail-next", index < 0 || index >= mediaItems.length - 1);
}

async function openAdjacentDetail(direction) {
  const index = currentDetailIndex();
  const next = mediaItems[index + direction];
  if (!next) return;
  if (!handleAdultOpen(next.id)) await openDetail(next.id);
}

function renderComments(comments) {
  $("comments-list").innerHTML = comments.map((comment) => {
    const canDelete = currentUser && (Number(comment.user_id) === Number(currentUser.id) || Number(activeDetail?.user_id) === Number(currentUser.id));
    const avatarUrl = comment.user_avatar_url || (comment.user_avatar_path ? `${apiOrigin}/api/users/${comment.user_id}/avatar` : "");
    return `
      <div class="comment">
        <div class="comment-head">
          <div class="avatar tiny">${avatarUrl ? `<img src="${avatarUrl}" alt="">` : escapeHtml((comment.display_name || comment.username || "IG").slice(0, 2).toUpperCase())}</div>
          <strong>${escapeHtml(comment.display_name || comment.username)}</strong>
          ${canDelete ? `<button type="button" class="comment-delete" data-delete-comment="${comment.id}">Delete</button>` : ""}
        </div>
        <p>${escapeHtml(comment.body)}</p>
      </div>
    `;
  }).join("");
}

async function deleteComment(id) {
  if (!confirm("Delete this comment?")) return;
  await apiFetch(`/api/comments/${id}`, { method: "DELETE" });
  if (activeDetail) await openDetail(activeDetail.id);
}

async function toggleBookmark(id, bookmarked = null) {
  if (!currentUser) return $("auth-dialog").showModal();
  const item = mediaItems.find((entry) => Number(entry.id) === Number(id)) || activeDetail;
  const nextBookmarked = bookmarked ?? !item?.bookmarked_by_me;
  const data = await apiFetch(`/api/media/${id}/bookmark`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ bookmarked: nextBookmarked }),
  });
  const updated = data.media;
  mediaItems = mediaItems.map((entry) => Number(entry.id) === Number(id) ? updated : entry);
  if (activeDetail && Number(activeDetail.id) === Number(id)) activeDetail = updated;
  renderMediaGrid();
  if (activeDetail && Number(activeDetail.id) === Number(id)) {
    $("detail-bookmark").textContent = updated.bookmarked_by_me ? "Saved" : "Bookmark";
  }
  showToast(updated.bookmarked_by_me ? "Saved to bookmarks." : "Removed from bookmarks.", "success");
}

async function toggleLike(id, liked = null) {
  if (!currentUser) return $("auth-dialog").showModal();
  const item = mediaItems.find((entry) => Number(entry.id) === Number(id)) || activeDetail;
  const nextLiked = liked ?? !item?.liked_by_me;
  const data = await apiFetch(`/api/media/${id}/like`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ liked: nextLiked }),
  });
  const updated = data.media;
  mediaItems = mediaItems.map((entry) => Number(entry.id) === Number(id) ? updated : entry);
  if (activeDetail && Number(activeDetail.id) === Number(id)) activeDetail = updated;
  renderMediaGrid();
  if (activeDetail && Number(activeDetail.id) === Number(id)) {
    $("detail-like").textContent = updated.liked_by_me ? "Unlike" : "Like";
    $("detail-meta").textContent = `${updated.category_name} by ${updated.display_name || updated.username} - ${formatBytes(updated.file_size)} - ${updated.like_count || 0} likes`;
  }
  showToast(updated.liked_by_me ? "Liked." : "Like removed.", "success");
  if (isDesmondUser()) await loadStats().catch(() => {});
}

async function openSurprise() {
  const data = await apiFetch("/api/media/random");
  if (data.media?.id) await openDetail(data.media.id);
}

async function loadCollections(mine = false) {
  const data = await apiFetch(`/api/collections${mine ? "?mine=true" : ""}`);
  collectionsState = data.collections || [];
  renderCollections();
  return collectionsState;
}

function renderCollections() {
  const list = $("collections-list");
  if (!collectionsState.length) {
    list.innerHTML = `<p class="muted">No collections yet.</p>`;
    return;
  }
  list.innerHTML = collectionsState.map((collection) => `
    <article class="collection-card">
      <button type="button" data-collection-open="${collection.id}" class="collection-cover">
        ${collection.cover_url ? `<img src="${collection.cover_url}" alt="">` : `<span>${collection.cover_locked ? "18+" : escapeHtml(collection.name.slice(0, 2).toUpperCase())}</span>`}
      </button>
      <div>
        <h3>${escapeHtml(collection.name)}</h3>
        <p class="muted">${escapeHtml(collection.description || "No description")} · ${collection.item_count || 0} posts · ${collection.is_public ? "Public" : "Private"}</p>
        <p class="muted">by ${escapeHtml(collection.display_name || collection.username || "Unknown")}</p>
      </div>
    </article>
  `).join("");
}

async function openCollectionsDialog() {
  setNotice("collections-error", "");
  $("collection-media").innerHTML = "";
  $("collections-dialog").showModal();
  await loadCollections(false);
}

async function createCollection(event) {
  event.preventDefault();
  if (!currentUser) return $("auth-dialog").showModal();
  setNotice("collections-error", "");
  try {
    await apiFetch("/api/collections", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: $("collection-name").value,
        description: $("collection-description").value,
        is_public: $("collection-public").checked,
      }),
    });
    $("collection-form").reset();
    $("collection-public").checked = true;
    await loadCollections(false);
  } catch (err) {
    setNotice("collections-error", err.message);
  }
}

async function openCollection(id) {
  const data = await apiFetch(`/api/collections/${id}`);
  const media = data.media || [];
  $("collection-media").innerHTML = `
    <div class="section-title-row"><h3>${escapeHtml(data.collection.name)}</h3><span class="muted">${media.length} posts</span></div>
    ${media.length ? media.map((item) => `
      <button class="mini-media" type="button" data-open="${item.id}">
        ${renderPreview(item, "mini")}
        <span>${adultBadge(item)}${escapeHtml(item.title)}</span>
      </button>
    `).join("") : `<p class="muted">This collection is empty.</p>`}
  `;
}

async function openCollectionPicker(mediaId) {
  if (!currentUser) return $("auth-dialog").showModal();
  selectedCollectionMediaId = mediaId;
  setNotice("collection-picker-error", "");
  const collections = await loadCollections(true);
  $("collection-picker-select").innerHTML = collections.map((collection) => (
    `<option value="${collection.id}">${escapeHtml(collection.name)}</option>`
  )).join("");
  if (!collections.length) {
    setNotice("collection-picker-error", "Create a collection first.");
  }
  $("collection-picker-dialog").showModal();
}

async function addToCollection(event) {
  event.preventDefault();
  if (!selectedCollectionMediaId) return;
  const collectionId = $("collection-picker-select").value;
  if (!collectionId) return setNotice("collection-picker-error", "Choose a collection first.");
  try {
    await apiFetch(`/api/collections/${collectionId}/items`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ media_id: Number(selectedCollectionMediaId), saved: true }),
    });
    $("collection-picker-dialog").close();
    showToast("Added to collection.", "success");
  } catch (err) {
    setNotice("collection-picker-error", err.message);
  }
}

async function openStudio() {
  if (!currentUser) return $("auth-dialog").showModal();
  const data = await apiFetch("/api/me/media");
  const items = data.media || [];
  const totals = items.reduce((acc, item) => {
    acc.views += Number(item.views || 0);
    acc.downloads += Number(item.downloads || 0);
    acc.likes += Number(item.like_count || 0);
    return acc;
  }, { views: 0, downloads: 0, likes: 0 });
  $("studio-posts").textContent = items.length;
  $("studio-views").textContent = totals.views;
  $("studio-downloads").textContent = totals.downloads;
  $("studio-likes").textContent = totals.likes;
  $("studio-list").innerHTML = items.length ? items.map((item) => `
    <article class="studio-item">
      <button type="button" data-open="${item.id}" class="studio-thumb">
        ${renderPreview(item, "mini")}
      </button>
      <div>
        <h3>${adultBadge(item)}${escapeHtml(item.title)}</h3>
        <p class="muted">${item.views || 0} views · ${item.downloads || 0} downloads · ${item.like_count || 0} likes</p>
      </div>
      <button type="button" data-edit-media="${item.id}">Edit</button>
      <button type="button" data-delete-media="${item.id}">Delete</button>
    </article>
  `).join("") : `<p class="muted">You have not uploaded anything yet.</p>`;
  if (!$("studio-dialog").open) $("studio-dialog").showModal();
}

async function editOwnMedia(id) {
  const item = (await apiFetch(`/api/media/${id}`)).media;
  setNotice("edit-media-error", "");
  $("edit-media-id").value = item.id;
  $("edit-media-title").value = item.title || "";
  $("edit-media-description").value = item.description || "";
  $("edit-media-tags").value = (item.tags || []).join(", ");
  $("edit-media-category").value = item.category_id || categories[0]?.id || "";
  $("edit-media-visibility").value = item.visibility || "public";
  $("edit-media-comments-enabled").checked = item.comments_enabled !== false;
  $("edit-media-downloads-enabled").checked = item.downloads_enabled !== false;
  $("edit-media-pinned").checked = Boolean(item.pinned_at);
  $("edit-media-adult").checked = Boolean(item.is_adult);
  $("edit-media-dialog").showModal();
}

async function submitEditMedia(event) {
  event.preventDefault();
  const id = $("edit-media-id").value;
  const title = $("edit-media-title").value.trim();
  const categoryId = Number($("edit-media-category").value || categories[0]?.id || 0);
  if (!title) return setNotice("edit-media-error", "Title is required.");
  if (!categoryId) return setNotice("edit-media-error", "Choose a category.");
  try {
    const data = await apiFetch(`/api/media/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title,
        description: $("edit-media-description").value.trim() || null,
        category_id: categoryId,
        tags: $("edit-media-tags").value.split(",").map((tag) => tag.trim()).filter(Boolean),
        is_adult: $("edit-media-adult").checked,
        visibility: $("edit-media-visibility").value,
        comments_enabled: $("edit-media-comments-enabled").checked,
        downloads_enabled: $("edit-media-downloads-enabled").checked,
        pinned: $("edit-media-pinned").checked,
      }),
    });
    mediaItems = mediaItems.map((entry) => Number(entry.id) === Number(id) ? data.media : entry);
    $("edit-media-dialog").close();
    await openStudio();
    await refreshAll();
  } catch (err) {
    setNotice("edit-media-error", err.message);
  }
}

async function restoreOwnMedia(id) {
  await apiFetch(`/api/media/${id}/restore`, { method: "POST" });
  await openStudio();
  await refreshAll();
}

async function loadPagedFeed(path, mode, page = 1, { scrollToTop = false } = {}) {
  galleryMode = mode;
  galleryPage = Math.max(1, Number(page) || 1);
  galleryLoading = true;
  setTextIfPresent("result-count", `Loading page ${galleryPage}`);
  showIfPresent("empty-state", false);
  updateGalleryPagination();
  const params = new URLSearchParams({
    limit: String(GALLERY_PAGE_SIZE + 1),
    offset: String((galleryPage - 1) * GALLERY_PAGE_SIZE),
  });
  try {
    const data = await apiFetch(`${path}?${params}`);
    const rows = data.media || [];
    galleryHasNext = rows.length > GALLERY_PAGE_SIZE;
    mediaItems = rows.slice(0, GALLERY_PAGE_SIZE);
    renderMediaGrid();
    if (scrollToTop) safeEl("gallery-grid")?.scrollIntoView({ behavior: userSettings().reduce_motion ? "auto" : "smooth", block: "start" });
  } catch (err) {
    mediaItems = [];
    galleryHasNext = false;
    $("gallery-grid").innerHTML = "";
    setTextIfPresent("result-count", err.message || "Feed failed to load");
    setEmptyState("Feed check needed", "This feed could not load. Use Checks or Refresh to test the live backend.");
  } finally {
    galleryLoading = false;
    updateGalleryPagination();
  }
}

async function loadFollowingFeed(page = 1, options = {}) {
  if (!currentUser) return $("auth-dialog").showModal();
  return loadPagedFeed("/api/feed/following", "following", page, options);
}

async function loadLikedFeed(page = 1, options = {}) {
  if (!currentUser) return $("auth-dialog").showModal();
  return loadPagedFeed("/api/me/likes", "liked", page, options);
}

function friendButtonLabel(status) {
  return {
    self: "You",
    friends: "Friends",
    pending_out: "Requested",
    pending_in: "Accept Request",
  }[status || "none"] || "Add Friend";
}

function userCard(user, { compact = false } = {}) {
  const avatar = user.avatar_url
    ? `<img src="${user.avatar_url}" alt="">`
    : escapeHtml((user.display_name || user.username || "IG").slice(0, 2).toUpperCase());
  return `
    <article class="user-card">
      <button type="button" class="avatar" style="border-color:${escapeHtml(user.profile_color || "#37c9a7")}" data-profile="${escapeHtml(user.username)}">${avatar}</button>
      <div>
        <h3>${escapeHtml(user.display_name || user.username)}</h3>
        <p class="muted">@${escapeHtml(user.username)}${user.profile_headline ? ` - ${escapeHtml(user.profile_headline)}` : ""}</p>
        ${!compact && user.bio ? `<p>${escapeHtml(user.bio)}</p>` : ""}
        ${!compact ? `<p class="muted">${Number(user.media_count || 0)} posts · ${Number(user.follower_count || 0)} followers</p>` : ""}
      </div>
      <div class="user-card-actions">
        <button type="button" data-profile="${escapeHtml(user.username)}">Profile</button>
        <button type="button" data-follow-user="${user.id}">${user.followed_by_me ? "Unfollow" : "Follow"}</button>
        <button type="button" data-friend-user="${user.id}" ${["self", "friends", "pending_out"].includes(user.friend_status) ? "disabled" : ""}>${friendButtonLabel(user.friend_status)}</button>
      </div>
    </article>
  `;
}

async function openUserSearchDialog() {
  $("user-search-dialog").showModal();
  const input = $("user-search-input");
  if (!input.value.trim() && currentUser) input.value = "";
  input.focus();
  await searchUsers();
}

async function searchUsers() {
  const query = $("user-search-input").value.trim();
  const list = $("user-search-results");
  if (!query) {
    list.innerHTML = `<p class="muted">Search by username, display name, bio, or headline.</p>`;
    return;
  }
  try {
    const data = await apiFetch(`/api/users/search?q=${encodeURIComponent(query)}`);
    const users = data.users || [];
    list.innerHTML = users.length ? users.map((user) => userCard(user)).join("") : `<p class="muted">No users found.</p>`;
  } catch (err) {
    list.innerHTML = `<p class="muted">${escapeHtml(err.message)}</p>`;
  }
}

async function toggleFollowUser(userId, following = null) {
  if (!currentUser) return $("auth-dialog").showModal();
  const button = document.querySelector(`[data-follow-user="${userId}"]`);
  const next = following ?? !(button?.textContent || "").toLowerCase().includes("unfollow");
  await apiFetch(`/api/users/${userId}/follow`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ following: next }),
  });
  if (activeProfileUsername) await openProfile(activeProfileUsername);
  else await searchUsers();
}

async function sendFriendRequest(userId) {
  if (!currentUser) return $("auth-dialog").showModal();
  const data = await apiFetch(`/api/users/${userId}/friend-request`, { method: "POST" });
  const status = data.status || "pending_out";
  document.querySelectorAll(`[data-friend-user="${userId}"]`).forEach((button) => {
    button.textContent = friendButtonLabel(status);
    button.disabled = ["friends", "pending_out"].includes(status);
  });
  if (activeProfileUsername) await openProfile(activeProfileUsername);
}

async function openProfile(username) {
  if (!username) return;
  activeProfileUsername = username;
  const data = await apiFetch(`/api/users/${encodeURIComponent(username)}/profile`);
  renderProfile(data);
  if (location.hash !== `#user/${encodeURIComponent(username)}`) {
    history.replaceState(null, "", `#user/${encodeURIComponent(username)}`);
  }
  if (!$("profile-dialog").open) $("profile-dialog").showModal();
}

function renderProfile(data) {
  const user = data.user || {};
  $("profile-dialog-title").textContent = user.display_name || user.username || "Profile";
  const tags = (user.featured_tags || []).map((tag) => `<span>${escapeHtml(tag)}</span>`).join("");
  const media = data.media || [];
  const collections = data.collections || [];
  const friends = data.friends || [];
  $("profile-view").innerHTML = `
    <section class="profile-hero" style="--profile-accent:${escapeHtml(user.profile_color || "#37c9a7")}">
      <div class="avatar large">${user.avatar_url ? `<img src="${user.avatar_url}" alt="">` : escapeHtml((user.display_name || user.username || "IG").slice(0, 2).toUpperCase())}</div>
      <div>
        <h2>${escapeHtml(user.display_name || user.username)}</h2>
        <p class="muted">@${escapeHtml(user.username || "")}${user.location_label ? ` · ${escapeHtml(user.location_label)}` : ""}</p>
        ${user.profile_headline ? `<p class="profile-headline">${escapeHtml(user.profile_headline)}</p>` : ""}
        ${user.bio ? `<p>${escapeHtml(user.bio)}</p>` : ""}
        ${tags ? `<div class="tag-row">${tags}</div>` : ""}
      </div>
      <div class="profile-actions">
        <button type="button" data-follow-user="${user.id}">${user.followed_by_me ? "Unfollow" : "Follow"}</button>
        <button type="button" data-friend-user="${user.id}" ${["self", "friends", "pending_out"].includes(user.friend_status) ? "disabled" : ""}>${friendButtonLabel(user.friend_status)}</button>
        <button type="button" data-copy-profile="${escapeHtml(user.username || "")}">Copy Link</button>
        ${user.website_url ? `<a class="button-link" href="${escapeHtml(user.website_url)}" target="_blank" rel="noopener">Website</a>` : ""}
      </div>
    </section>
    <section class="profile-stats">
      <div><strong>${Number(user.media_count || 0)}</strong><span>Posts</span></div>
      <div><strong>${Number(user.follower_count || 0)}</strong><span>Followers</span></div>
      <div><strong>${Number(user.following_count || 0)}</strong><span>Following</span></div>
      <div><strong>${Number(user.friend_count || 0)}</strong><span>Friends</span></div>
      <div><strong>${formatBytes(user.download_count || 0)}</strong><span>Downloads</span></div>
      <div><strong>${Number(user.like_count || 0)}</strong><span>Likes</span></div>
    </section>
    <section class="profile-section">
      <div class="section-title-row"><h3>Recent Uploads</h3><span class="muted">${media.length}</span></div>
      <div class="mini-media-grid">${media.length ? media.map((item) => `
        <button class="mini-media" type="button" data-open="${item.id}">
          ${renderPreview(item, "mini")}
          <span>${adultBadge(item)}${escapeHtml(item.title)}</span>
        </button>
      `).join("") : `<p class="muted">No public uploads to show.</p>`}</div>
    </section>
    <section class="profile-section">
      <div class="section-title-row"><h3>Collections</h3><span class="muted">${collections.length}</span></div>
      <div class="collection-list">${collections.length ? collections.map((collection) => `
        <article class="collection-card">
          <button type="button" data-collection-open="${collection.id}" class="collection-cover">
            ${collection.cover_url ? `<img src="${collection.cover_url}" alt="">` : `<span>${collection.cover_locked ? "18+" : escapeHtml(collection.name.slice(0, 2).toUpperCase())}</span>`}
          </button>
          <div><h3>${escapeHtml(collection.name)}</h3><p class="muted">${escapeHtml(collection.description || "No description")} · ${collection.item_count || 0} posts</p></div>
        </article>
      `).join("") : `<p class="muted">No public collections to show.</p>`}</div>
    </section>
    <section class="profile-section">
      <div class="section-title-row"><h3>Friends</h3><span class="muted">${friends.length}</span></div>
      <div class="user-results compact">${friends.length ? friends.map((friend) => userCard(friend, { compact: true })).join("") : `<p class="muted">No friends to show.</p>`}</div>
    </section>
  `;
}

async function openFriendsDialog() {
  if (!currentUser) return $("auth-dialog").showModal();
  $("friends-dialog").showModal();
  await loadFriendPanel();
}

async function loadFriendPanel() {
  const requests = await apiFetch("/api/friends/requests");
  const friends = await apiFetch("/api/me/friends");
  const incoming = requests.incoming || [];
  const outgoing = requests.outgoing || [];
  $("friend-requests-list").innerHTML = `
    ${incoming.length ? `<h3>Incoming</h3>${incoming.map((item) => `
      <article class="user-card">
        ${userCard(item.user, { compact: true })}
        <div class="user-card-actions">
          <button type="button" data-friend-action="accept" data-request-id="${item.id}">Accept</button>
          <button type="button" data-friend-action="decline" data-request-id="${item.id}">Decline</button>
        </div>
      </article>
    `).join("")}` : `<p class="muted">No incoming friend requests.</p>`}
    ${outgoing.length ? `<h3>Outgoing</h3>${outgoing.map((item) => `
      <article class="user-card">
        ${userCard(item.user, { compact: true })}
        <div class="user-card-actions"><button type="button" data-friend-action="cancel" data-request-id="${item.id}">Cancel</button></div>
      </article>
    `).join("")}` : ""}
  `;
  $("friends-list").innerHTML = (friends.friends || []).length ? friends.friends.map((friend) => userCard(friend, { compact: true })).join("") : `<p class="muted">No friends yet.</p>`;
}

async function respondFriendRequest(requestId, action) {
  await apiFetch(`/api/friends/requests/${requestId}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action }),
  });
  await loadFriendPanel();
}

async function copyProfileLink(username) {
  const url = `${location.origin}${location.pathname}#user/${encodeURIComponent(username)}`;
  try {
    await navigator.clipboard.writeText(url);
  } catch (_err) {
    const temp = document.createElement("input");
    temp.value = url;
    document.body.appendChild(temp);
    temp.select();
    document.execCommand("copy");
    temp.remove();
  }
}

async function deleteOwnMedia(id) {
  if (!confirm("Archive this post? It will disappear from the gallery but can be restored from Studio.")) return;
  await apiFetch(`/api/media/${id}`, { method: "DELETE" });
  await openStudio();
  await refreshAll();
}

function openReport(mediaId) {
  if (!currentUser) return $("auth-dialog").showModal();
  selectedReportMediaId = mediaId;
  setNotice("report-error", "");
  $("report-dialog").showModal();
}

async function submitReport(event) {
  event.preventDefault();
  if (!selectedReportMediaId) return;
  try {
    await apiFetch(`/api/media/${selectedReportMediaId}/report`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        reason: $("report-reason").value,
        details: $("report-details").value,
      }),
    });
    setNotice("report-error", "Report sent.", "success");
    $("report-dialog").close();
  } catch (err) {
    setNotice("report-error", err.message);
  }
}

async function saveAvatar() {
  if (!currentUser) return;
  const file = $("settings-avatar-file").files[0];
  if (!file) return setNotice("settings-error", "Choose an image first.");
  if (file.size > 5 * 1024 * 1024) return setNotice("settings-error", "Profile pictures must be 5MB or smaller.");
  const body = new FormData();
  body.set("file", file);
  try {
    const data = await apiFetch("/api/me/avatar", { method: "POST", body });
    currentUser = data.user;
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    fillSettingsForm();
  } catch (err) {
    setNotice("settings-error", err.message);
  }
}

function ensureUploadControlFields() {
  const form = $("upload-form");
  if (!form || $("upload-visibility")) return;
  const submit = form.querySelector('button[type="submit"], .primary');
  const wrapper = document.createElement("div");
  wrapper.className = "upload-controls-grid";
  wrapper.innerHTML = `
    <label class="field">Visibility
      <select id="upload-visibility">
        <option value="public">Public - appears in the gallery</option>
        <option value="unlisted">Unlisted - link/profile only</option>
        <option value="private">Private - only me</option>
      </select>
    </label>
    <label class="check-row"><input id="upload-comments-enabled" type="checkbox" checked> Allow comments</label>
    <label class="check-row"><input id="upload-downloads-enabled" type="checkbox" checked> Allow downloads</label>
    <label class="check-row"><input id="upload-pinned" type="checkbox"> Pin in my studio/feed</label>
  `;
  if (submit) form.insertBefore(wrapper, submit);
  else form.appendChild(wrapper);
}

function closeUploadDialog() {
  if (uploadInFlight) return;
  setNotice("upload-error", "");
  const form = safeEl("upload-form");
  if (form) form.reset();
  setTextIfPresent("file-label", "Choose image, GIF, or video under 500MB");
  resetUploadProgress();
  const dialog = safeEl("upload-dialog");
  if (dialog?.open) dialog.close();
  checkUploadReadiness();
}

function setUploadProgress(percent, label = "Uploading", options = {}) {
  showIfPresent("upload-progress-wrap", true);
  const wrap = safeEl("upload-progress-wrap");
  const bar = safeEl("upload-progress-bar");
  const pct = Math.max(0, Math.min(100, Number(percent || 0)));
  if (wrap) wrap.classList.toggle("is-processing", Boolean(options.processing));
  if (bar) bar.style.width = `${pct}%`;
  setTextIfPresent("upload-progress-percent", `${Math.round(pct)}%`);
  setTextIfPresent("upload-progress-label", label);
}

function resetUploadProgress() {
  showIfPresent("upload-progress-wrap", false);
  const wrap = safeEl("upload-progress-wrap");
  const bar = safeEl("upload-progress-bar");
  if (wrap) wrap.classList.remove("is-processing");
  if (bar) bar.style.width = "0%";
  setTextIfPresent("upload-progress-percent", "0%");
  setTextIfPresent("upload-progress-label", "Preparing upload");
  const button = safeEl("upload-submit");
  if (button) {
    button.classList.remove("is-uploading");
    button.textContent = "Post";
  }
}

function titleFromFilename(filename) {
  return String(filename || "upload")
    .replace(/\.[^.]+$/, "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .slice(0, 160);
}

function updateUploadFileSummary() {
  const file = safeEl("upload-file")?.files?.[0];
  setTextIfPresent("file-label", file ? `${file.name} - ${formatBytes(file.size)}` : "Choose image, GIF, or video under 500MB");
  setTextIfPresent("upload-file-summary", file ? `${file.type || "Unknown type"} · ${formatBytes(file.size)} · ${file.name}` : "No file selected.");
  const title = safeEl("upload-title");
  if (file && title && !title.value.trim()) title.value = titleFromFilename(file.name);
  checkUploadReadiness();
}

function openSettingsPanel() {
  if (!currentUser) return safeEl("auth-dialog")?.showModal();
  fillSettingsForm();
  safeEl("settings-dialog")?.showModal();
}

async function openCurrentUserDestination(target) {
  const destination = target || (currentUser ? ACCOUNT_NAVIGATION.signedInUsernameTarget : ACCOUNT_NAVIGATION.signedOutUsernameTarget);
  if (!currentUser) return safeEl("auth-dialog")?.showModal();
  if (destination === "settings") return openSettingsPanel();
  return openProfile(currentUser.username);
}

async function submitUpload(event) {
  event.preventDefault();
  if (uploadInFlight) return;
  if (!currentUser) {
    $("upload-dialog").close();
    $("auth-dialog").showModal();
    return;
  }
  setNotice("upload-error", "");
  const file = $("upload-file").files[0];
  if (!file) return setNotice("upload-error", "Choose a file first.");
  if (file.size > MAX_UPLOAD_BYTES) return setNotice("upload-error", "Uploads must be 500MB or smaller.");
  const body = new FormData();
  body.set("file", file);
  body.set("title", $("upload-title").value);
  body.set("description", $("upload-description").value);
  body.set("tags", $("upload-tags").value);
  body.set("is_adult", $("upload-adult").checked ? "true" : "false");
  if ($("upload-visibility")) body.set("visibility", $("upload-visibility").value || "public");
  if ($("upload-comments-enabled")) body.set("comments_enabled", $("upload-comments-enabled").checked ? "true" : "false");
  if ($("upload-downloads-enabled")) body.set("downloads_enabled", $("upload-downloads-enabled").checked ? "true" : "false");
  if ($("upload-pinned")) body.set("pinned", $("upload-pinned").checked ? "true" : "false");
  if ($("upload-category").value) {
    body.set("category_id", $("upload-category").value);
  } else {
    body.set("category_name", $("new-category-name").value);
    body.set("category_kind", $("new-category-kind").value);
  }
  try {
    uploadInFlight = true;
    uploadStartedAt = Date.now();
    setDisabledIfPresent("upload-submit", true);
    const submit = safeEl("upload-submit");
    if (submit) {
      submit.classList.add("is-uploading");
      submit.textContent = "Uploading...";
    }
    setUploadProgress(0, "Starting upload");
    const uploaded = await apiUpload("/api/media", body, ({ loaded, total, percent, phase }) => {
      const seconds = Math.max(1, Math.round((Date.now() - uploadStartedAt) / 1000));
      if (phase === "processing") {
        setUploadProgress(100, "Upload received. Saving media chunks on the server...", { processing: true });
        return;
      }
      if (total) {
        setUploadProgress(percent, `${formatBytes(loaded)} of ${formatBytes(total)} uploaded in ${seconds}s`);
      } else {
        setUploadProgress(percent || 12, `${formatBytes(loaded)} uploaded. Measuring transfer...`, { processing: true });
      }
    });
    setUploadProgress(100, "Saved. Refreshing gallery");
    uploadInFlight = false;
    closeUploadDialog();
    focusGalleryOnNewestUploads();
    await refreshAll();
    const uploadedId = uploaded?.media?.id;
    if (uploadedId && confirm("Upload saved. Open it now?")) await openDetail(uploadedId);
    else showToast("Upload saved.", "success");
  } catch (err) {
    setNotice("upload-error", err.message);
    showToast(err.message || "Upload failed.", "error");
  } finally {
    uploadInFlight = false;
    resetUploadProgress();
    checkUploadReadiness();
  }
}

async function resendEmailVerification() {
  const button = $("resend-email-verification");
  if (!currentUser || button.hidden) return;
  button.disabled = true;
  const originalText = button.textContent;
  button.textContent = "Sending...";
  try {
    const data = await apiFetch("/api/auth/resend-verification", { method: "POST" });
    $("account-bio").textContent = data.email_verification_sent
      ? "Verification code sent. Check your inbox."
      : data.already_verified
        ? "Email already verified."
        : "Verification code could not be sent.";
  } catch (err) {
    $("account-bio").textContent = err.message;
  } finally {
    button.disabled = false;
    button.textContent = originalText;
  }
}


function ensureLiveControlButtons() {
  const refresh = safeEl("refresh");
  const parent = refresh?.parentElement || document.querySelector(".topbar-actions") || document.body;
  const addButton = (id, text, handler) => {
    if (safeEl(id)) return;
    const button = document.createElement("button");
    button.id = id;
    button.type = "button";
    button.textContent = text;
    button.addEventListener("click", handler);
    parent.insertBefore(button, refresh?.nextSibling || null);
  };
  addButton("following-feed", "Following", loadFollowingFeed);
  addButton("liked-feed", "Liked", loadLikedFeed);
  addButton("live-checks-open", "Checks", () => runLiveChecks({ silent: false }));
}

function renderLiveChecks(data, silent = false) {
  const status = safeEl("connection-status");
  const checks = data?.checks || [];
  latestLiveSnapshot = data?.snapshot || latestLiveSnapshot;
  const failing = checks.filter((check) => check.ok === false && check.severity !== "warn");
  const warnings = checks.filter((check) => check.ok === false && check.severity === "warn");
  const activePosts = Number(latestLiveSnapshot?.media_active || 0);
  const label = !navigator.onLine ? "Offline" : failing.length ? "Attention" : warnings.length ? "Warnings" : activePosts ? `Live · ${activePosts}` : "Live";
  if (status) {
    status.textContent = label;
    status.title = checks.map((check) => `${check.label}: ${check.detail}`).join("\n");
    status.dataset.state = failing.length ? "error" : warnings.length ? "warn" : "ok";
    status.hidden = false;
  }
  if (!mediaItems.length && !galleryLoading && activePosts && !galleryFiltersActive()) {
    setTextIfPresent("result-count", `${activePosts} posts live · refresh if they do not appear`);
  }
  const missing = Number(data?.snapshot?.missing_db_files || 0);
  if (!silent && checks.length) {
    let message = checks.map((check) => `${check.ok ? "✓" : check.severity === "warn" ? "!" : "✕"} ${check.label}: ${check.detail}`).join("\n");
    if (missing > 0 && isDesmondUser()) message += `\n\n${missing} legacy file(s) still need migration. Use OK on the next prompt to migrate up to 10 safely.`;
    alert(message);
  }
  return { missing };
}

async function runLiveChecks({ silent = false } = {}) {
  if (!navigator.onLine) {
    renderLiveChecks({ checks: [{ label: "Browser network", ok: false, severity: "error", detail: "Your browser reports no internet connection." }] }, silent);
    return;
  }
  try {
    const data = await apiFetch("/api/live/checks");
    const rendered = renderLiveChecks(data, silent);
    if (!silent && rendered.missing > 0 && isDesmondUser() && confirm("Run a safe DB file migration batch now? This migrates up to 10 legacy disk files to DB blobs and may take a moment.")) {
      const migrated = await apiFetch("/api/live/migrate", { method: "POST" });
      alert(`Migration batch complete. Migrated: ${migrated.migrated?.migrated || 0}. Missing after batch: ${migrated.snapshot?.missing_db_files || 0}.`);
      return runLiveChecks({ silent: true });
    }
    return data;
  } catch (err) {
    renderLiveChecks({ checks: [{ label: "Backend", ok: false, severity: "error", detail: err.message }] }, silent);
  }
}

function startSilentChecks() {
  window.addEventListener("online", () => runLiveChecks({ silent: true }));
  window.addEventListener("offline", () => renderLiveChecks({ checks: [{ label: "Browser network", ok: false, severity: "error", detail: "Offline." }] }, true));
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) runLiveChecks({ silent: true });
  });
  setInterval(() => runLiveChecks({ silent: true }), LIVE_CHECK_INTERVAL_MS);
  setInterval(() => { if (token) refreshMe().catch(() => {}); }, 120000);
}

function checkUploadReadiness() {
  const file = safeEl("upload-file")?.files?.[0];
  const title = safeEl("upload-title")?.value?.trim();
  let message = "";
  if (file) {
    const allowed = /^(image|video)\//.test(file.type || "") || /\.(jpe?g|png|webp|gif|mp4|webm|mov|m4v|ogg)$/i.test(file.name || "");
    if (!allowed) message = "This file type may be rejected. Use an image, GIF, or video.";
    else if (file.size > MAX_UPLOAD_BYTES) message = "This file is over 500MB and will be rejected.";
    else if (!title) message = "Add a title before uploading.";
  }
  setNotice("upload-error", message);
  setDisabledIfPresent("upload-submit", Boolean(message) || uploadInFlight);
}

function showKeyboardShortcuts() {
  alert([
    "Keyboard shortcuts",
    "/ - focus search",
    "U - upload",
    "R - refresh current view",
    "S - share current view",
    "Arrow Left/Right - previous/next media or page",
    "Escape - close the open dialog",
  ].join("\n"));
}

function closeTopDialog() {
  const dialogs = Array.from(document.querySelectorAll("dialog[open]"));
  const dialog = dialogs.at(-1);
  if (!dialog) return false;
  if (dialog.id === "detail-dialog") closeDetailDialog();
  else dialog.close();
  return true;
}

function bindKeyboardShortcuts() {
  document.addEventListener("keydown", async (event) => {
    const target = event.target;
    const isTyping = ["INPUT", "TEXTAREA", "SELECT"].includes(target?.tagName) || target?.isContentEditable;
    if (event.key === "Escape" && closeTopDialog()) return;
    if (isTyping && event.key !== "Escape") return;
    if (event.key === "/") {
      event.preventDefault();
      safeEl("search")?.focus();
    } else if (event.key.toLowerCase() === "u") {
      event.preventDefault();
      currentUser ? $("upload-dialog").showModal() : $("auth-dialog").showModal();
    } else if (event.key.toLowerCase() === "r") {
      event.preventDefault();
      await refreshAll();
      showToast("Gallery refreshed.", "success");
    } else if (event.key.toLowerCase() === "s") {
      event.preventDefault();
      await shareCurrentView();
    } else if (event.key === "ArrowLeft") {
      if (safeEl("detail-dialog")?.open) await openAdjacentDetail(-1);
      else if (galleryPage > 1) await loadCurrentGalleryPage(galleryPage - 1, { scrollToTop: true });
    } else if (event.key === "ArrowRight") {
      if (safeEl("detail-dialog")?.open) await openAdjacentDetail(1);
      else if (galleryHasNext) await loadCurrentGalleryPage(galleryPage + 1, { scrollToTop: true });
    }
  });
}

function updateBackToTopVisibility() {
  showIfPresent("back-to-top", window.scrollY > 520);
}

function bindEvents() {
  ensureUploadControlFields();
  ensureLiveControlButtons();
  bindKeyboardShortcuts();
  window.addEventListener("scroll", updateBackToTopVisibility, { passive: true });
  on("back-to-top", "click", () => window.scrollTo({ top: 0, behavior: userSettings().reduce_motion ? "auto" : "smooth" }));
  document.addEventListener("error", (event) => {
    const target = event.target;
    if (target?.tagName === "IMG") {
      target.alt = "Preview unavailable";
      target.closest(".media-preview")?.classList.add("preview-missing");
    }
  }, true);
  $("auth-open").addEventListener("click", () => openCurrentUserDestination());
  if ($("auth-close")) $("auth-close").addEventListener("click", () => $("auth-dialog").close());
  $("logout").addEventListener("click", async () => {
    token = "";
    currentUser = null;
    writeStore(TOKEN_KEY, "");
    writeStore(USER_KEY, "");
    renderAuth();
    await refreshAll();
  });
  $("auth-toggle").addEventListener("click", () => setRegisterMode(!registerMode));
  $("auth-form").addEventListener("submit", submitAuth);
  $("resend-email-verification").addEventListener("click", resendEmailVerification);
  on("account-avatar", "click", () => openCurrentUserDestination(ACCOUNT_NAVIGATION.sidebarProfileTarget));
  on("account-name", "click", () => openCurrentUserDestination(ACCOUNT_NAVIGATION.sidebarProfileTarget));
  $("settings-email-save").addEventListener("click", saveEmailAndSendCode);
  $("settings-email-verify").addEventListener("click", verifyEmailCode);
  $("surprise-open").addEventListener("click", openSurprise);
  on("share-view", "click", shareCurrentView);
  on("shortcuts-open", "click", showKeyboardShortcuts);
  on("users-open", "click", openUserSearchDialog);
  on("profile-open", "click", () => currentUser && openProfile(currentUser.username));
  on("user-search-close", "click", () => $("user-search-dialog").close());
  on("user-search-input", "input", () => {
    clearTimeout(window.__userSearchTimer);
    window.__userSearchTimer = setTimeout(searchUsers, 180);
  });
  on("friends-open", "click", openFriendsDialog);
  on("friends-close", "click", () => $("friends-dialog").close());
  on("friend-tab-incoming", "click", () => {
    showIfPresent("friend-requests-list", true);
    showIfPresent("friends-list", false);
  });
  on("friend-tab-list", "click", () => {
    showIfPresent("friend-requests-list", false);
    showIfPresent("friends-list", true);
  });
  on("following-feed", "click", loadFollowingFeed);
  on("liked-feed", "click", loadLikedFeed);
  on("live-checks-open", "click", () => runLiveChecks({ silent: false }));
  $("collections-open").addEventListener("click", openCollectionsDialog);
  $("collections-close").addEventListener("click", () => $("collections-dialog").close());
  if ($("collection-picker-close")) $("collection-picker-close").addEventListener("click", () => $("collection-picker-dialog").close());
  $("collection-form").addEventListener("submit", createCollection);
  $("collection-picker-form").addEventListener("submit", addToCollection);
  $("studio-open").addEventListener("click", openStudio);
  $("studio-close").addEventListener("click", () => $("studio-dialog").close());
  if ($("settings-close")) $("settings-close").addEventListener("click", () => $("settings-dialog").close());
  $("report-form").addEventListener("submit", submitReport);
  if ($("report-close")) $("report-close").addEventListener("click", () => $("report-dialog").close());
  $("clear-tag").addEventListener("click", () => {
    clearGalleryFilters();
  });
  $("tag-cloud").addEventListener("click", (event) => {
    const tagButton = event.target.closest("[data-tag]");
    if (!tagButton) return;
    $("search").value = tagButton.dataset.tag;
    loadMedia(1);
  });
  $("settings-open").addEventListener("click", () => {
    openCurrentUserDestination(ACCOUNT_NAVIGATION.settingsTarget);
  });
  $("settings-form").addEventListener("submit", submitSettings);
  $("settings-age-save").addEventListener("click", submitAgeVerification);
  $("age-verify-form").addEventListener("submit", submitAgeVerification);
  $("age-close").addEventListener("click", () => $("age-dialog").close());
  $("avatar-save").addEventListener("click", saveAvatar);
  $("upload-open").addEventListener("click", () => currentUser ? $("upload-dialog").showModal() : $("auth-dialog").showModal());
  if ($("upload-close")) $("upload-close").addEventListener("click", closeUploadDialog);
  $("upload-form").addEventListener("submit", submitUpload);
  on("edit-media-close", "click", () => $("edit-media-dialog").close());
  on("edit-media-form", "submit", submitEditMedia);
  $("upload-category").addEventListener("change", toggleNewCategory);
  $("upload-file").addEventListener("change", updateUploadFileSummary);
  const dropZone = document.querySelector(".drop-zone");
  if (dropZone) {
    ["dragenter", "dragover"].forEach((eventName) => dropZone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropZone.classList.add("is-dragging");
    }));
    dropZone.addEventListener("dragleave", () => {
      dropZone.classList.remove("is-dragging");
    });
    dropZone.addEventListener("drop", (event) => {
      event.preventDefault();
      dropZone.classList.remove("is-dragging");
      const files = event.dataTransfer?.files;
      const input = safeEl("upload-file");
      if (input && files?.length) {
        input.files = files;
        updateUploadFileSummary();
      }
    });
  }
  if ($("upload-title")) $("upload-title").addEventListener("input", checkUploadReadiness);
  $("refresh").addEventListener("click", refreshAll);
  on("gallery-prev", "click", () => loadCurrentGalleryPage(galleryPage - 1, { scrollToTop: true }));
  on("gallery-next", "click", () => loadCurrentGalleryPage(galleryPage + 1, { scrollToTop: true }));
  on("active-filters", "click", (event) => {
    const chip = event.target.closest("[data-clear-filter]");
    if (!chip) return;
    const key = chip.dataset.clearFilter;
    if (key === "all") return clearGalleryFilters();
    if (key === "search") $("search").value = "";
    if (key === "kind") $("kind-filter").value = "";
    if (key === "category") $("category-filter").value = "";
    if (key === "sort") $("sort-filter").value = "new";
    loadMedia(1);
  });
  ["kind-filter", "category-filter", "sort-filter"].forEach((id) => $(id).addEventListener("input", () => loadMedia(1)));
  $("search").addEventListener("input", () => {
    clearTimeout(window.__gallerySearchTimer);
    renderActiveFilters();
    window.__gallerySearchTimer = setTimeout(() => loadMedia(1), SEARCH_DEBOUNCE_MS);
  });
  $("gallery-grid").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-open]");
    const profile = event.target.closest("[data-profile]");
    const like = event.target.closest("[data-like]");
    const bookmark = event.target.closest("[data-bookmark]");
    const collect = event.target.closest("[data-collect]");
    const copy = event.target.closest("[data-copy]");
    const manage = event.target.closest("[data-edit-media]");
    const ageGate = event.target.closest("[data-age-gate]");
    const download = event.target.closest("[data-download]");
    const del = event.target.closest("[data-delete-media]");
    if (profile) return openProfile(profile.dataset.profile);
    if (open && !handleAdultOpen(open.dataset.open)) await openDetail(open.dataset.open);
    if (manage) await editOwnMedia(manage.dataset.editMedia);
    if (del) await deleteOwnMedia(del.dataset.deleteMedia);
    if (download) await downloadMedia(download.dataset.download);
    if (like) await toggleLike(like.dataset.like);
    if (bookmark) await toggleBookmark(bookmark.dataset.bookmark);
    if (collect) await openCollectionPicker(collect.dataset.collect);
    if (copy) await copyAddress(copy.dataset.copy);
    if (ageGate) openAgeDialog("Verify your age to download this 18+ post.");
  });
  $("user-search-results").addEventListener("click", async (event) => {
    const profile = event.target.closest("[data-profile]");
    const follow = event.target.closest("[data-follow-user]");
    const friend = event.target.closest("[data-friend-user]");
    if (profile) await openProfile(profile.dataset.profile);
    if (follow) await toggleFollowUser(follow.dataset.followUser);
    if (friend) await sendFriendRequest(friend.dataset.friendUser);
  });
  $("friends-dialog").addEventListener("click", async (event) => {
    const action = event.target.closest("[data-friend-action]");
    const profile = event.target.closest("[data-profile]");
    const follow = event.target.closest("[data-follow-user]");
    const friend = event.target.closest("[data-friend-user]");
    if (action) await respondFriendRequest(action.dataset.requestId, action.dataset.friendAction);
    if (profile) await openProfile(profile.dataset.profile);
    if (follow) await toggleFollowUser(follow.dataset.followUser);
    if (friend) await sendFriendRequest(friend.dataset.friendUser);
  });
  $("profile-close").addEventListener("click", () => {
    activeProfileUsername = "";
    if (location.hash.startsWith("#user/")) history.replaceState(null, "", location.pathname + location.search);
    $("profile-dialog").close();
  });
  $("profile-view").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-open]");
    const collection = event.target.closest("[data-collection-open]");
    const profile = event.target.closest("[data-profile]");
    const follow = event.target.closest("[data-follow-user]");
    const friend = event.target.closest("[data-friend-user]");
    const copyProfile = event.target.closest("[data-copy-profile]");
    if (open && !handleAdultOpen(open.dataset.open)) await openDetail(open.dataset.open);
    if (collection) {
      $("collections-dialog").showModal();
      await openCollection(collection.dataset.collectionOpen);
    }
    if (profile) await openProfile(profile.dataset.profile);
    if (follow) await toggleFollowUser(follow.dataset.followUser);
    if (friend) await sendFriendRequest(friend.dataset.friendUser);
    if (copyProfile) await copyProfileLink(copyProfile.dataset.copyProfile);
  });
  $("collections-list").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-collection-open]");
    if (open) await openCollection(open.dataset.collectionOpen);
  });
  $("collection-media").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-open]");
    if (open && !handleAdultOpen(open.dataset.open)) await openDetail(open.dataset.open);
  });
  $("studio-list").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-open]");
    const edit = event.target.closest("[data-edit-media]");
    const del = event.target.closest("[data-delete-media]");
    const restore = event.target.closest("[data-restore-media]");
    if (open && !handleAdultOpen(open.dataset.open)) await openDetail(open.dataset.open);
    if (edit) await editOwnMedia(edit.dataset.editMedia);
    if (del) await deleteOwnMedia(del.dataset.deleteMedia);
    if (restore) await restoreOwnMedia(restore.dataset.restoreMedia);
  });
  $("comments-list").addEventListener("click", async (event) => {
    const del = event.target.closest("[data-delete-comment]");
    if (del) await deleteComment(del.dataset.deleteComment);
  });
  $("detail-close").addEventListener("click", closeDetailDialog);
  on("detail-prev", "click", () => openAdjacentDetail(-1));
  on("detail-next", "click", () => openAdjacentDetail(1));
  $("detail-dialog").addEventListener("cancel", () => stopMediaPlayback($("detail-dialog")));
  $("detail-dialog").addEventListener("close", () => stopMediaPlayback($("detail-dialog")));
  $("detail-dialog").addEventListener("click", (event) => {
    if (event.target === $("detail-dialog")) closeDetailDialog();
    const profile = event.target.closest("[data-profile]");
    if (profile) openProfile(profile.dataset.profile);
  });
  $("detail-like").addEventListener("click", () => activeDetail && toggleLike(activeDetail.id));
  $("detail-bookmark").addEventListener("click", () => activeDetail && toggleBookmark(activeDetail.id));
  $("detail-collect").addEventListener("click", () => activeDetail && openCollectionPicker(activeDetail.id));
  $("detail-report").addEventListener("click", () => activeDetail && openReport(activeDetail.id));
  $("detail-copy").addEventListener("click", () => activeDetail && copyAddress(activeDetail.id));
  $("detail-download").addEventListener("click", async (event) => {
    event.preventDefault();
    if (activeDetail) await downloadMedia(activeDetail.id);
  });
  $("comment-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!currentUser) return $("auth-dialog").showModal();
    const body = $("comment-body").value.trim();
    if (!body || !activeDetail) return;
    await apiFetch(`/api/media/${activeDetail.id}/comments`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ body }),
    });
    $("comment-body").value = "";
    await openDetail(activeDetail.id);
  });
}

async function boot() {
  bindEvents();
  renderAuth();
  startSilentChecks();
  const hasBackendConfig = await initApiOrigin();
  if (REMOTE_MODE && !hasBackendConfig) return;
  try {
    if (token) await refreshMe();
    await refreshAll();
    const hashProfile = decodeURIComponent(location.hash || "").match(/^#user\/(.+)/)?.[1];
    if (hashProfile) await openProfile(hashProfile);
    $("connection-status").textContent = REMOTE_MODE ? "Live" : "Local";
    runLiveChecks({ silent: true });
  } catch (err) {
    $("connection-status").textContent = "Offline";
    $("result-count").textContent = err.message;
  }
}

boot();
