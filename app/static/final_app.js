const API_BASE = "/api";
const REMOTE_MODE = window.location.hostname.endsWith("github.io");
const CONFIG_FILE = "live-config.json";
const TOKEN_KEY = "image_gallery_token";
const USER_KEY = "image_gallery_user";
const MAX_UPLOAD_BYTES = 500 * 1024 * 1024;
const DEFAULT_USER_SETTINGS = {
  theme_mode: "system",
  accent_color: "#37c9a7",
  grid_density: "comfortable",
  default_sort: "new",
  items_per_page: 60,
  autoplay_previews: false,
  muted_previews: true,
  reduce_motion: false,
  open_original_in_new_tab: false,
  blur_video_previews: false,
};

let apiOrigin = "";
let token = readStore(TOKEN_KEY);
let currentUser = readJsonStore(USER_KEY);
let categories = [];
let mediaItems = [];
let collectionsState = [];
let activeDetail = null;
let selectedCollectionMediaId = null;
let selectedReportMediaId = null;
let registerMode = false;
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

function filenameFromDisposition(disposition, fallback = "download") {
  const match = String(disposition || "").match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i);
  const raw = decodeURIComponent(match?.[1] || match?.[2] || fallback);
  return raw.replace(/[\\/\0]/g, "_").slice(0, 180) || fallback;
}

function setNotice(id, message) {
  const el = $(id);
  el.textContent = message || "";
  el.hidden = !message;
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
  $("logout").hidden = !currentUser;
  $("settings-open").hidden = !currentUser;
  $("studio-open").hidden = !currentUser;
  $("account-card").hidden = !currentUser;
  if (currentUser) {
    $("account-name").textContent = currentUser.display_name || currentUser.username;
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
  setValue("settings-bio", currentUser.bio || "");
  setChecked("settings-public-profile", currentUser.public_profile !== false);
  setChecked("settings-show-liked-count", currentUser.show_liked_count !== false);
  setValue("pref-theme-mode", prefs.theme_mode || "system");
  setValue("pref-accent-color", prefs.accent_color || "#37c9a7");
  setValue("pref-grid-density", prefs.grid_density || "comfortable");
  setValue("pref-default-sort", prefs.default_sort || "new");
  setValue("pref-items-per-page", prefs.items_per_page || 60);
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
      profile_color: $("settings-profile-color").value || "#37c9a7",
      public_profile: $("settings-public-profile").checked,
      show_liked_count: $("settings-show-liked-count").checked,
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
      items_per_page: Number($("pref-items-per-page").value || 60),
      autoplay_previews: $("pref-autoplay-previews").checked,
      muted_previews: $("pref-muted-previews").checked,
      reduce_motion: $("pref-reduce-motion").checked,
      open_original_in_new_tab: $("pref-open-original").checked,
      blur_video_previews: $("pref-blur-video-previews").checked,
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
    setNotice("settings-error", "Saved.");
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
    setNotice("settings-error", "Email verified.");
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
    setNotice(noticeId, "Age verified.");
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
    setTimeout(() => URL.revokeObjectURL(objectUrl), 1500);
  } catch (err) {
    if (err.status === 403 && item.is_adult) openAgeDialog(err.message);
    else alert(err.message || "Download failed.");
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
  await Promise.all([loadCategories(), isDesmondUser() ? loadStats() : Promise.resolve(), loadTags()]);
  await loadMedia();
}

async function loadTags() {
  const data = await apiFetch("/api/tags");
  const tags = data.tags || [];
  const showCounts = isDesmondUser();
  $("tag-cloud").innerHTML = tags.length ? tags.map((item) => (
    `<button type="button" data-tag="${escapeHtml(item.tag)}">${escapeHtml(item.tag)}${showCounts ? ` <span>${item.count}</span>` : ""}</button>`
  )).join("") : `<span class="muted">No tags yet</span>`;
}

async function loadCategories() {
  const data = await apiFetch("/api/categories");
  categories = data.categories || [];
  const filter = $("category-filter");
  const upload = $("upload-category");
  const selectedFilter = filter.value;
  const selectedUpload = upload.value;
  filter.innerHTML = `<option value="">All categories</option>`;
  upload.innerHTML = `<option value="">Create new category</option>`;
  for (const category of categories) {
    const count = isDesmondUser() ? ` (${category.media_count || 0})` : "";
    filter.insertAdjacentHTML("beforeend", `<option value="${category.id}">${escapeHtml(category.name)}${count}</option>`);
    upload.insertAdjacentHTML("beforeend", `<option value="${category.id}">${escapeHtml(category.name)}</option>`);
  }
  filter.value = selectedFilter;
  upload.value = selectedUpload || (categories[0]?.id ?? "");
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

async function loadMedia() {
  const params = new URLSearchParams();
  if ($("kind-filter").value) params.set("media_kind", $("kind-filter").value);
  if ($("category-filter").value) params.set("category_id", $("category-filter").value);
  if ($("search").value.trim()) params.set("q", $("search").value.trim());
  params.set("sort", $("sort-filter").value);
  params.set("limit", userSettings().items_per_page || 60);
  const data = await apiFetch(`/api/media?${params}`);
  mediaItems = data.media || [];
  renderMediaGrid();
}

function renderMediaGrid() {
  const grid = $("gallery-grid");
  grid.innerHTML = "";
  $("result-count").textContent = `${mediaItems.length} ${mediaItems.length === 1 ? "post" : "posts"}`;
  $("empty-state").hidden = mediaItems.length > 0;
  for (const item of mediaItems) {
    const card = document.createElement("article");
    card.className = `media-card${item.is_adult ? " adult-card" : ""}`;
    const prefs = userSettings();
    card.innerHTML = `
      <button class="media-preview" type="button" data-open="${item.id}">${renderPreview(item)}</button>
      <div class="media-info">
        <div class="author-row">
          <div class="avatar tiny" style="border-color:${escapeHtml(item.profile_color || "#37c9a7")}">${item.user_avatar_url ? `<img src="${item.user_avatar_url}" alt="">` : escapeHtml((item.display_name || item.username || "IG").slice(0, 2).toUpperCase())}</div>
          <div>
          <h2>${adultBadge(item)}${escapeHtml(item.title)}</h2>
          <p class="muted">${escapeHtml(item.category_name)} by ${escapeHtml(item.display_name || item.username)}${item.visibility && item.visibility !== "public" ? ` · ${escapeHtml(item.visibility)}` : ""}${item.pinned_at ? " · pinned" : ""}</p>
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
    ${item.user_avatar_url ? `<div class="profile-mini"><div class="avatar"><img src="${item.user_avatar_url}" alt=""></div><div><strong>${escapeHtml(item.display_name || item.username)}</strong>${item.user_bio ? `<p>${escapeHtml(item.user_bio)}</p>` : ""}${item.user_website_url ? `<a href="${item.user_website_url}" target="_blank" rel="noopener">Website</a>` : ""}</div></div>` : ""}
    <p>${escapeHtml(item.description || "")}</p>
  `;
  $("detail-tags").innerHTML = (item.tags || []).map((tag) => `<span>${escapeHtml(tag)}</span>`).join("");
  $("detail-like").textContent = item.liked_by_me ? "Unlike" : "Like";
  $("detail-bookmark").textContent = item.bookmarked_by_me ? "Saved" : "Bookmark";
  $("detail-download").href = "#";
  $("detail-download").dataset.download = item.id;
  $("detail-download").toggleAttribute("aria-disabled", item.downloads_enabled === false);
  renderComments(data.comments || []);
  const commentForm = $("comment-form");
  if (commentForm) commentForm.hidden = item.comments_enabled === false && Number(item.user_id) !== Number(currentUser?.id);
  if (!$("detail-dialog").open) $("detail-dialog").showModal();
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
      <button type="button" data-edit-media="${item.id}">Edit</button>\n      <button type="button" data-delete-media="${item.id}">Delete</button>
    </article>
  `).join("") : `<p class="muted">You have not uploaded anything yet.</p>`;
  $("studio-dialog").showModal();
}

async function editOwnMedia(id) {
  const item = (await apiFetch(`/api/media/${id}`)).media;
  const title = prompt("Edit title:", item.title || "");
  if (title === null) return;
  const description = prompt("Edit description:", item.description || "");
  if (description === null) return;
  const tags = prompt("Edit tags, comma separated:", (item.tags || []).join(", "));
  if (tags === null) return;
  const visibility = (prompt("Visibility: public, unlisted, or private", item.visibility || "public") || "public").toLowerCase();
  if (!["public", "unlisted", "private"].includes(visibility)) return alert("Visibility must be public, unlisted, or private.");
  const commentsEnabled = confirm("Allow comments on this post? OK = yes, Cancel = no");
  const downloadsEnabled = confirm("Allow downloads on this post? OK = yes, Cancel = no");
  const pinned = confirm("Pin this post to the top of your results? OK = yes, Cancel = no");
  const adult = confirm("Mark this post as 18+? OK = yes, Cancel = no");
  const categoryOptions = categories.map((cat) => `${cat.id}: ${cat.name}`).join("\n");
  const categoryRaw = prompt(`Category ID:\n${categoryOptions}`, item.category_id || categories[0]?.id || "");
  if (categoryRaw === null) return;
  const categoryId = Number(categoryRaw || item.category_id || categories[0]?.id || 0);
  const data = await apiFetch(`/api/media/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title,
      description,
      category_id: categoryId,
      tags: tags.split(",").map((tag) => tag.trim()).filter(Boolean),
      is_adult: adult,
      visibility,
      comments_enabled: commentsEnabled,
      downloads_enabled: downloadsEnabled,
      pinned,
    }),
  });
  mediaItems = mediaItems.map((entry) => Number(entry.id) === Number(id) ? data.media : entry);
  await openStudio();
  await refreshAll();
}

async function restoreOwnMedia(id) {
  await apiFetch(`/api/media/${id}/restore`, { method: "POST" });
  await openStudio();
  await refreshAll();
}

async function loadFollowingFeed() {
  if (!currentUser) return $("auth-dialog").showModal();
  const data = await apiFetch("/api/feed/following");
  mediaItems = data.media || [];
  renderMediaGrid();
}

async function loadLikedFeed() {
  if (!currentUser) return $("auth-dialog").showModal();
  const data = await apiFetch("/api/me/likes");
  mediaItems = data.media || [];
  renderMediaGrid();
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
    setNotice("report-error", "Report sent.");
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

async function submitUpload(event) {
  event.preventDefault();
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
    await apiFetch("/api/media", { method: "POST", body });
    $("upload-form").reset();
    $("upload-dialog").close();
    await refreshAll();
  } catch (err) {
    setNotice("upload-error", err.message);
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
  const failing = checks.filter((check) => check.ok === false && check.severity !== "warn");
  const warnings = checks.filter((check) => check.ok === false && check.severity === "warn");
  const label = !navigator.onLine ? "Offline" : failing.length ? "Attention" : warnings.length ? "Warnings" : "Live";
  if (status) {
    status.textContent = label;
    status.title = checks.map((check) => `${check.label}: ${check.detail}`).join("\n");
    status.dataset.state = failing.length ? "error" : warnings.length ? "warn" : "ok";
    status.hidden = false;
  }
  if (!silent && checks.length) {
    alert(checks.map((check) => `${check.ok ? "✓" : check.severity === "warn" ? "!" : "✕"} ${check.label}: ${check.detail}`).join("\n"));
  }
}

async function runLiveChecks({ silent = false } = {}) {
  if (!navigator.onLine) {
    renderLiveChecks({ checks: [{ label: "Browser network", ok: false, severity: "error", detail: "Your browser reports no internet connection." }] }, silent);
    return;
  }
  try {
    const data = await apiFetch("/api/live/checks");
    renderLiveChecks(data, silent);
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
  setInterval(() => runLiveChecks({ silent: true }), 30000);
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
  setDisabledIfPresent("upload-submit", Boolean(message));
}

function bindEvents() {
  ensureUploadControlFields();
  ensureLiveControlButtons();
  document.addEventListener("error", (event) => {
    const target = event.target;
    if (target?.tagName === "IMG") {
      target.alt = "Preview unavailable";
      target.closest(".media-preview")?.classList.add("preview-missing");
    }
  }, true);
  $("auth-open").addEventListener("click", () => $("auth-dialog").showModal());
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
  $("settings-email-save").addEventListener("click", saveEmailAndSendCode);
  $("settings-email-verify").addEventListener("click", verifyEmailCode);
  $("surprise-open").addEventListener("click", openSurprise);
  on("following-feed", "click", loadFollowingFeed);
  on("liked-feed", "click", loadLikedFeed);
  on("live-checks-open", "click", () => runLiveChecks({ silent: false }));
  $("collections-open").addEventListener("click", openCollectionsDialog);
  $("collections-close").addEventListener("click", () => $("collections-dialog").close());
  $("collection-form").addEventListener("submit", createCollection);
  $("collection-picker-form").addEventListener("submit", addToCollection);
  $("studio-open").addEventListener("click", openStudio);
  $("studio-close").addEventListener("click", () => $("studio-dialog").close());
  $("report-form").addEventListener("submit", submitReport);
  $("clear-tag").addEventListener("click", () => {
    $("search").value = "";
    loadMedia();
  });
  $("tag-cloud").addEventListener("click", (event) => {
    const tagButton = event.target.closest("[data-tag]");
    if (!tagButton) return;
    $("search").value = tagButton.dataset.tag;
    loadMedia();
  });
  $("settings-open").addEventListener("click", () => {
    fillSettingsForm();
    $("settings-dialog").showModal();
  });
  $("settings-form").addEventListener("submit", submitSettings);
  $("settings-age-save").addEventListener("click", submitAgeVerification);
  $("age-verify-form").addEventListener("submit", submitAgeVerification);
  $("age-close").addEventListener("click", () => $("age-dialog").close());
  $("avatar-save").addEventListener("click", saveAvatar);
  $("upload-open").addEventListener("click", () => currentUser ? $("upload-dialog").showModal() : $("auth-dialog").showModal());
  $("upload-form").addEventListener("submit", submitUpload);
  $("upload-category").addEventListener("change", toggleNewCategory);
  $("upload-file").addEventListener("change", () => {
    const file = $("upload-file").files[0];
    $("file-label").textContent = file ? `${file.name} - ${formatBytes(file.size)}` : "Choose image, GIF, or video under 500MB";
    checkUploadReadiness();
  });
  if ($("upload-title")) $("upload-title").addEventListener("input", checkUploadReadiness);
  $("refresh").addEventListener("click", refreshAll);
  ["search", "kind-filter", "category-filter", "sort-filter"].forEach((id) => $(id).addEventListener("input", loadMedia));
  $("gallery-grid").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-open]");
    const like = event.target.closest("[data-like]");
    const bookmark = event.target.closest("[data-bookmark]");
    const collect = event.target.closest("[data-collect]");
    const copy = event.target.closest("[data-copy]");
    const manage = event.target.closest("[data-edit-media]");
    const ageGate = event.target.closest("[data-age-gate]");
    const download = event.target.closest("[data-download]");
    const del = event.target.closest("[data-delete-media]");
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
  $("detail-dialog").addEventListener("cancel", () => stopMediaPlayback($("detail-dialog")));
  $("detail-dialog").addEventListener("close", () => stopMediaPlayback($("detail-dialog")));
  $("detail-dialog").addEventListener("click", (event) => {
    if (event.target === $("detail-dialog")) closeDetailDialog();
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
    $("connection-status").textContent = REMOTE_MODE ? "Live" : "Local";
    runLiveChecks({ silent: true });
  } catch (err) {
    $("connection-status").textContent = "Offline";
    $("result-count").textContent = err.message;
  }
}

boot();
