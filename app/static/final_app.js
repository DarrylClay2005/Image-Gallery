const API_BASE = "/api";
const REMOTE_MODE = window.location.hostname.endsWith("github.io");
const CONFIG_FILE = "live-config.json";
const TOKEN_KEY = "image_gallery_token";
const USER_KEY = "image_gallery_user";
const MAX_UPLOAD_BYTES = 250 * 1024 * 1024;
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
let activeDetail = null;
let registerMode = false;

const $ = (id) => document.getElementById(id);

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
  return `${apiOrigin}${path}`;
}

async function apiFetch(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(apiUrl(path), { ...options, headers });
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(data.detail || data.message || "Request failed");
  return data;
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
  $("account-card").hidden = !currentUser;
  if (currentUser) {
    $("account-name").textContent = currentUser.display_name || currentUser.username;
    $("account-bio").textContent = currentUser.bio || "Your settings apply only to this account.";
    renderAvatar("account-avatar", currentUser);
  }
  applyAccountSettings();
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

async function refreshMe() {
  if (!token) return;
  const data = await apiFetch("/api/me");
  currentUser = data.user;
  writeStore(USER_KEY, JSON.stringify(currentUser));
  renderAuth();
  fillSettingsForm();
}

async function initApiOrigin() {
  if (!REMOTE_MODE) {
    apiOrigin = "";
    $("connection-status").textContent = "Local";
    return;
  }
  try {
    const response = await fetch(CONFIG_FILE, { cache: "no-store" });
    const config = await response.json();
    apiOrigin = String(config.gallery_url || "").replace(/\/$/, "");
    $("connection-status").textContent = apiOrigin ? "Live" : "No backend";
  } catch (_err) {
    $("connection-status").textContent = "No backend";
  }
}

async function refreshAll() {
  await Promise.all([loadCategories(), loadStats()]);
  await loadMedia();
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
    filter.insertAdjacentHTML("beforeend", `<option value="${category.id}">${escapeHtml(category.name)} (${category.media_count || 0})</option>`);
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
    card.className = "media-card";
    const prefs = userSettings();
    const muted = prefs.muted_previews ? "muted" : "";
    const autoplay = prefs.autoplay_previews ? "autoplay loop" : "";
    const preview = item.media_kind === "video"
      ? `<video src="${item.url}" ${muted} ${autoplay} playsinline preload="metadata"></video>`
      : `<img src="${item.url}" alt="${escapeHtml(item.title)}" loading="lazy" />`;
    card.innerHTML = `
      <button class="media-preview" type="button" data-open="${item.id}">${preview}</button>
      <div class="media-info">
        <div class="author-row">
          <div class="avatar tiny" style="border-color:${escapeHtml(item.profile_color || "#37c9a7")}">${item.user_avatar_url ? `<img src="${item.user_avatar_url}" alt="">` : escapeHtml((item.display_name || item.username || "IG").slice(0, 2).toUpperCase())}</div>
          <div>
          <h2>${escapeHtml(item.title)}</h2>
          <p class="muted">${escapeHtml(item.category_name)} by ${escapeHtml(item.display_name || item.username)}</p>
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
          <button type="button" data-copy="${item.id}">Copy Address</button>
          <a href="${item.download_url}" ${prefs.open_original_in_new_tab ? 'target="_blank" rel="noopener"' : ""}>Download</a>
        </div>
      </div>
    `;
    grid.appendChild(card);
  }
}

async function openDetail(id) {
  const data = await apiFetch(`/api/media/${id}`);
  activeDetail = data.media;
  const item = activeDetail;
  $("detail-title").textContent = item.title;
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
  $("detail-download").href = item.download_url;
  renderComments(data.comments || []);
  if (!$("detail-dialog").open) $("detail-dialog").showModal();
}

function renderComments(comments) {
  $("comments-list").innerHTML = comments.map((comment) => `
    <div class="comment">
      <div class="comment-head">
        <div class="avatar tiny">${comment.user_avatar_path ? `<img src="${apiOrigin}/uploads/${comment.user_avatar_path}" alt="">` : escapeHtml((comment.display_name || comment.username || "IG").slice(0, 2).toUpperCase())}</div>
        <strong>${escapeHtml(comment.display_name || comment.username)}</strong>
      </div>
      <p>${escapeHtml(comment.body)}</p>
    </div>
  `).join("");
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
  await loadStats();
}

async function copyAddress(id) {
  const item = mediaItems.find((entry) => Number(entry.id) === Number(id)) || activeDetail;
  if (!item) return;
  await navigator.clipboard.writeText(item.url);
}

function toggleNewCategory() {
  const creating = !$("upload-category").value;
  $("new-category-wrap").hidden = !creating;
  $("new-category-kind-wrap").hidden = !creating;
  $("new-category-name").required = creating;
}

function setRegisterMode(next) {
  registerMode = next;
  $("auth-title").textContent = next ? "Create Account" : "Login";
  $("auth-submit").textContent = next ? "Create Account" : "Login";
  $("auth-toggle").textContent = next ? "Use Login" : "Create Account";
  $("display-name-wrap").hidden = !next;
}

async function submitAuth(event) {
  event.preventDefault();
  setNotice("auth-error", "");
  try {
    const payload = {
      username: $("auth-username").value.trim(),
      password: $("auth-password").value,
    };
    if (registerMode) payload.display_name = $("auth-display-name").value.trim();
    const data = await apiFetch(registerMode ? "/api/auth/register" : "/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    token = data.token;
    currentUser = data.user;
    writeStore(TOKEN_KEY, token);
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    await refreshMe();
    $("auth-dialog").close();
    await refreshAll();
  } catch (err) {
    setNotice("auth-error", err.message);
  }
}

function fillSettingsForm() {
  if (!currentUser) return;
  const prefs = userSettings();
  $("settings-display-name").value = currentUser.display_name || currentUser.username || "";
  $("settings-bio").value = currentUser.bio || "";
  $("settings-website").value = currentUser.website_url || "";
  $("settings-location").value = currentUser.location_label || "";
  $("settings-profile-color").value = currentUser.profile_color || "#37c9a7";
  $("settings-public-profile").checked = currentUser.public_profile !== false;
  $("settings-show-liked-count").checked = currentUser.show_liked_count !== false;
  $("pref-theme-mode").value = prefs.theme_mode;
  $("pref-accent-color").value = prefs.accent_color;
  $("pref-grid-density").value = prefs.grid_density;
  $("pref-default-sort").value = prefs.default_sort;
  $("pref-items-per-page").value = prefs.items_per_page;
  $("pref-autoplay-previews").checked = Boolean(prefs.autoplay_previews);
  $("pref-muted-previews").checked = Boolean(prefs.muted_previews);
  $("pref-reduce-motion").checked = Boolean(prefs.reduce_motion);
  $("pref-open-original").checked = Boolean(prefs.open_original_in_new_tab);
  $("pref-blur-video-previews").checked = Boolean(prefs.blur_video_previews);
  renderAvatar("settings-avatar-preview", currentUser);
}

async function submitSettings(event) {
  event.preventDefault();
  if (!currentUser) return;
  setNotice("settings-error", "");
  try {
    let data = await apiFetch("/api/me/profile", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        display_name: $("settings-display-name").value,
        bio: $("settings-bio").value,
        website_url: $("settings-website").value,
        location_label: $("settings-location").value,
        profile_color: $("settings-profile-color").value,
        public_profile: $("settings-public-profile").checked,
        show_liked_count: $("settings-show-liked-count").checked,
      }),
    });
    currentUser = data.user;
    data = await apiFetch("/api/me/settings", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
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
      }),
    });
    currentUser = data.user;
    writeStore(USER_KEY, JSON.stringify(currentUser));
    renderAuth();
    $("settings-dialog").close();
    await loadMedia();
  } catch (err) {
    setNotice("settings-error", err.message);
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
  if (file.size > MAX_UPLOAD_BYTES) return setNotice("upload-error", "Uploads must be 250MB or smaller.");
  const body = new FormData();
  body.set("file", file);
  body.set("title", $("upload-title").value);
  body.set("description", $("upload-description").value);
  body.set("tags", $("upload-tags").value);
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

function bindEvents() {
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
  $("settings-open").addEventListener("click", () => {
    fillSettingsForm();
    $("settings-dialog").showModal();
  });
  $("settings-form").addEventListener("submit", submitSettings);
  $("avatar-save").addEventListener("click", saveAvatar);
  $("upload-open").addEventListener("click", () => currentUser ? $("upload-dialog").showModal() : $("auth-dialog").showModal());
  $("upload-form").addEventListener("submit", submitUpload);
  $("upload-category").addEventListener("change", toggleNewCategory);
  $("upload-file").addEventListener("change", () => {
    const file = $("upload-file").files[0];
    $("file-label").textContent = file ? `${file.name} - ${formatBytes(file.size)}` : "Choose image, GIF, or video under 250MB";
  });
  $("refresh").addEventListener("click", refreshAll);
  ["search", "kind-filter", "category-filter", "sort-filter"].forEach((id) => $(id).addEventListener("input", loadMedia));
  $("gallery-grid").addEventListener("click", async (event) => {
    const open = event.target.closest("[data-open]");
    const like = event.target.closest("[data-like]");
    const bookmark = event.target.closest("[data-bookmark]");
    const copy = event.target.closest("[data-copy]");
    if (open) await openDetail(open.dataset.open);
    if (like) await toggleLike(like.dataset.like);
    if (bookmark) await toggleBookmark(bookmark.dataset.bookmark);
    if (copy) await copyAddress(copy.dataset.copy);
  });
  $("detail-close").addEventListener("click", () => $("detail-dialog").close());
  $("detail-like").addEventListener("click", () => activeDetail && toggleLike(activeDetail.id));
  $("detail-bookmark").addEventListener("click", () => activeDetail && toggleBookmark(activeDetail.id));
  $("detail-copy").addEventListener("click", () => activeDetail && copyAddress(activeDetail.id));
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
  await initApiOrigin();
  if (REMOTE_MODE && !apiOrigin) return;
  try {
    if (token) await refreshMe();
    await refreshAll();
    $("connection-status").textContent = REMOTE_MODE ? "Live" : "Local";
  } catch (err) {
    $("connection-status").textContent = "Offline";
    $("result-count").textContent = err.message;
  }
}

boot();
