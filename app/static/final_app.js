const API_BASE = "/api";
const REMOTE_MODE = window.location.hostname.endsWith("github.io");
const CONFIG_FILE = "live-config.json";
const TOKEN_KEY = "image_gallery_token";
const USER_KEY = "image_gallery_user";
const MAX_UPLOAD_BYTES = 250 * 1024 * 1024;

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
    const preview = item.media_kind === "video"
      ? `<video src="${item.url}" muted playsinline preload="metadata"></video>`
      : `<img src="${item.url}" alt="${escapeHtml(item.title)}" loading="lazy" />`;
    card.innerHTML = `
      <button class="media-preview" type="button" data-open="${item.id}">${preview}</button>
      <div class="media-info">
        <div>
          <h2>${escapeHtml(item.title)}</h2>
          <p class="muted">${escapeHtml(item.category_name)} by ${escapeHtml(item.display_name || item.username)}</p>
        </div>
        <div class="metric-row">
          <span>${item.like_count || 0} likes</span>
          <span>${item.downloads || 0} downloads</span>
          <span>${formatBytes(item.file_size)}</span>
        </div>
        <div class="card-actions">
          <button type="button" data-like="${item.id}">${item.liked_by_me ? "Unlike" : "Like"}</button>
          <button type="button" data-copy="${item.id}">Copy Address</button>
          <a href="${item.download_url}">Download</a>
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
  $("detail-description").textContent = item.description || "";
  $("detail-tags").innerHTML = (item.tags || []).map((tag) => `<span>${escapeHtml(tag)}</span>`).join("");
  $("detail-like").textContent = item.liked_by_me ? "Unlike" : "Like";
  $("detail-download").href = item.download_url;
  renderComments(data.comments || []);
  if (!$("detail-dialog").open) $("detail-dialog").showModal();
}

function renderComments(comments) {
  $("comments-list").innerHTML = comments.map((comment) => `
    <div class="comment">
      <strong>${escapeHtml(comment.display_name || comment.username)}</strong>
      <p>${escapeHtml(comment.body)}</p>
    </div>
  `).join("");
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
    $("auth-dialog").close();
    await refreshAll();
  } catch (err) {
    setNotice("auth-error", err.message);
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
    const copy = event.target.closest("[data-copy]");
    if (open) await openDetail(open.dataset.open);
    if (like) await toggleLike(like.dataset.like);
    if (copy) await copyAddress(copy.dataset.copy);
  });
  $("detail-close").addEventListener("click", () => $("detail-dialog").close());
  $("detail-like").addEventListener("click", () => activeDetail && toggleLike(activeDetail.id));
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
    await refreshAll();
    $("connection-status").textContent = REMOTE_MODE ? "Live" : "Local";
  } catch (err) {
    $("connection-status").textContent = "Offline";
    $("result-count").textContent = err.message;
  }
}

boot();
