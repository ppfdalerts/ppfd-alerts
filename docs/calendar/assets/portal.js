"use strict";
const $ = id => document.getElementById(id);
let feeds = [];
let activeFeed = null;
let activeMetadata = null;
let requestVersion = 0;
const customUrls = new Map();
const detailFields = ["show_unit", "show_position", "show_work_code", "show_shift", "show_employee", "show_description", "all_day", "busy"];
const fieldId = name => name.replaceAll("_", "-");
const feedToken = feed => new URL(feed.url).pathname.split("/").pop().replace(/\.ics$/, "");
const storageKey = feed => "roster-feed-choices-v1:" + feedToken(feed);
const staticHosting = document.documentElement.dataset.hosting === "github-pages";
let backendOrigin;
function message(text = "") { $("message").textContent = text; }
async function api(path, data) {
  const options = {credentials: "omit", cache: "no-store"};
  if (data !== undefined) { options.method = "POST"; options.headers = {"Content-Type": "application/json"}; options.body = JSON.stringify(data); }
  let endpoint = path;
  if (staticHosting) {
    if (path === "/portal/links") endpoint = "./directory.json";
    else if (path.startsWith("/portal/customization/")) endpoint = "./people/" + path.split("/").pop() + ".json";
    else {
      if (!backendOrigin) {
        const config = await fetch("./backend.json", {cache: "no-store"}).then(response => response.json());
        const parsed = new URL(config.api_origin);
        if (parsed.protocol !== "https:" || parsed.pathname !== "/" || parsed.search || parsed.hash || parsed.username || parsed.password) throw new Error("The customization service is not configured.");
        backendOrigin = parsed.origin;
      }
      endpoint = backendOrigin + path;
    }
  }
  const response = await fetch(endpoint, options);
  const body = await response.json();
  if (!response.ok) {
    throw new Error(typeof body.detail === "string" ? body.detail : "Something went wrong. Please try again.");
  }
  return body;
}
function renderFeeds() {
  const words = $("name-search").value.toLowerCase().trim().split(/\s+/);
  const rows = [];
  for (const feed of feeds) {
    if (!words.every(word => feed.name.toLowerCase().includes(word))) continue;
    const row = document.createElement("tr");
    const name = document.createElement("th"); name.scope = "row"; name.textContent = feed.name;
    const cell = document.createElement("td");
    const box = document.createElement("div"); box.className = "feed-link";
    const url = customUrls.get(feed.url) || feed.url;
    const link = document.createElement("a"); link.className = "feed-url"; link.href = url;
    link.textContent = url.includes("?options=") ? url.split("?")[0] + "?options=…" : url;
    const copy = document.createElement("button"); copy.className = "copy-link"; copy.type = "button"; copy.textContent = "Copy";
    copy.setAttribute("aria-label", "Copy calendar URL for " + feed.name);
    copy.addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(url); copy.textContent = "Copied!"; }
      catch {
        await openCustomize(feed);
        message("Open Customize to select and copy the complete subscription URL.");
      }
    });
    const customize = document.createElement("button"); customize.type = "button"; customize.className = "customize-link"; customize.textContent = "Customize";
    customize.dataset.token = feedToken(feed);
    customize.setAttribute("aria-label", "Customize calendar for " + feed.name);
    customize.addEventListener("click", () => openCustomize(feed).catch(error => message(error.message)));
    const actions = document.createElement("div"); actions.className = "row-actions"; actions.append(copy, customize);
    box.append(link, actions); cell.append(box); row.append(name, cell); rows.push(row);
  }
  $("feed-rows").replaceChildren(...rows); $("empty-list").hidden = rows.length !== 0;
}
function updateFormVisibility() {
  $("custom-title-group").hidden = $("title-style").value !== "custom";
  $("code-choices").hidden = $("code-mode").value === "all";
}
function applyChoices(choices) {
  $("calendar-name").value = typeof choices.calendar_name === "string" ? choices.calendar_name : "";
  $("title-style").value = choices.title_style;
  $("custom-title").value = typeof choices.custom_title === "string" ? choices.custom_title : "";
  for (const field of detailFields) $(fieldId(field)).checked = choices[field] === true;
  $("reminder-minutes").value = String(choices.reminder_minutes);
  $("code-mode").value = choices.code_mode;
  $("past-days").value = choices.past_days === null ? "" : String(choices.past_days);
  $("ahead-days").value = choices.ahead_days === null ? "" : String(choices.ahead_days);
  const codes = Array.isArray(choices.codes) ? choices.codes : [];
  const allCodes = new Set([...activeMetadata.codes.map(item => item.value), ...codes.filter(item => typeof item === "string")]);
  const labels = [];
  for (const code of [...allCodes].sort()) {
    const label = document.createElement("label"); label.className = "option-check";
    const checkbox = document.createElement("input"); checkbox.type = "checkbox"; checkbox.value = code; checkbox.checked = codes.includes(code);
    const source = activeMetadata.codes.find(item => item.value === code);
    label.append(checkbox, document.createTextNode((code || "(No code)") + (source ? " · " + source.count : " · not currently on roster"))); labels.push(label);
  }
  $("code-choices").replaceChildren(...labels); updateFormVisibility();
}
function readChoices() {
  const choices = {calendar_name: $("calendar-name").value, title_style: $("title-style").value,
    custom_title: $("custom-title").value, reminder_minutes: Number($("reminder-minutes").value),
    code_mode: $("code-mode").value, codes: [...$("code-choices").querySelectorAll("input:checked")].map(input => input.value),
    past_days: $("past-days").value === "" ? null : Number($("past-days").value),
    ahead_days: $("ahead-days").value === "" ? null : Number($("ahead-days").value)};
  for (const field of detailFields) choices[field] = $(fieldId(field)).checked;
  return choices;
}
async function openCustomize(feed) {
  const version = ++requestVersion;
  message("Loading feed settings…");
  const metadata = await api("/portal/customization/" + feedToken(feed));
  if (version !== requestVersion) return;
  activeFeed = feed; activeMetadata = metadata;
  let choices = metadata.defaults;
  try {
    const stored = localStorage.getItem(storageKey(feed));
    if (stored && stored.length <= 8192) choices = {...choices, ...JSON.parse(stored)};
  } catch { /* Browser storage is optional. */ }
  $("custom-person").textContent = feed.name;
  $("calendar-name").placeholder = feed.name + " — Work Roster";
  applyChoices(choices); $("custom-result").hidden = true; $("custom-message").textContent = "";
  $("generate-feed").disabled = false; message();
  if (!$("customize-dialog").open) $("customize-dialog").showModal();
  $("calendar-name").focus();
  await generateFeed(false);
}
async function generateFeed(remember = true) {
  const version = ++requestVersion;
  const feed = activeFeed;
  const preferences = readChoices();
  $("generate-feed").disabled = true; $("custom-message").textContent = "Creating your preview…";
  try {
    const route = remember && activeMetadata.hosted_publication ? "/portal/publish" : "/portal/customize";
    const result = await api(route, {token: feedToken(feed), preferences});
    if (version !== requestVersion || !$("customize-dialog").open) return;
    $("custom-feed-url").value = result.url; $("custom-apple-link").href = result.webcal_url;
    $("copy-custom-feed").textContent = "Copy";
    $("preview-count").textContent = result.entry_count + " matching entries · " + result.calendar_name;
    const preview = result.preview;
    $("preview-title").textContent = preview ? preview.title : "No entries match these choices";
    $("preview-time").textContent = preview ? (preview.all_day ? preview.start + " · All day" : new Date(preview.start).toLocaleString() + " – " + new Date(preview.end).toLocaleString()) : "";
    $("preview-location").textContent = preview ? preview.location : "";
    $("preview-location").hidden = !preview?.location;
    $("preview-description").textContent = preview ? preview.description : "";
    $("preview-description").hidden = !preview?.description;
    $("custom-result").hidden = false;
    $("custom-link-section").hidden = result.needs_save === true;
    $("custom-message").textContent = result.needs_save ? "This is a preview. Choose ‘Preview and create my link’ to save these choices and publish the feed." : result.new_publication ? "Your choices are saved. GitHub can take a few minutes to publish a new link; wait before subscribing." : "";
    if (remember) {
      customUrls.set(feed.url, result.url); renderFeeds();
      try {
        localStorage.setItem(storageKey(feed), JSON.stringify(preferences));
        localStorage.setItem(storageKey(feed) + ":url", result.url);
      } catch { /* Optional. */ }
    }
  } catch (error) {
    if (version === requestVersion) { $("custom-message").textContent = error.message; $("custom-result").hidden = true; }
  } finally { if (version === requestVersion) $("generate-feed").disabled = false; }
}
$("customize-form").addEventListener("submit", event => { event.preventDefault(); generateFeed(); });
$("customize-form").addEventListener("input", () => {
  ++requestVersion; updateFormVisibility(); $("custom-result").hidden = true;
  $("generate-feed").disabled = false; $("custom-message").textContent = "Create a new link to preview these changes.";
});
$("close-customize").addEventListener("click", () => $("customize-dialog").close());
$("customize-dialog").addEventListener("close", () => {
  ++requestVersion;
  const token = activeFeed && feedToken(activeFeed);
  [...document.querySelectorAll(".customize-link")].find(button => button.dataset.token === token)?.focus();
});
$("reset-customize").addEventListener("click", () => {
  ++requestVersion; applyChoices(activeMetadata.defaults); $("custom-result").hidden = true; $("generate-feed").disabled = false;
  $("custom-message").textContent = "Defaults restored. Create a new link to apply them.";
  try {
    localStorage.removeItem(storageKey(activeFeed)); localStorage.removeItem(storageKey(activeFeed) + ":url");
  } catch { /* Optional. */ }
});
$("copy-custom-feed").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText($("custom-feed-url").value); $("copy-custom-feed").textContent = "Copied!"; }
  catch { $("custom-feed-url").focus(); $("custom-feed-url").select(); $("custom-message").textContent = "Copy the highlighted complete URL."; }
});
async function loadFeeds() {
  const result = await api("/portal/links");
  feeds = result.people;
  for (const feed of feeds) {
    try {
      const stored = localStorage.getItem(storageKey(feed) + ":url");
      if (!stored || stored.length > 5000) continue;
      const candidate = new URL(stored), original = new URL(feed.url);
      const variantPrefix = original.pathname.replace(/\.ics$/, "/");
      const publishedVariant = candidate.pathname.startsWith(variantPrefix) && /^[a-f0-9]{24}\.ics$/.test(candidate.pathname.slice(variantPrefix.length)) && !candidate.search;
      const legacyVariant = candidate.pathname === original.pathname && candidate.searchParams.size === 1 && candidate.searchParams.has("options");
      if (candidate.origin === original.origin && !candidate.hash && (publishedVariant || legacyVariant)) customUrls.set(feed.url, stored);
    } catch { /* Browser storage is optional. Never use unrelated saved URLs. */ }
  }
  $("name-search").value = ""; renderFeeds(); message();
  $("updated-time").textContent = result.last_updated ? "Roster last checked " + new Date(result.last_updated).toLocaleString() + "." : "Waiting for the first roster update.";
  const coverage = result.window;
  const far = result.far_refresh;
  const range = coverage ? "Available dates: " + coverage.from + " through " + coverage.through + ". " : "";
  const extended = far?.last_success ? "Extended range last checked " + new Date(far.last_success).toLocaleString() + "." : "The extended range will appear after its first collection finishes.";
  const issue = far?.state === "failed" ? " Extended refresh failed; the last collected entries are still available." : far?.state === "auth_required" ? " Sign-in is required before extended updates can resume." : "";
  $("coverage-time").textContent = range + extended + (far?.state === "checking" ? " Extended-range collection is running." : "") + issue;
}
$("name-search").addEventListener("input", renderFeeds);
message("Loading calendar links…");
loadFeeds().catch(() => message("Could not load calendar links. Please refresh this page to try again."));
