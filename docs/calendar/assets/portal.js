"use strict";
const $ = id => document.getElementById(id);
let feeds = [];
let activeFeed = null;
let activeMetadata = null;
let activeConfiguration = null;
let requestVersion = 0;
const customUrls = new Map();
const detailFields = ["show_unit", "show_position", "show_work_code", "show_shift", "show_employee", "show_description", "all_day", "busy", "show_symbols"];
const fieldId = name => name.replaceAll("_", "-");
const feedToken = feed => new URL(feed.url).pathname.split("/").pop().replace(/\.ics$/, "");
const storageKey = feed => "roster-feed-choices-v1:" + feedToken(feed);
const staticHosting = document.documentElement.dataset.hosting === "github-pages";
let backendOrigin;
function message(text = "") { $("message").textContent = text; }
async function api(path, data, live = false) {
  const options = {credentials: "omit", cache: "no-store"};
  const controller = new AbortController();
  options.signal = controller.signal;
  const timeout = setTimeout(() => controller.abort(), 10000);
  try {
  if (data !== undefined) { options.method = "POST"; options.headers = {"Content-Type": "application/json"}; options.body = JSON.stringify(data); }
  let endpoint = path;
  if (staticHosting) {
    if (path === "/portal/links") endpoint = "./directory.json";
    else if (path.startsWith("/portal/customization/") && !live) endpoint = "./people/" + path.split("/").pop() + ".json";
    else {
      if (!backendOrigin) {
        const config = await fetch("./backend.json", {cache: "no-store", signal:controller.signal}).then(response => response.json());
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
  } catch (error) {
    if (error.name === "AbortError" || error instanceof TypeError || error instanceof SyntaxError) throw new Error("The settings service is unavailable right now. Existing calendar links still work. Please try again later.");
    throw error;
  } finally { clearTimeout(timeout); }
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
    const copy = document.createElement("button"); copy.className = "copy-link"; copy.type = "button"; copy.textContent = "Copy link";
    copy.setAttribute("aria-label", "Copy calendar URL for " + feed.name);
    copy.addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(url); copy.textContent = "Copied!"; }
      catch {
        try {
          await openCustomize(feed);
          $("combined-url-details").open = true;
          $("custom-feed-url").focus(); $("custom-feed-url").select();
        } catch (error) { message(error.message); }
      }
    });
    const customize = document.createElement("button"); customize.type = "button"; customize.className = "customize-link"; customize.textContent = "Set up calendar";
    customize.dataset.token = feedToken(feed);
    customize.setAttribute("aria-label", "Set up calendar for " + feed.name);
    customize.addEventListener("click", () => openCustomize(feed).catch(error => message(error.message)));
    const actions = document.createElement("div"); actions.className = "row-actions"; actions.append(customize, copy);
    const urlDetails = document.createElement("details"); urlDetails.className = "row-url";
    const summary = document.createElement("summary"); summary.textContent = "View link";
    urlDetails.append(summary, link);
    box.append(actions, urlDetails); cell.append(box); row.append(name, cell); rows.push(row);
  }
  $("feed-rows").replaceChildren(...rows); $("empty-list").hidden = rows.length !== 0;
  $("people-count").textContent = rows.length === feeds.length ? feeds.length + " people" : rows.length + " of " + feeds.length + " people";
}
function setCalendarMode(mode) {
  const separate = mode === "separate";
  $("mode-combined").checked = !separate; $("mode-separate").checked = separate;
  $("combined-setup").hidden = separate; $("separate-calendars").hidden = !separate;
}
for (const radio of document.querySelectorAll('input[name="calendar-mode"]')) radio.addEventListener("change", () => setCalendarMode(radio.value));
function showSubscription(url, webcalUrl) {
  $("custom-feed-url").value = url;
  $("custom-apple-link").href = webcalUrl || url.replace(/^https?:/, "webcal:");
  $("copy-custom-feed").textContent = "Copy calendar link";
  $("custom-link-section").hidden = false;
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
  const colorRows = [];
  const overrides = choices.code_colors || {};
  const symbols = choices.code_symbols || {};
  const colorCodes = new Set([...activeMetadata.codes.map(item => item.value), ...Object.keys(overrides), ...Object.keys(symbols)]);
  for (const code of [...colorCodes].sort()) {
    const source = activeMetadata.codes.find(item => item.value === code);
    const sourceColor = source?.color || "#FFFFFF";
    const row = document.createElement("div"); row.className = "code-color-row";
    const label = document.createElement("label"); label.className = "code-color-label";
    const text = document.createElement("span"); text.textContent = code || "(No code)";
    const description = document.createElement("small"); description.textContent = source?.name || "Work code";
    const input = document.createElement("input"); input.type = "color"; input.value = overrides[code] || sourceColor;
    input.dataset.code = code; input.dataset.sourceColor = sourceColor;
    input.dataset.overridden = Object.hasOwn(overrides, code) ? "true" : "false";
    input.setAttribute("aria-label", "Color for " + (code || "no code"));
    input.addEventListener("input", () => { input.dataset.overridden = "true"; });
    label.append(input, text, description);
    const reset = document.createElement("button"); reset.type = "button"; reset.className = "text-button"; reset.textContent = "TeleStaff color";
    reset.setAttribute("aria-label", "Reset color for " + code + " to TeleStaff");
    reset.addEventListener("click", () => { input.value = sourceColor; input.dataset.overridden = "false"; markUnsaved(); });
    const symbolLabel = document.createElement("label"); symbolLabel.className = "symbol-control";
    const symbolName = document.createElement("span"); symbolName.textContent = "Title symbol";
    const symbol = document.createElement("select"); symbol.dataset.code = code;
    symbol.setAttribute("aria-label", "Title symbol for " + code);
    for (const choice of [{value:"auto",label:"Automatic · follows color"},{value:"",label:"No symbol"},...(activeMetadata.symbol_choices || [])]) {
      const option = document.createElement("option"); option.value = choice.value;
      option.textContent = (choice.value && choice.value !== "auto" ? choice.value + " " : "") + choice.label;
      symbol.append(option);
    }
    symbol.value = Object.hasOwn(symbols,code) ? symbols[code] : "auto";
    symbolLabel.append(symbolName,symbol);
    row.append(label, reset, symbolLabel); colorRows.push(row);
  }
  $("code-colors").replaceChildren(...colorRows);
}
function readChoices() {
  const choices = {calendar_name: $("calendar-name").value, title_style: $("title-style").value,
    custom_title: $("custom-title").value, reminder_minutes: Number($("reminder-minutes").value),
    code_mode: $("code-mode").value, codes: [...$("code-choices").querySelectorAll("input:checked")].map(input => input.value),
    past_days: $("past-days").value === "" ? null : Number($("past-days").value),
    ahead_days: $("ahead-days").value === "" ? null : Number($("ahead-days").value),
    code_colors: Object.fromEntries([...$("code-colors").querySelectorAll('input[type="color"]')].filter(input => input.dataset.overridden === "true").map(input => [input.dataset.code, input.value.toUpperCase()])),
    code_symbols: Object.fromEntries([...$("code-colors").querySelectorAll('select[data-code]')].filter(input => input.value !== "auto").map(input => [input.dataset.code,input.value]))};
  for (const field of detailFields) choices[field] = $(fieldId(field)).checked;
  return choices;
}
async function openCustomize(feed) {
  const version = ++requestVersion;
  message("Opening calendar setup…");
  let metadata = await api("/portal/customization/" + feedToken(feed));
  if (staticHosting) {
    try { metadata = await api("/portal/customization/" + feedToken(feed), undefined, true); }
    catch { /* Published settings remain readable if the local service is offline. */ }
  }
  if (version !== requestVersion) return;
  activeFeed = feed; activeMetadata = metadata;
  const choices = $("saved-feed"); choices.replaceChildren();
  const selectedUrl = customUrls.get(feed.url);
  let selectedKey = "";
  for (const configuration of metadata.feeds || []) {
    const option = document.createElement("option"); option.value = configuration.choice_key;
    option.textContent = configuration.choice_key ? (configuration.preferences.calendar_name || configuration.preferences.custom_title || "Saved custom calendar") + " · " + configuration.choice_key.slice(0,6) : "Main calendar · original URL";
    choices.append(option);
    if (configuration.url === selectedUrl) selectedKey = configuration.choice_key;
  }
  choices.value = selectedKey;
  $("saved-feed-options").hidden = (metadata.feeds || []).length < 2;
  activeConfiguration = metadata.feeds?.find(item => item.choice_key === selectedKey);
  if (!activeConfiguration) throw new Error("The calendar settings service is updating. Please refresh shortly.");
  $("custom-person").textContent = feed.name;
  $("calendar-name").placeholder = feed.name + " — Work Roster";
  for (const details of $("customize-dialog").querySelectorAll("details")) details.open = false;
  setCalendarMode("combined");
  applyChoices(activeConfiguration.preferences); $("custom-result").hidden = true; $("custom-message").textContent = "";
  showSubscription(activeConfiguration.url);
  renderCodeCalendars();
  $("generate-feed").disabled = false; message();
  if (!$("customize-dialog").open) $("customize-dialog").showModal();
  $("customize-dialog").scrollTop = 0;
  $("mode-combined").focus({preventScroll:true});
  await generateFeed(false);
}
async function generateFeed(remember = true) {
  const version = ++requestVersion;
  const feed = activeFeed;
  const preferences = readChoices();
  $("generate-feed").disabled = true; $("custom-message").textContent = "Creating your preview…";
  try {
    const route = remember ? "/portal/publish" : "/portal/customize";
    const result = await api(route, {token: feedToken(feed), preferences, choice_key: activeConfiguration.choice_key, expected_revision: activeConfiguration.revision});
    if (version !== requestVersion || !$("customize-dialog").open) return;
    showSubscription(result.url, result.webcal_url);
    $("preview-count").textContent = result.entry_count + " matching entries · " + result.calendar_name;
    const preview = result.preview;
    $("preview-title").textContent = preview ? preview.title : "No entries match these choices";
    $("preview-time").textContent = preview ? (preview.all_day ? preview.start + " · All day" : new Date(preview.start).toLocaleString() + " – " + new Date(preview.end).toLocaleString()) : "";
    $("preview-location").textContent = preview ? preview.location : "";
    $("preview-location").hidden = !preview?.location;
    $("preview-description").textContent = preview ? preview.description : "";
    $("preview-description").hidden = !preview?.description;
    const entries = [];
    for (const entry of result.entries || []) {
      const item = document.createElement("li");
      const title = document.createElement("strong"); title.textContent = entry.title;
      const times = document.createElement("span"); times.textContent = entry.start.length === 10 ? entry.start + " · All day" : new Date(entry.start).toLocaleString() + " – " + new Date(entry.end).toLocaleString();
      const swatch = document.createElement("span"); swatch.className = "entry-swatch"; swatch.style.backgroundColor = entry.color || "#FFFFFF";
      item.append(swatch, title, times); entries.push(item);
    }
    $("preview-entries").replaceChildren(...entries);
    $("preview-title").style.borderLeft = preview?.color ? "6px solid " + preview.color : "";
    $("preview-title").style.paddingLeft = preview?.color ? "10px" : "";
    $("custom-result").hidden = false;
    $("custom-link-section").hidden = false;
    $("custom-message").textContent = result.needs_save ? "Preview only. Save changes to update this same calendar link." : remember ? (result.new_publication ? "Saved to the same URL. Your link stays the same. Updates may take a few minutes to publish and appear after your calendar refreshes." : "Saved to the same URL. Keep your existing subscription.") : "";
    if (remember) {
      activeConfiguration.preferences = preferences; activeConfiguration.revision = result.revision;
      try {
        const fresh = await api("/portal/customization/" + feedToken(feed),undefined,true);
        if (version === requestVersion && activeFeed === feed) {
          activeMetadata = fresh;
          activeConfiguration = fresh.feeds.find(item => item.choice_key === activeConfiguration.choice_key);
        }
      } catch { /* The saved settings are already durable; published metadata can catch up. */ }
      if (version !== requestVersion || activeFeed !== feed || !$("customize-dialog").open) return;
      renderCodeCalendars();
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
function markUnsaved() {
  ++requestVersion; updateFormVisibility(); $("custom-result").hidden = true;
  $("generate-feed").disabled = false; $("custom-message").textContent = "Unsaved changes. Save to update this same subscription URL.";
  $("enable-code-calendars").disabled = true;
  $("code-calendar-message").textContent = "Save the settings above before creating or updating these optional calendars.";
}
$("customize-form").addEventListener("input", event => { if (event.target.id !== "saved-feed") markUnsaved(); });
$("saved-feed").addEventListener("change", () => {
  ++requestVersion;
  activeConfiguration = activeMetadata.feeds.find(item => item.choice_key === $("saved-feed").value);
  applyChoices(activeConfiguration.preferences); $("custom-result").hidden = true;
  showSubscription(activeConfiguration.url);
  renderCodeCalendars();
  generateFeed(false);
});
$("close-customize").addEventListener("click", () => $("customize-dialog").close());
$("customize-dialog").addEventListener("close", () => {
  ++requestVersion;
  const token = activeFeed && feedToken(activeFeed);
  [...document.querySelectorAll(".customize-link")].find(button => button.dataset.token === token)?.focus();
});
$("reset-customize").addEventListener("click", () => {
  ++requestVersion; applyChoices(activeMetadata.defaults); $("custom-result").hidden = true; $("generate-feed").disabled = false;
  $("custom-message").textContent = "Defaults restored, including all codes and exact times. Save to update this same URL.";
  $("enable-code-calendars").disabled = true;
  $("code-calendar-message").textContent = "Save the restored settings above first.";
  try {
    localStorage.removeItem(storageKey(activeFeed)); localStorage.removeItem(storageKey(activeFeed) + ":url");
  } catch { /* Optional. */ }
});
$("copy-custom-feed").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText($("custom-feed-url").value); $("copy-custom-feed").textContent = "Copied!"; }
  catch { $("combined-url-details").open = true; $("custom-feed-url").focus(); $("custom-feed-url").select(); $("custom-message").textContent = "Copy the highlighted calendar link."; }
});
function renderCodeCalendars() {
  const info = activeConfiguration?.code_calendars || {enabled:false,calendars:[]};
  $("enable-code-calendars").hidden = info.enabled;
  $("enable-code-calendars").disabled = !info.calendars.length;
  $("code-calendar-message").textContent = info.enabled ? "Your links are ready. Add each calendar you want, then choose its color in your calendar app." : info.calendars.length ? "Creating these links won’t change your existing calendar." : "No work-code calendars are available for these settings yet.";
  const rows=[];
  for (const item of info.calendars) {
    const row = document.createElement("div"); row.className = "code-calendar-item";
    const heading = document.createElement("div"); heading.className = "code-calendar-heading";
    const title = document.createElement("strong"); title.textContent = (item.code || "No code").replace(/^[.*^+~!?]+/, "");
    const swatch = document.createElement("i"); swatch.style.backgroundColor = item.color; swatch.className = "entry-swatch"; swatch.setAttribute("aria-hidden","true");
    heading.append(swatch,title);
    const description = document.createElement("p"); description.className = "code-calendar-description"; description.textContent = (item.name ? item.name + " · " : "") + item.entry_count + " entries";
    row.append(heading,description);
    if (info.enabled) {
      const details = document.createElement("details"); details.className = "link-details";
      const summary = document.createElement("summary"); summary.textContent = "View link & suggested color";
      const input = document.createElement("input"); input.readOnly = true; input.value = item.url; input.setAttribute("aria-label", "Calendar URL for " + item.code);
      const color = document.createElement("span"); color.className = "calendar-color-hint"; color.textContent = "Suggested color: " + item.color;
      details.append(summary,input,color);
      const box = document.createElement("div"); box.className = "calendar-code-actions";
      const copy = document.createElement("button"); copy.type = "button"; copy.className = "secondary"; copy.textContent = "Copy link";
      copy.setAttribute("aria-label", "Copy link for " + item.code);
      copy.addEventListener("click", async () => {
        try { await navigator.clipboard.writeText(item.url); copy.textContent = "Copied!"; }
        catch { details.open = true; input.focus(); input.select(); }
      });
      const apple = document.createElement("a"); apple.className = "primary"; apple.href = item.url.replace(/^https?:/,"webcal:"); apple.textContent = "Add to Apple Calendar ↗";
      apple.setAttribute("aria-label", "Add " + (item.code || "no code") + " to Apple Calendar");
      box.append(apple,copy); row.append(box,details);
    }
    rows.push(row);
  }
  $("code-calendar-links").replaceChildren(...rows);
}
$("enable-code-calendars").addEventListener("click", async () => {
  const feed=activeFeed, configuration=activeConfiguration;
  $("enable-code-calendars").disabled=true;
  $("code-calendar-message").textContent="Preparing separate calendar links…";
  try {
    const result=await api("/portal/code-calendars",{token:feedToken(feed),choice_key:configuration.choice_key});
    configuration.code_calendars=result;
    if (activeFeed!==feed || activeConfiguration!==configuration || !$("customize-dialog").open) return;
    renderCodeCalendars();
    $("code-calendar-message").textContent=result.new_publication && result.hosted_publication ? "Links created. Wait a few minutes for them to publish, then add each calendar below." : "Your links are ready. Add each calendar you want, then choose its color in your calendar app.";
  } catch(error) {
    if(activeConfiguration===configuration) { $("code-calendar-message").textContent=error.message; $("enable-code-calendars").disabled=false; }
  }
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
