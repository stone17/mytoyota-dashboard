window.safeLocaleDateString = (date, locales, options) => {
    const d = new Date(date);
    try { return d.toLocaleDateString(locales, options); }
    catch (e) { const fallback = { ...options }; delete fallback.timeZone; return d.toLocaleDateString(locales, fallback); }
};
window.safeLocaleString = (date, locales, options) => {
    const d = new Date(date);
    try { return d.toLocaleString(locales, options); }
    catch (e) { const fallback = { ...options }; delete fallback.timeZone; return d.toLocaleString(locales, fallback); }
};
window.safeLocaleTimeString = (date, locales, options) => {
    const d = new Date(date);
    try { return d.toLocaleTimeString(locales, options); }
    catch (e) { const fallback = { ...options }; delete fallback.timeZone; return d.toLocaleTimeString(locales, fallback); }
};