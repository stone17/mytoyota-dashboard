window.safeLocaleDateString = (date, locales, options) => {
    try { return new Date(date).toLocaleDateString(locales, options); }
    catch (e) { const fallback = { ...options }; delete fallback.timeZone; return new Date(date).toLocaleDateString(locales, fallback); }
};
window.safeLocaleString = (date, locales, options) => {
    try { return new Date(date).toLocaleString(locales, options); }
    catch (e) { const fallback = { ...options }; delete fallback.timeZone; return new Date(date).toLocaleString(locales, fallback); }
};
window.safeLocaleTimeString = (date, locales, options) => {
    try { return new Date(date).toLocaleTimeString(locales, options); }
    catch (e) { const fallback = { ...options }; delete fallback.timeZone; return new Date(date).toLocaleTimeString(locales, fallback); }
};