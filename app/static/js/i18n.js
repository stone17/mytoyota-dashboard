class I18n {
    constructor() {
        this.locale = localStorage.getItem('language') || 'en';
        this.translations = {};
    }
    
    async init() {
        try {
            const response = await fetch(`/static/locales/${this.locale}.json`);
            if (!response.ok) throw new Error("Locale not found");
            this.translations = await response.json();
        } catch (e) {
            console.error("Failed to load locale:", this.locale, e);
            if (this.locale !== 'en') {
                const fallback = await fetch(`/static/locales/en.json`);
                this.translations = await fallback.json();
            }
        }
        document.documentElement.lang = this.locale;
        this.translateDOM();
    }

    t(key, placeholders = {}) {
        let text = key.split('.').reduce((obj, i) => (obj ? obj[i] : null), this.translations) || key;
        for (let p in placeholders) {
            text = text.replace(`{${p}}`, placeholders[p]);
        }
        return text;
    }

    translateDOM(root = document) {
        root.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            const text = this.t(key);
            
            // If it's a specific input with placeholder, we want to replace placeholder
            if (el.tagName === 'INPUT' && el.type !== 'submit' && el.type !== 'button' || el.tagName === 'TEXTAREA') {
                // Not standard to use data-i18n for placeholders directly if they also have value,
                // but if we used data-i18n-placeholder it would be safer.
                // We'll trust data-i18n replaces textContent normally, 
                // but let's check for explicitly data-i18n-placeholder
            } else {
                if (text !== key) {
                    // Do not replace innerHTML completely if it has children like SVGs.
                    // Actually, to be safe, if there's no children, textContent is fine.
                    // If it has children, we need a strategy. We will put the data-i18n ON THE SPAN specifically.
                    el.textContent = text;
                }
            }
        });

        root.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
            const key = el.getAttribute('data-i18n-placeholder');
            const text = this.t(key);
            if (text !== key) el.setAttribute('placeholder', text);
        });
        
        root.querySelectorAll('[data-i18n-title]').forEach(el => {
            const key = el.getAttribute('data-i18n-title');
            const text = this.t(key);
            if (text !== key) el.setAttribute('title', text);
        });
    }
}

window.i18n = new I18n();

// Initialize I18n on page load.
document.addEventListener('DOMContentLoaded', async () => {
    await window.i18n.init();
    
    // Also re-translate when language changes
    window.addEventListener('languageChanged', async () => {
        await window.i18n.init();
    });
});
